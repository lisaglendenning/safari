package edu.uw.zookeeper.orchestra;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;

public class VolumeLookupService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public VolumeLookupService getVolumeLookupService(
                ControlClientService<?> controlClient,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            VolumeLookupService instance = new VolumeLookupService(controlClient);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    protected static class VolumeLookupNode extends ZNodeLabelTrie.DefaultsNode<VolumeLookupNode> {
    
        public static VolumeLookupNode root() {
            return new VolumeLookupNode(
                    Optional.<ZNodeLabelTrie.Pointer<VolumeLookupNode>>absent());
        }
        
        protected AtomicReference<VolumeAssignment> value;
    
        protected VolumeLookupNode(
                Optional<ZNodeLabelTrie.Pointer<VolumeLookupNode>> parent) {
            this(null, parent);
        }
        
        protected VolumeLookupNode(
                VolumeAssignment value,
                Optional<ZNodeLabelTrie.Pointer<VolumeLookupNode>> parent) {
            super(parent);
            this.value = new AtomicReference<VolumeAssignment>(value);
        }
        
        public VolumeAssignment get() {
            return value.get();
        }
        
        public VolumeAssignment getAndSet(VolumeAssignment value) {
            return this.value.getAndSet(value);
        }
        
        public VolumeAssignment setVolume(Volume volume) {
            VolumeAssignment prev = value.get();
            VolumeAssignment updated = (prev == null) 
                    ? VolumeAssignment.of(volume, null)
                            : prev.setVolume(volume);
            if (value.compareAndSet(prev, updated)) {
                return prev;
            } else {
                return setVolume(volume);
            }
        }
        
        public VolumeAssignment setAssignment(Identifier assignment) {
            VolumeAssignment prev = value.get();
            VolumeAssignment updated = (prev == null) 
                    ? VolumeAssignment.of(Volume.of(null, VolumeDescriptor.of(path())), assignment)
                            : prev.setAssignment(assignment);
            if (value.compareAndSet(prev, updated)) {
                return prev;
            } else {
                return setAssignment(assignment);
            }
        }
    
        protected VolumeLookupNode newChild(ZNodeLabel.Component label) {
            ZNodeLabelTrie.Pointer<VolumeLookupNode> pointer = ZNodeLabelTrie.SimplePointer.of(label, this);
            return new VolumeLookupNode(Optional.of(pointer));
        }
    }
    
    private static final ZNodeLabel.Path VOLUMES_PATH = Control.path(Orchestra.Volumes.class);

    protected final Logger logger;
    protected final ControlClientService<?> controlClient;
    protected final ZNodeLabelTrie<VolumeLookupNode> lookupTrie;
    protected final ConcurrentMap<Identifier, ZNodeLabel.Path> byVolumeId;
    
    public VolumeLookupService(
            ControlClientService<?> controlClient) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.controlClient = controlClient;
        this.lookupTrie = ZNodeLabelTrie.of(VolumeLookupNode.root());
        this.byVolumeId = new ConcurrentHashMap<Identifier, ZNodeLabel.Path>();
    }
    
    public ZNodeLabelTrie<VolumeLookupNode> asTrie() {
        return lookupTrie;
    }
    
    public VolumeAssignment get(ZNodeLabel label) {
        VolumeLookupNode node = asTrie().longestPrefix(label);
        VolumeAssignment assignment = node.get();
        while ((assignment == null) && node.parent().isPresent()) {
            node = node.parent().orNull().get();
            assignment = node.get();
        }
        return assignment;
    }
    
    public ListenableFuture<VolumeAssignment> lookup(ZNodeLabel.Path input) {
        VolumeAssignment cached = get(input);
        return Futures.immediateFuture(cached);
    }
    
    public VolumeAssignment byVolumeId(Identifier id) {
        ZNodeLabel.Path path = byVolumeId.get(id);
        if (path != null) {
            VolumeLookupNode node = asTrie().get(path);
            if (node != null) {
                return node.get();
            }
        }
        return null;
    }
    
    @Override
    protected void startUp() throws Exception {
        new UpdateFromCache();
        
        // Global barrier - Wait for all volumes to be assigned
        Predicate<Materializer<?,?>> allAssigned = new Predicate<Materializer<?,?>>() {
            @Override
            public boolean apply(@Nullable Materializer<?,?> input) {
                ZNodeLabel.Component label = Orchestra.Volumes.Entity.Ensemble.LABEL;
                boolean done = true;
                for (Materializer.MaterializedNode e: input.get(VOLUMES_PATH).values()) {
                    if (! e.containsKey(label)) {
                        done = false;
                        break;
                    }
                }
                return done;
            }
        };
        Control.FetchUntil.newInstance(VOLUMES_PATH, allAssigned, controlClient.materializer(), MoreExecutors.sameThreadExecutor()).get();

        if (logger.isInfoEnabled()) {
            for (Identifier volumeId: byVolumeId.keySet()) {
                VolumeAssignment assignment = byVolumeId(volumeId);
                if (assignment != null) {
                    logger.info("{}", assignment);
                }
            }
        }
    }

    @Override
    protected void shutDown() throws Exception {
    }
    
    protected class UpdateFromCache {
        
        protected UpdateFromCache() {
            controlClient.register(this);
            
            initialize();
        }
        
        protected void initialize() {
            Materializer.MaterializedNode volumes = controlClient.materializer().get(VOLUMES_PATH);
            if (volumes != null) {
                for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> child: volumes.entrySet()) {
                    Identifier volumeId = Identifier.valueOf(child.getKey().toString());
                    ZNodeLabel.Path volumeRoot = byVolumeId.get(volumeId);
                    VolumeLookupNode lookupNode = (volumeRoot == null) ? null : asTrie().root().add(volumeRoot);
                    Materializer.MaterializedNode volumeChild = child.getValue().get(Orchestra.Volumes.Entity.Volume.LABEL);
                    if (volumeChild != null) {
                        VolumeDescriptor volumeDescriptor = (VolumeDescriptor) volumeChild.get().get();
                        if (volumeDescriptor != null) {
                            if (volumeRoot == null) {
                                volumeRoot = volumeDescriptor.getRoot();
                                byVolumeId.putIfAbsent(volumeId, volumeRoot);
                                lookupNode = asTrie().root().add(volumeRoot);
                            }
                        }
                        if (lookupNode != null) {
                            lookupNode.setVolume(Volume.of(volumeId, volumeDescriptor));
                        }
                    }
                    if (lookupNode != null) {
                        Materializer.MaterializedNode assignmentChild = child.getValue().get(Orchestra.Volumes.Entity.Ensemble.LABEL);
                        if (assignmentChild != null) {
                            Identifier assignment = (Identifier) assignmentChild.get().get();
                            if (assignment != null) {
                                lookupNode.setAssignment(assignment);
                            }
                        }
                    }
                }
            }
        }
        
        @Subscribe
        public void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
            ZNodeLabel.Path path = event.path().get();
            if (ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED != event.type() 
                    || ! VOLUMES_PATH.prefixOf(path)) {
                return;
            }
            
            if (VOLUMES_PATH.equals(path)) {
                asTrie().clear();
                byVolumeId.clear();
            } else {
                ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
                ZNodeLabel.Component pathTail = path.tail();
                if (VOLUMES_PATH.equals(pathHead)) {
                    Identifier volumeId = Identifier.valueOf(pathTail.toString());
                    ZNodeLabel.Path volumeRoot = byVolumeId.remove(volumeId);
                    if (volumeRoot != null) {
                        VolumeLookupNode lookupNode = asTrie().get(volumeRoot);
                        if (lookupNode != null) {
                            lookupNode.getAndSet(null);
                        }
                    }
                } else {
                    Identifier volumeId = Identifier.valueOf(pathHead.tail().toString());
                    ZNodeLabel.Path volumeRoot = byVolumeId.get(volumeId);
                    if (volumeRoot != null) {
                        VolumeLookupNode lookupNode = asTrie().get(volumeRoot);
                        if (lookupNode != null) {
                            if (Orchestra.Volumes.Entity.Volume.LABEL.equals(pathTail)) {
                                lookupNode.setVolume(Volume.of(volumeId, VolumeDescriptor.of(volumeRoot)));
                            } else if (Orchestra.Volumes.Entity.Ensemble.LABEL.equals(pathTail)) {
                                if (lookupNode != null) {
                                    lookupNode.setAssignment(null);
                                }
                            } else {
                                throw new AssertionError(path);
                            }
                        }
                    }
                }
            }
        }

        @Subscribe
        public void handleViewUpdate(ZNodeViewCache.ViewUpdate event) {
            ZNodeLabel.Path path = event.path();
            if (ZNodeViewCache.View.DATA != event.view() 
                    || ! VOLUMES_PATH.prefixOf(path)
                    || VOLUMES_PATH.equals(path)) {
                return;
            }
            
            Materializer.MaterializedNode node = controlClient.materializer().get(path);
            ZNodeLabelTrie.Pointer<Materializer.MaterializedNode> pointer = node.parent().get();
            if (! VOLUMES_PATH.equals(pointer.get().path())) {
                Identifier volumeId = Identifier.valueOf(pointer.get().parent().get().label().toString());
                ZNodeLabel.Path volumeRoot = byVolumeId.get(volumeId);
                VolumeLookupNode lookupNode = (volumeRoot != null) ? asTrie().get(volumeRoot) : null;
                if (Orchestra.Volumes.Entity.Volume.LABEL.equals(pointer.label())) {
                    VolumeDescriptor volumeDescriptor = (VolumeDescriptor) node.get().get();
                    if (volumeRoot == null) {
                        volumeRoot = volumeDescriptor.getRoot();
                        byVolumeId.putIfAbsent(volumeId, volumeRoot);
                    }
                    if (lookupNode == null) {
                        lookupNode = asTrie().root().add(volumeRoot);
                    }
                    Volume volume = Volume.of(volumeId, volumeDescriptor);
                    VolumeAssignment prev = lookupNode.setVolume(volume);
                    if (logger.isInfoEnabled()) {
                        if (prev == null || ! Objects.equal(volume, prev.getVolume())) {
                            logger.info("{} -> {}", prev, lookupNode.get());
                        }
                    }
                    // we might have been unable to determine the assignment before now
                    Materializer.MaterializedNode assignmentNode = pointer.get().get(Orchestra.Volumes.Entity.Ensemble.LABEL);
                    if (assignmentNode != null) {
                        Identifier assignment = (Identifier) assignmentNode.get().get();
                        prev = lookupNode.setAssignment(assignment);
                        if (logger.isInfoEnabled()) {
                            if (prev == null || ! Objects.equal(prev.getAssignment(), assignment)) {
                                logger.info("{} -> {}", prev, lookupNode.get());
                            }
                        }
                    }
                } else if (Orchestra.Volumes.Entity.Ensemble.LABEL.equals(pointer.label())) {
                    if (lookupNode != null) {
                        Identifier assignment = (Identifier) node.get().get();
                        VolumeAssignment prev = lookupNode.setAssignment(assignment);
                        if (logger.isInfoEnabled()) {
                            if (prev == null || ! Objects.equal(prev.getAssignment(), assignment)) {
                                logger.info("{} -> {}", prev, lookupNode.get());
                            }
                        }
                    }
                } else {
                    throw new AssertionError(path);
                }
            }
        }
    }
}
