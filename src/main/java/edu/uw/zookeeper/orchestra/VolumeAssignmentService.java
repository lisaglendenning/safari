package edu.uw.zookeeper.orchestra;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.orchestra.VolumeLookup.VolumeLookupNode;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.util.Reference;

@DependsOn(ControlMaterializerService.class)
public class VolumeAssignmentService extends AbstractIdleService implements Reference<VolumeLookup> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public VolumeAssignmentService getVolumeLookupService(
                ControlMaterializerService<?> controlClient,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            VolumeAssignmentService instance = new VolumeAssignmentService(controlClient.materializer());
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    public VolumeAssignmentService newInstance(
            Materializer<?,?> client) {
        return new VolumeAssignmentService(client);
    }
    
    private static final ZNodeLabel.Path VOLUMES_PATH = Control.path(Orchestra.Volumes.class);

    protected final Logger logger;
    protected final Materializer<?,?> client;
    protected final VolumeLookup lookup;
    protected final ConcurrentMap<Identifier, Identifier> assignments;
    protected final UpdateFromCache updater;
    
    public VolumeAssignmentService(
            Materializer<?,?> client) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.client = client;
        this.lookup = VolumeLookup.newInstance();
        this.assignments = new MapMaker().makeMap();
        this.updater = new UpdateFromCache();
    }
    
    @Override
    public VolumeLookup get() {
        return lookup;
    }

    public VolumeAssignment get(ZNodeLabel label) {
        // TODO
        VolumeLookupNode node = get().asTrie().longestPrefix(label);
        Volume volume = node.get();
        while ((volume == null) && node.parent().isPresent()) {
            node = node.parent().orNull().get();
            volume = node.get();
        }
        return null;
    }

    public ListenableFuture<VolumeAssignment> lookup(ZNodeLabel.Path input) {
        // TODO
        VolumeAssignment cached = get(input);
        return Futures.immediateFuture(cached);
    }
    
    @Override
    protected void startUp() throws Exception {
        updater.initialize();
        
        if (logger.isInfoEnabled()) {
            for (Identifier volumeId: get().getVolumeIds()) {
                Volume volume = get().get(volumeId);
                if (volume != null) {
                    Identifier assignment = assignments.get(volumeId);
                    logger.info("{}", VolumeAssignment.of(volume, assignment));
                }
            }
        }
    }

    @Override
    protected void shutDown() throws Exception {
    }
    
    protected class UpdateFromCache {
        
        public UpdateFromCache() {}
        
        public void initialize() {
            client.register(this);
            
            Materializer.MaterializedNode volumes = client.get(VOLUMES_PATH);
            if (volumes != null) {
                for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> child: volumes.entrySet()) {
                    Identifier volumeId = Identifier.valueOf(child.getKey().toString());
                    Materializer.MaterializedNode volumeChild = child.getValue().get(Orchestra.Volumes.Entity.Volume.LABEL);
                    if (volumeChild != null) {
                        VolumeDescriptor volumeDescriptor = (VolumeDescriptor) volumeChild.get().get();
                        if (volumeDescriptor != null) {
                            Volume volume = Volume.of(volumeId, volumeDescriptor);
                            get().put(volume);
                        }
                    }
                    Materializer.MaterializedNode assignmentChild = child.getValue().get(Orchestra.Volumes.Entity.Ensemble.LABEL);
                    if (assignmentChild != null) {
                        Identifier assignment = (Identifier) assignmentChild.get().get();
                        if (assignment != null) {
                            assignments.put(volumeId, assignment);
                        }
                    }
                }
            }
        }
        
        @Subscribe
        public void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
            ZNodeLabel.Path path = event.path().get();
            if ((ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED != event.type()) 
                    || ! VOLUMES_PATH.prefixOf(path)) {
                return;
            }
            
            if (VOLUMES_PATH.equals(path)) {
                get().clear();
                assignments.clear();
            } else {
                ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
                ZNodeLabel.Component pathTail = path.tail();
                if (VOLUMES_PATH.equals(pathHead)) {
                    Identifier volumeId = Identifier.valueOf(pathTail.toString());
                    assignments.remove(volumeId);
                    get().remove(volumeId);
                } else {
                    Identifier volumeId = Identifier.valueOf(pathHead.tail().toString());
                    if (Orchestra.Volumes.Entity.Volume.LABEL.equals(pathTail)) {
                        get().remove(volumeId);
                    } else if (Orchestra.Volumes.Entity.Ensemble.LABEL.equals(pathTail)) {
                        assignments.remove(volumeId);
                    } else {
                        throw new AssertionError(path);
                    }
                }
            }
        }

        @Subscribe
        public void handleViewUpdate(ZNodeViewCache.ViewUpdate event) {
            ZNodeLabel.Path path = event.path();
            if ((ZNodeViewCache.View.DATA != event.view())
                    || ! VOLUMES_PATH.prefixOf(path)
                    || VOLUMES_PATH.equals(path)) {
                return;
            }
            
            Materializer.MaterializedNode node = client.get(path);
            ZNodeLabelTrie.Pointer<Materializer.MaterializedNode> pointer = node.parent().get();
            if (! VOLUMES_PATH.equals(pointer.get().path())) {
                Identifier volumeId = Identifier.valueOf(pointer.get().parent().get().label().toString());
                if (Orchestra.Volumes.Entity.Volume.LABEL.equals(pointer.label())) {
                    VolumeDescriptor volumeDescriptor = (VolumeDescriptor) node.get().get();
                    Volume volume = Volume.of(volumeId, volumeDescriptor);
                    Volume prev = (volumeDescriptor == null) ? get().remove(volumeId) : get().put(volume);
                    if (logger.isInfoEnabled()) {
                        if (! Objects.equal(volume, prev)) {
                            logger.info("{} -> {}", prev, volume);
                        }
                    }
                } else if (Orchestra.Volumes.Entity.Ensemble.LABEL.equals(pointer.label())) {
                    Identifier assignment = (Identifier) node.get().get();
                    Identifier prevAssignment = (assignment == null) ? assignments.remove(volumeId) : assignments.put(volumeId, assignment);
                    if (logger.isInfoEnabled()) {
                        if (! Objects.equal(prevAssignment, assignment)) {
                            logger.info("Assignment ({}) {} -> {}", volumeId, prevAssignment, assignment);
                        }
                    }
                } else {
                    throw new AssertionError(path);
                }
            }
        }
    }
}
