package edu.uw.zookeeper.orchestra;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
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
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.util.Reference;

@DependsOn(ControlMaterializerService.class)
public class VolumeLookupService extends AbstractIdleService implements Reference<VolumeLookup> {

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
                ControlMaterializerService<?> controlClient,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            VolumeLookupService instance = new VolumeLookupService(controlClient);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    public VolumeLookupService newInstance(
            ControlMaterializerService<?> controlClient) {
        return new VolumeLookupService(controlClient);
    }
    
    private static final ZNodeLabel.Path VOLUMES_PATH = Control.path(Orchestra.Volumes.class);

    protected final Logger logger;
    protected final ControlMaterializerService<?> controlClient;
    protected final VolumeLookup lookup;
    protected final UpdateFromCache updater;
    
    public VolumeLookupService(
            ControlMaterializerService<?> controlClient) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.controlClient = controlClient;
        this.lookup = VolumeLookup.newInstance();
        this.updater = new UpdateFromCache();
    }
    
    @Override
    public VolumeLookup get() {
        return lookup;
    }
    
    public ListenableFuture<VolumeAssignment> lookup(ZNodeLabel.Path input) {
        VolumeAssignment cached = get().get(input);
        return Futures.immediateFuture(cached);
    }
    
    @Override
    protected void startUp() throws Exception {
        updater.initialize();
        
        if (logger.isInfoEnabled()) {
            for (Identifier volumeId: get().getVolumeIds()) {
                VolumeAssignment assignment = get().byVolumeId(volumeId);
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
        
        public UpdateFromCache() {}
        
        public void initialize() {
            controlClient.register(this);
            
            Materializer.MaterializedNode volumes = controlClient.materializer().get(VOLUMES_PATH);
            if (volumes != null) {
                for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> child: volumes.entrySet()) {
                    Identifier volumeId = Identifier.valueOf(child.getKey().toString());
                    ZNodeLabel.Path volumeRoot = get().getVolumeRoot(volumeId);
                    VolumeLookup.VolumeLookupNode lookupNode = (volumeRoot == null) ? null : get().asTrie().root().add(volumeRoot);
                    Materializer.MaterializedNode volumeChild = child.getValue().get(Orchestra.Volumes.Entity.Volume.LABEL);
                    if (volumeChild != null) {
                        VolumeDescriptor volumeDescriptor = (VolumeDescriptor) volumeChild.get().get();
                        if (volumeDescriptor != null) {
                            if (volumeRoot == null) {
                                volumeRoot = volumeDescriptor.getRoot();
                                get().putVolumeRoot(volumeId, volumeRoot);
                                lookupNode = get().asTrie().root().add(volumeRoot);
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
                get().clear();
            } else {
                ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
                ZNodeLabel.Component pathTail = path.tail();
                if (VOLUMES_PATH.equals(pathHead)) {
                    Identifier volumeId = Identifier.valueOf(pathTail.toString());
                    ZNodeLabel.Path volumeRoot = get().removeVolumeRoot(volumeId);
                    if (volumeRoot != null) {
                        VolumeLookup.VolumeLookupNode lookupNode = get().asTrie().get(volumeRoot);
                        if (lookupNode != null) {
                            lookupNode.getAndSet(null);
                        }
                    }
                } else {
                    Identifier volumeId = Identifier.valueOf(pathHead.tail().toString());
                    ZNodeLabel.Path volumeRoot = get().getVolumeRoot(volumeId);
                    if (volumeRoot != null) {
                        VolumeLookup.VolumeLookupNode lookupNode = get().asTrie().get(volumeRoot);
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
                ZNodeLabel.Path volumeRoot = get().getVolumeRoot(volumeId);
                VolumeLookup.VolumeLookupNode lookupNode = (volumeRoot != null) ? get().asTrie().get(volumeRoot) : null;
                if (Orchestra.Volumes.Entity.Volume.LABEL.equals(pointer.label())) {
                    VolumeDescriptor volumeDescriptor = (VolumeDescriptor) node.get().get();
                    if (volumeRoot == null) {
                        volumeRoot = volumeDescriptor.getRoot();
                        get().putVolumeRoot(volumeId, volumeRoot);
                    }
                    if (lookupNode == null) {
                        lookupNode = get().asTrie().root().add(volumeRoot);
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
