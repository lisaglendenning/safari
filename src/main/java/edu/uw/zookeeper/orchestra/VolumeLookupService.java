package edu.uw.zookeeper.orchestra;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.ServiceMonitor;

@DependsOn(ControlMaterializerService.class)
public class VolumeLookupService extends AbstractIdleService implements Reference<VolumeCache> {

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
                ServiceMonitor monitor) throws InterruptedException, ExecutionException, KeeperException {
            VolumeLookupService instance = new VolumeLookupService(controlClient.materializer());
            monitor.addOnStart(instance);
            return instance;
        }
    }
    
    public VolumeLookupService newInstance(
            Materializer<?,?> client) {
        return new VolumeLookupService(client);
    }
    
    protected static final ZNodeLabel.Path VOLUMES_PATH = Control.path(Orchestra.Volumes.class);

    protected final Logger logger;
    protected final Materializer<?,?> client;
    protected final VolumeCache volumes;
    protected final CachedFunction<ZNodeLabel.Path, Volume> lookup;
    protected final UpdateFromCache updater;
    
    public VolumeLookupService(
            Materializer<?,?> client) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.client = client;
        this.volumes = VolumeCache.newInstance();
        this.lookup = CachedFunction.create(
                new Function<ZNodeLabel.Path, Volume>() {
                    @Override
                    public @Nullable
                    Volume apply(@Nullable ZNodeLabel.Path input) {
                        return get().get(input);
                    }
                },
                new AsyncFunction<ZNodeLabel.Path, Volume>() {
                    @Override
                    public ListenableFuture<Volume> apply(ZNodeLabel.Path input)
                            throws Exception {
                        // FIXME
                        return Futures.immediateFuture(get().get(input));
                    }
                });
        this.updater = new UpdateFromCache();
    }
    
    @Override
    public VolumeCache get() {
        return volumes;
    }
    
    public CachedFunction<ZNodeLabel.Path, Volume> lookup() {
        return lookup;
    }

    @Override
    protected void startUp() throws Exception {
        updater.initialize();
        
        if (logger.isInfoEnabled()) {
            logger.info("{}", get());
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
            } else {
                ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
                ZNodeLabel.Component pathTail = path.tail();
                if (VOLUMES_PATH.equals(pathHead)) {
                    Identifier volumeId = Identifier.valueOf(pathTail.toString());
                    get().remove(volumeId);
                } else {
                    Identifier volumeId = Identifier.valueOf(pathHead.tail().toString());
                    if (Orchestra.Volumes.Entity.Volume.LABEL.equals(pathTail)) {
                        get().remove(volumeId);
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
                }
            }
        }
    }
}