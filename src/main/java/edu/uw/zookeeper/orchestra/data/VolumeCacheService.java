package edu.uw.zookeeper.orchestra.data;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.common.CachedFunction;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class VolumeCacheService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public VolumeCacheService getVolumeLookupService(
                VolumeCache cache,
                ControlMaterializerService<?> controlClient,
                ServiceMonitor monitor) {
            return monitor.addOnStart(
                    newInstance(controlClient.materializer(), cache));
        }

        @Provides @Singleton
        public VolumeCache getVolumeCache() {
            return VolumeCache.newInstance();
        }
    }
    
    public static VolumeCacheService newInstance(
            Materializer<?> client,
            VolumeCache cache) {
        return new VolumeCacheService(client, cache);
    }
    
    protected static final ZNodeLabel.Path VOLUMES_PATH = Control.path(ControlSchema.Volumes.class);

    protected final Logger logger;
    protected final Materializer<?> materializer;
    protected final VolumeCache cache;
    protected final CachedFunction<ZNodeLabel.Path, Volume> lookup;
    
    protected VolumeCacheService(
            final Materializer<?> materializer,
            final VolumeCache cache) {
        this.logger = LogManager.getLogger(getClass());
        this.materializer = materializer;
        this.cache = cache;
        this.lookup = CachedFunction.create(
                new Function<ZNodeLabel.Path, Volume>() {
                    @Override
                    public @Nullable
                    Volume apply(@Nullable ZNodeLabel.Path input) {
                        return cache.get(input);
                    }
                },
                new AsyncFunction<ZNodeLabel.Path, Volume>() {
                    @Override
                    public ListenableFuture<Volume> apply(
                            final ZNodeLabel.Path path)
                            throws Exception {
                        // since we can't hash on an arbitrary path,
                        // we must do a scan
                        Processor<Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<Volume>> processor = new Processor<Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<Volume>>() {
                            @Override
                            public Optional<Volume> apply(Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>> input)
                                    throws Exception {
                                Optional<Volume> result = Optional.fromNullable(cache.get(path));
                                // UNFORTUNATELY...the cache doesn't get updated automagically until after this message is processed
                                // so we need to dig into the message to see if it is the information we want
                                // and update the cache ourselves...
                                if (!result.isPresent() && input.isPresent()) {
                                    Records.Request request = input.get().first();
                                    if (request.opcode() == OpCode.GET_DATA) {
                                        ZNodeLabel.Path requestPath = ZNodeLabel.Path.of(((Records.PathGetter) request).getPath());
                                        Schema.SchemaNode schemaNode = ControlSchema.getInstance().get().match(requestPath);
                                        if (schemaNode == ControlSchema.getInstance().byElement(ControlSchema.Volumes.Entity.Volume.class)) {
                                            Materializer.MaterializedNode node = materializer.get(requestPath);
                                            if ((node != null) && (node.get() != null)) {
                                                VolumeDescriptor v = (VolumeDescriptor) node.get().get();
                                                if ((v != null) && v.contains(path)) {
                                                    Identifier id = Identifier.valueOf(((ZNodeLabel.Path) requestPath.head()).tail().toString());
                                                    Volume volume = Volume.of(id, v);
                                                    cache.put(volume);
                                                    result = Optional.of(volume);
                                                }
                                            }
                                        }
                                    }
                                }
                                return result;
                            }
                        };
                        return Control.FetchUntil.newInstance(VOLUMES_PATH, processor, materializer);
                    }
                });
    }
    
    public VolumeCache asCache() {
        return cache;
    }
    
    public CachedFunction<ZNodeLabel.Path, Volume> asLookup() {
        return lookup;
    }
    
    @Subscribe
    public void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
        ZNodeLabel.Path path = event.path().get();
        if ((ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED != event.type()) 
                || ! VOLUMES_PATH.prefixOf(path)) {
            return;
        }
        
        if (VOLUMES_PATH.equals(path)) {
            cache.clear();
        } else {
            ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
            ZNodeLabel pathTail = path.tail();
            if (VOLUMES_PATH.equals(pathHead)) {
                Identifier volumeId = Identifier.valueOf(pathTail.toString());
                cache.remove(volumeId);
            } else {
                Identifier volumeId = Identifier.valueOf(pathHead.tail().toString());
                if (ControlSchema.Volumes.Entity.Volume.LABEL.equals(pathTail)) {
                    cache.remove(volumeId);
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
        
        Materializer.MaterializedNode node = materializer.get(path);
        ZNodeLabelTrie.Pointer<Materializer.MaterializedNode> pointer = node.parent().get();
        if (! VOLUMES_PATH.equals(pointer.get().path())) {
            Identifier volumeId = Identifier.valueOf(pointer.get().parent().get().label().toString());
            if (ControlSchema.Volumes.Entity.Volume.LABEL.equals(pointer.label())) {
                VolumeDescriptor volumeDescriptor = (VolumeDescriptor) node.get().get();
                Volume volume = Volume.of(volumeId, volumeDescriptor);
                Volume prev = (volumeDescriptor == null) ? cache.remove(volumeId) : cache.put(volume);
                if (logger.isInfoEnabled()) {
                    if (! Objects.equal(volume, prev)) {
                        logger.info("Volume updated to {} (previously {})", volume, prev);
                    }
                }
            }
        }
    }
    
    @Override
    protected void startUp() throws Exception {
        materializer.register(this);
        
        Materializer.MaterializedNode volumes = materializer.get(VOLUMES_PATH);
        if (volumes != null) {
            for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> child: volumes.entrySet()) {
                Identifier volumeId = Identifier.valueOf(child.getKey().toString());
                Materializer.MaterializedNode volumeChild = child.getValue().get(ControlSchema.Volumes.Entity.Volume.LABEL);
                if (volumeChild != null) {
                    VolumeDescriptor volumeDescriptor = (VolumeDescriptor) volumeChild.get().get();
                    if (volumeDescriptor != null) {
                        Volume volume = Volume.of(volumeId, volumeDescriptor);
                        cache.put(volume);
                    }
                }
            }
        }
        
        logger.info("{}", cache);
    }

    @Override
    protected void shutDown() throws Exception {
        materializer.unregister(this);
    }
}
