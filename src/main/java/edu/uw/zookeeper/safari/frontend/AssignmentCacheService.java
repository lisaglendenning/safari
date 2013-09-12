package edu.uw.zookeeper.safari.frontend;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.CachedLookupService;
import edu.uw.zookeeper.safari.common.SharedLookup;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;

public class AssignmentCacheService extends CachedLookupService<Identifier,Identifier> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public AssignmentCacheService getVolumeAssignmentService(
                ControlMaterializerService control,
                ServiceMonitor monitor) {
            return monitor.addOnStart(
                    AssignmentCacheService.newInstance(
                            control.materializer()));
        }
    }
    
    public static AssignmentCacheService newInstance(
            final Materializer<?> materializer) {
        final ConcurrentMap<Identifier, Identifier> cache = new MapMaker().makeMap();
        CachedLookup<Identifier, Identifier> lookup = CachedLookup.create(
                cache,
                SharedLookup.create(
                        new AsyncFunction<Identifier, Identifier>() {
                            @Override
                            public ListenableFuture<Identifier> apply(final Identifier volume)
                                    throws Exception {
                                final ControlSchema.Volumes.Entity entity = ControlSchema.Volumes.Entity.of(volume);
                                final Processor<Object, Optional<Identifier>> processor = new Processor<Object, Optional<Identifier>>() {
                                    @Override
                                    public Optional<Identifier> apply(
                                            Object input) throws Exception {
                                        return Optional.fromNullable(cache.get(volume));
                                    }
                                };
                                return Control.FetchUntil.newInstance(
                                        (ZNodeLabel.Path) ZNodeLabel.joined(entity.path(), ControlSchema.Volumes.Entity.Ensemble.LABEL), 
                                                processor, materializer);
                            }
                        }));
        return new AssignmentCacheService(materializer, lookup);
    }
    
    protected static final ZNodeLabel.Path VOLUMES_PATH = Control.path(ControlSchema.Volumes.class);

    protected final Logger logger;
    
    protected AssignmentCacheService(
            Materializer<?> materializer,
            CachedLookup<Identifier, Identifier> cache) {
        super(materializer, cache);
        this.logger = LogManager.getLogger(getClass());
    }

    @Subscribe
    public void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
        ZNodeLabel.Path path = event.path().get();
        if ((ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED != event.type()) 
                || ! VOLUMES_PATH.prefixOf(path)) {
            return;
        }
        
        if (VOLUMES_PATH.equals(path)) {
            cache.asCache().clear();
        } else {
            ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
            ZNodeLabel pathTail = path.tail();
            if (VOLUMES_PATH.equals(pathHead)) {
                Identifier volumeId = Identifier.valueOf(pathTail.toString());
                cache.asCache().remove(volumeId);
            } else {
                Identifier volumeId = Identifier.valueOf(pathHead.tail().toString());
                if (ControlSchema.Volumes.Entity.Ensemble.LABEL.equals(pathTail)) {
                    cache.asCache().remove(volumeId);
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
            if (ControlSchema.Volumes.Entity.Ensemble.LABEL.equals(pointer.label())) {
                Identifier assignment = (Identifier) node.get().get();
                Identifier prevAssignment = (assignment == null) ? cache.asCache().remove(volumeId) : cache.asCache().put(volumeId, assignment);
                if (logger.isInfoEnabled()) {
                    if (! Objects.equal(prevAssignment, assignment)) {
                        logger.info("Volume {} assigned to {} (previously {})", volumeId, assignment, prevAssignment);
                    }
                }
            }
        }
    }
    
    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        Materializer.MaterializedNode volumes = materializer.get(VOLUMES_PATH);
        if (volumes != null) {
            for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> child: volumes.entrySet()) {
                Identifier volumeId = Identifier.valueOf(child.getKey().toString());
                Materializer.MaterializedNode assignmentChild = child.getValue().get(ControlSchema.Volumes.Entity.Ensemble.LABEL);
                if (assignmentChild != null) {
                    Identifier assignment = (Identifier) assignmentChild.get().get();
                    if (assignment != null) {
                        cache.asCache().put(volumeId, assignment);
                    }
                }
            }
        }
        
        logger.info("Volume assignments: {}", cache.asCache());
    }
}
