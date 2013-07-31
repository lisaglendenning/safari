package edu.uw.zookeeper.orchestra;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.MapMaker;
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
import edu.uw.zookeeper.util.ServiceMonitor;

@DependsOn(ControlMaterializerService.class)
public class VolumeAssignmentService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public VolumeAssignmentService getVolumeAssignmentService(
                ControlMaterializerService<?> controlClient,
                ServiceMonitor monitor) throws InterruptedException, ExecutionException, KeeperException {
            VolumeAssignmentService instance = new VolumeAssignmentService(controlClient.materializer());
            monitor.addOnStart(instance);
            return instance;
        }
    }
    
    public VolumeAssignmentService newInstance(
            Materializer<?,?> client) {
        return new VolumeAssignmentService(client);
    }
    
    protected static final ZNodeLabel.Path VOLUMES_PATH = Control.path(Orchestra.Volumes.class);

    protected final Logger logger;
    protected final Materializer<?,?> client;
    protected final ConcurrentMap<Identifier, Identifier> assignments;
    protected final CachedFunction<Identifier, Identifier> lookup;
    protected final UpdateFromCache updater;
    
    public VolumeAssignmentService(
            Materializer<?,?> client) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.client = client;
        this.assignments = new MapMaker().makeMap();
        this.lookup = CachedFunction.create(
                new Function<Identifier, Identifier>() {
                    @Override
                    public @Nullable
                    Identifier apply(@Nullable Identifier input) {
                        return assignments.get(input);
                    }
                },
                new AsyncFunction<Identifier, Identifier>() {
                    @Override
                    public ListenableFuture<Identifier> apply(Identifier input)
                            throws Exception {
                        // FIXME
                        return Futures.immediateFuture(assignments.get(input));
                    }
                });
        this.updater = new UpdateFromCache();
    }

    public CachedFunction<Identifier, Identifier> lookup() {
        return lookup;
    }

    @Override
    protected void startUp() throws Exception {
        updater.initialize();
        
        if (logger.isInfoEnabled()) {
            logger.info("{}", assignments);
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
                assignments.clear();
            } else {
                ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
                ZNodeLabel.Component pathTail = path.tail();
                if (VOLUMES_PATH.equals(pathHead)) {
                    Identifier volumeId = Identifier.valueOf(pathTail.toString());
                    assignments.remove(volumeId);
                } else {
                    Identifier volumeId = Identifier.valueOf(pathHead.tail().toString());
                    if (Orchestra.Volumes.Entity.Ensemble.LABEL.equals(pathTail)) {
                        assignments.remove(volumeId);
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
                if (Orchestra.Volumes.Entity.Ensemble.LABEL.equals(pointer.label())) {
                    Identifier assignment = (Identifier) node.get().get();
                    Identifier prevAssignment = (assignment == null) ? assignments.remove(volumeId) : assignments.put(volumeId, assignment);
                    if (logger.isInfoEnabled()) {
                        if (! Objects.equal(prevAssignment, assignment)) {
                            logger.info("Assignment ({}) {} -> {}", volumeId, prevAssignment, assignment);
                        }
                    }
                }
            }
        }
    }
}
