package edu.uw.zookeeper.safari.data;

import java.util.List;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;


/**
 * Listens for new volume versions in the cache.
 */
public class VolumeVersionCreatedListener<O extends Operation.ProtocolResponse<?>> extends AbstractWatchListener {
    
    public static <O extends Operation.ProtocolResponse<?>> VolumeVersionCreatedListener<O> newInstance(
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        VolumeVersionCreatedListener<O> instance = new VolumeVersionCreatedListener<O>(client, cache, service, watch);
        service.addListener(instance, SameThreadExecutor.getInstance());
        if (service.isRunning()) {
            instance.starting();
        }
        return instance;
    }
    
    protected final LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache;
    protected final PathToQuery<VolumeStateQuery,?> query;
    
    public VolumeVersionCreatedListener(
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        super(service, watch, WatchMatcher.exact(
                ControlSchema.Safari.Volumes.Volume.Log.Version.PATH,
                Watcher.Event.EventType.NodeCreated,
                Watcher.Event.EventType.NodeChildrenChanged));
        this.cache = cache;
        this.query = PathToQuery.forFunction(
                client,
                new VolumeStateQuery());
    }
    
    @Override
    public void handleWatchEvent(WatchEvent event) {
        if (service.isRunning()) {
            query.apply(event.getPath()).call();
        }
    }
    
    @Override
    public void running() {
        cache.lock().readLock().lock();
        try {
            final ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.get(
                    cache.cache());
            if (volumes != null) {
                for (ControlZNode<?> v : volumes.values()) {
                    ControlSchema.Safari.Volumes.Volume.Log log = ((ControlSchema.Safari.Volumes.Volume) v).getLog();
                    if (log != null) {
                        for (ControlZNode<?> node : log.values()) {
                            handleWatchEvent(NodeWatchEvent.nodeCreated(node.path()));
                        }
                    }
                }
            }
        } finally {
            cache.lock().readLock().unlock();
        }
    }
    
    protected static final class VolumeStateQuery implements Function<ZNodePath, List<? extends Records.Request>> {

        private final Operations.Requests.Sync sync;
        private final Operations.Requests.GetChildren getChildren;
        private final Operations.Requests.GetData getData;
        
        public VolumeStateQuery() {
            this.sync = Operations.Requests.sync();
            this.getChildren = Operations.Requests.getChildren();
            this.getData = Operations.Requests.getData();
        }
        
        @Override
        public List<? extends Records.Request> apply(final ZNodePath input) {
            final ZNodePath path = input.join(ControlSchema.Safari.Volumes.Volume.Log.Version.State.LABEL);
            return ImmutableList.of(
                    sync.setPath(input).build(),
                    getChildren.setPath(input).build(),
                    sync.setPath(path).build(),
                    getData.setPath(path).build());
        }
    }
}
