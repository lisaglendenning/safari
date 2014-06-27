package edu.uw.zookeeper.safari.data;

import org.apache.zookeeper.Watcher;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;


/**
 * Listens for new volume versions in the cache.
 */
public class VolumeVersionCreatedListener<O extends Operation.ProtocolResponse<?>> extends AbstractWatchListener {
    
    public static <O extends Operation.ProtocolResponse<?>> VolumeVersionCreatedListener<O> listen(
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        VolumeVersionCreatedListener<O> listener = 
                new VolumeVersionCreatedListener<O>(client, cache, service, watch);
        listener.listen();
        return listener;
    }
    
    protected final LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache;
    protected final PathToQuery<?,?> query;
    
    @SuppressWarnings("unchecked")
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
        this.query = PathToQuery.forRequests(
                client,
                Operations.Requests.sync(),
                Operations.Requests.getChildren(),
                Operations.Requests.sync(),
                Operations.Requests.getData());
    }
    
    @Override
    public void handleWatchEvent(WatchEvent event) {
        if (isRunning()) {
            query.apply(event.getPath().join(ControlSchema.Safari.Volumes.Volume.Log.Version.State.LABEL)).call();
        }
    }
    
    @Override
    public void running() {
        cache.lock().readLock().lock();
        try {
            final ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.fromTrie(
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
}
