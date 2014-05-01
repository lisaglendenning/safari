package edu.uw.zookeeper.safari.data;

import org.apache.zookeeper.Watcher;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.control.ControlZNode;


/**
 * Listens for new directory nodes in the cache.
 */
public abstract class CacheNodeCreatedListener extends AbstractWatchListener {

    protected final LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache;
    
    protected CacheNodeCreatedListener(
            AbsoluteZNodePath path,
            Service service,
            WatchListeners watch,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache) {
        super(service, watch, WatchMatcher.exact(
                path,
                Watcher.Event.EventType.NodeCreated));
        this.cache = cache;
    }

    @Override
    public void running() {
        cache.lock().readLock().lock();
        try {
            final ControlZNode<?> parent = cache.cache().get(((AbsoluteZNodePath) getWatchMatcher().getPath()).parent());
            if (parent != null) {
                for (ControlZNode<?> node : parent.values()) {
                    handleWatchEvent(NodeWatchEvent.nodeCreated(node.path()));
                }
            }
        } finally {
            cache.lock().readLock().unlock();
        }
    }
}
