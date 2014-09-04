package edu.uw.zookeeper.safari.region;

import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class RoleCacheListener extends LoggingWatchMatchListener {

    public static Watchers.CacheNodeCreatedListener<ControlZNode<?>> listen(
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            Identifier region,
            Automaton<RegionRole, LeaderEpoch> role,
            Service service,
            WatchListeners cacheEvents) {
        RoleCacheListener instance = new RoleCacheListener(cache, region, role);
        return Watchers.CacheNodeCreatedListener.listen(
                cache, 
                service, 
                cacheEvents, 
                instance, 
                instance.logger());
    }
    
    private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
    private final Automaton<RegionRole, LeaderEpoch> role;
    
    private RoleCacheListener(
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            Identifier region,
            Automaton<RegionRole, LeaderEpoch> role) {
        super(WatchMatcher.exact(
                ControlSchema.Safari.Regions.Region.Leader.pathOf(region), 
                EventType.NodeDeleted, EventType.NodeDataChanged));
        this.cache = cache;
        this.role = role;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        switch (event.getEventType()) {
        case NodeDataChanged:
        {
            cache.lock().readLock().lock();
            try {
                ControlSchema.Safari.Regions.Region.Leader node = (ControlSchema.Safari.Regions.Region.Leader) cache.cache().get(getWatchMatcher().getPath());
                if (node != null) {
                    Optional<LeaderEpoch> leader = node.getLeaderEpoch();
                    if (leader.isPresent()) {
                        role.apply(leader.get());
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
            break;
        }
        case NodeDeleted:
        {
            role.apply(null);
            break;
        }
        default:
            throw new AssertionError(String.valueOf(event));
        }
    }
}