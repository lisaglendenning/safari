package edu.uw.zookeeper.safari.region;

import java.util.EnumSet;

import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.AbstractWatchListener;

public class RoleCacheListener extends AbstractWatchListener {

    public static RoleCacheListener create(
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            Identifier region,
            Automaton<RegionRole, LeaderEpoch> role,
            Service service,
            WatchListeners events) {
        return new RoleCacheListener(cache, region, role, service, events);
    }
    
    protected final LockableZNodeCache<ControlZNode<?>,?,?> cache;
    protected final Automaton<RegionRole, LeaderEpoch> role;
    
    protected RoleCacheListener(
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            Identifier region,
            Automaton<RegionRole, LeaderEpoch> role,
            Service service,
            WatchListeners events) {
        super(service, events,
                WatchMatcher.exact(ControlSchema.Safari.Regions.Region.Leader.pathOf(region), EnumSet.of(EventType.NodeDeleted, EventType.NodeDataChanged)));
        this.cache = cache;
        this.role = role;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        LeaderEpoch leader = null;
        switch (event.getEventType()) {
        case NodeDataChanged:
        {
            cache.lock().readLock().lock();
            try {
                ControlSchema.Safari.Regions.Region.Leader node = (ControlSchema.Safari.Regions.Region.Leader) cache.cache().get(getWatchMatcher().getPath());
                if (node != null) {
                    leader = node.getLeaderEpoch().get();
                }
            } finally {
                cache.lock().readLock().unlock();
            }
            break;
        }
        case NodeDeleted:
        {
            leader = null;
            break;
        }
        default:
            throw new AssertionError(String.valueOf(event));
        }
        role.apply(leader);
    }
}