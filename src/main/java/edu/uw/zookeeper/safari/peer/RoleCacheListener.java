package edu.uw.zookeeper.safari.peer;

import java.util.EnumSet;

import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.data.AbstractWatchListener;

public class RoleCacheListener extends AbstractWatchListener {

    public static RoleCacheListener create(
            Service service,
            ControlMaterializerService control,
            Identifier region,
            Automaton<RegionRole, LeaderEpoch> role) {
        return new RoleCacheListener(service, control, region, role);
    }
    
    protected final ControlMaterializerService control;
    protected final Automaton<RegionRole, LeaderEpoch> role;
    
    protected RoleCacheListener(
            Service service,
            ControlMaterializerService control,
            Identifier region,
            Automaton<RegionRole, LeaderEpoch> role) {
        super(service, control.cacheEvents(),
                WatchMatcher.exact(ControlSchema.Safari.Regions.Region.Leader.pathOf(region), EnumSet.of(EventType.NodeDeleted, EventType.NodeDataChanged)));
        this.control = control;
        this.role = role;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        LeaderEpoch leader;
        switch (event.getEventType()) {
        case NodeDataChanged:
        {
            control.materializer().cache().lock().readLock().lock();
            try {
                leader = LeaderEpoch.fromMaterializer().apply(control.materializer().cache().cache().get(getWatchMatcher().getPath()));
            } finally {
                control.materializer().cache().lock().readLock().unlock();
            }
            assert (leader != null);
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