package edu.uw.zookeeper.safari.backend;

import org.apache.zookeeper.Watcher;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.CacheNodeCreatedListener;
import edu.uw.zookeeper.safari.data.PathToQuery;
import edu.uw.zookeeper.safari.data.PathToQueryWatcher;
import edu.uw.zookeeper.safari.data.VolumeVersionCreatedListener;

/**
 * Assumes functionality of LatestVolumesWatcher is already covered.
 */
public class VersionVolumesWatcher extends CacheNodeCreatedListener {

    public static VersionVolumesWatcher newInstance(
            Service service,
            ControlMaterializerService control) {
        newLeaseXomegaListener(control, control.materializer().cache(), service, control.cacheEvents());
        VersionVolumesWatcher instance = new VersionVolumesWatcher(
                newVolumeLogWatcher(service, control.notifications(), control),
                service,
                control.cacheEvents(), 
                control.materializer().cache());
        service.addListener(instance, SameThreadExecutor.getInstance());
        if (service.isRunning()) {
            instance.starting();
            instance.running();
        }
        return instance;
    }

    public static PathToQueryWatcher<?,?> newVolumeLogWatcher(
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,?,?> client) {
        final WatchMatcher matcher = WatchMatcher.exact(
                ControlSchema.Safari.Volumes.Volume.Log.PATH,
                Watcher.Event.EventType.NodeCreated, 
                Watcher.Event.EventType.NodeChildrenChanged);
        @SuppressWarnings("unchecked")
        PathToQuery<?,?> query = PathToQuery.forRequests(
                client, 
                Operations.Requests.sync(),
                Operations.Requests.getChildren().setWatch(true));
        return PathToQueryWatcher.newInstance(service, watch, matcher, query);
    }

    public static PathToQueryWatcher<?,?> newVolumeLeaseWatcher(
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,?,?> client) {
        final WatchMatcher matcher = WatchMatcher.exact(
                ControlSchema.Safari.Volumes.Volume.Log.Version.Lease.PATH,
                Watcher.Event.EventType.NodeCreated, 
                Watcher.Event.EventType.NodeDataChanged);
        @SuppressWarnings("unchecked")
        PathToQuery<?,?> query = PathToQuery.forRequests(
                client, 
                Operations.Requests.sync(),
                Operations.Requests.getData().setWatch(true));
        return PathToQueryWatcher.newInstance(service, watch, matcher, query);
    }

    public static PathToQueryWatcher<?,?> newVolumeXomegaWatcher(
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,?,?> client) {
        final WatchMatcher matcher = WatchMatcher.exact(
                ControlSchema.Safari.Volumes.Volume.Log.Version.Xomega.PATH,
                Watcher.Event.EventType.NodeCreated, 
                Watcher.Event.EventType.NodeDataChanged);
        @SuppressWarnings("unchecked")
        PathToQuery<?,?> query = PathToQuery.forRequests(
                client, 
                Operations.Requests.sync(),
                Operations.Requests.getData().setWatch(true));
        return PathToQueryWatcher.newInstance(service, watch, matcher, query);
    }
    
    public static <O extends Operation.ProtocolResponse<?>> LeaseXomegaListener<O> newLeaseXomegaListener(
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        return LeaseXomegaListener.newInstance(
                client, cache, service, watch);
    }
    
    public static final class LeaseXomegaListener<O extends Operation.ProtocolResponse<?>> extends VolumeVersionCreatedListener<O> {
        
        public static <O extends Operation.ProtocolResponse<?>> LeaseXomegaListener<O> newInstance(
                ClientExecutor<? super Records.Request,O,?> client,
                LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
                Service service,
                WatchListeners watch) {
            LeaseXomegaListener<O> instance = new LeaseXomegaListener<O>(
                    newVolumeLeaseWatcher(service, watch, client),
                    newVolumeXomegaWatcher(service, watch, client),
                    client, cache, service, watch);
            service.addListener(instance, SameThreadExecutor.getInstance());
            if (service.isRunning()) {
                instance.starting();
            }
            return instance;
        }
        
        protected final PathToQueryWatcher<?,?> leaseWatcher;
        protected final PathToQueryWatcher<?,?> xomegaWatcher;
        
        protected LeaseXomegaListener(
                PathToQueryWatcher<?,?> leaseWatcher,
                PathToQueryWatcher<?,?> xomegaWatcher,
                ClientExecutor<? super Records.Request, O, ?> client,
                LockableZNodeCache<ControlZNode<?>, Records.Request, ?> cache,
                Service service, WatchListeners watch) {
            super(client, cache, service, watch);
            this.leaseWatcher = leaseWatcher;
            this.xomegaWatcher = xomegaWatcher;
        }
    
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            if (service.isRunning()) {
                leaseWatcher.handleWatchEvent(
                        NodeWatchEvent.nodeCreated(
                                event.getPath().join(
                                        ControlSchema.Safari.Volumes.Volume.Log.Version.Lease.LABEL)));
                xomegaWatcher.handleWatchEvent(
                        NodeWatchEvent.nodeCreated(
                                event.getPath().join(
                                        ControlSchema.Safari.Volumes.Volume.Log.Version.Xomega.LABEL)));
            }
        }
    }

    private final PathToQueryWatcher<?,?> logWatcher;

    protected VersionVolumesWatcher(
            PathToQueryWatcher<?,?> logWatcher,
            Service service,
            WatchListeners watch,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache) {
        super(ControlSchema.Safari.Volumes.Volume.PATH, service, watch, cache);
        this.logWatcher = logWatcher;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        logWatcher.handleWatchEvent(event);
    }
}
