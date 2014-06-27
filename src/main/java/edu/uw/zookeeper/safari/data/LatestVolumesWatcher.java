package edu.uw.zookeeper.safari.data;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.common.Call;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

/**
 * Watches and queries the path and latest state for all volumes;
 */
public final class LatestVolumesWatcher extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> {

    public static LatestVolumesWatcher listen(
            Service service,
            ControlClientService control) {
        newVolumeLatestListener(control.materializer(), control.materializer().cache(), service, control.cacheEvents());
        newVolumeDeletedWatcher(service, control.notifications(), control.materializer());
        LatestVolumesWatcher listener = new LatestVolumesWatcher(
                newVolumeLatestWatcher(service, control.notifications(), control.materializer()),
                control.materializer(), 
                control.materializer().cache(),
                service,
                control.cacheEvents());
        listener.listen();
        newVolumeDirectoryWatcher(service, control.notifications(), control.materializer());
        return listener;
    }
    
    public static Watchers.RunnableWatcher<?> newVolumeDirectoryWatcher(
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,?,?> client) {
        final AbsoluteZNodePath path = ControlSchema.Safari.Volumes.PATH;
        final WatchMatcher matcher = WatchMatcher.exact(
                path,
                Watcher.Event.EventType.NodeCreated,
                Watcher.Event.EventType.NodeChildrenChanged);
        @SuppressWarnings("unchecked")
        final FixedQuery<?> query = FixedQuery.forIterable(client, 
                PathToRequests.forRequests(
                        Operations.Requests.sync(),
                        Operations.Requests.getChildren().setWatch(true)).apply(path));
        return Watchers.RunnableWatcher.listen(Call.create(query), service, watch, matcher);
    }
    
    public static Watchers.PathToQueryWatcher<?,?> newVolumeDeletedWatcher(
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,?,?> client) {
        final WatchMatcher matcher = WatchMatcher.exact(
                ControlSchema.Safari.Volumes.Volume.PATH,
                Watcher.Event.EventType.NodeDeleted);
        @SuppressWarnings("unchecked")
        final PathToQuery<?,?> query = PathToQuery.forRequests(
                client, 
                Operations.Requests.sync(),
                Operations.Requests.exists());
        return Watchers.PathToQueryWatcher.listen(service, watch, matcher, query);
    }
    
    public static Watchers.PathToQueryWatcher<?,?> newVolumeLatestWatcher(
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,?,?> client) {
        final WatchMatcher matcher = WatchMatcher.exact(
                ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH,
                Watcher.Event.EventType.NodeCreated, 
                Watcher.Event.EventType.NodeDataChanged);
        @SuppressWarnings("unchecked")
        PathToQuery<?,?> query = PathToQuery.forRequests(
                client, 
                Operations.Requests.sync(),
                Operations.Requests.getData().setWatch(true));
        return Watchers.PathToQueryWatcher.listen(service, watch, matcher, query);
    }
    
    public static <O extends Operation.ProtocolResponse<?>> VolumeLatestListener<O> newVolumeLatestListener(
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        return VolumeLatestListener.listen(client, cache, service, watch);
    }
    
    private final Logger logger;
    private final PathToQuery<VolumePathQuery,?> query;
    private final Watchers.PathToQueryWatcher<?,?> latestWatcher;

    protected LatestVolumesWatcher(
            Watchers.PathToQueryWatcher<?,?> latestWatcher,
            ClientExecutor<? super Records.Request,?,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        super(ControlSchema.Safari.Volumes.Volume.PATH, cache, service, watch);
        this.logger = LogManager.getLogger(this);
        this.query = PathToQuery.forFunction(client, new VolumePathQuery());
        this.latestWatcher = latestWatcher;
    }

    @Override
    public void handleWatchEvent(final WatchEvent event) {
        logger.debug("{}", event);
        if (service.isRunning()) {
            query.apply(event.getPath()).call();
            latestWatcher.handleWatchEvent(
                    NodeWatchEvent.nodeCreated(
                            event.getPath().join(
                                    ControlSchema.Safari.Volumes.Volume.Log.LABEL)
                                    .join(ControlSchema.Safari.Volumes.Volume.Log.Latest.LABEL)));
        }
    }
    
    protected static final class VolumePathQuery implements Function<ZNodePath, List<? extends Records.Request>> {

        private final Operations.Requests.Sync sync;
        private final Operations.Requests.Exists exists;
        private final Operations.Requests.GetData getData;
        
        public VolumePathQuery() {
            this.sync = Operations.Requests.sync();
            this.exists = Operations.Requests.exists().setWatch(true);
            this.getData = Operations.Requests.getData();
        }
        
        @Override
        public List<? extends Records.Request> apply(final ZNodePath input) {
            final ZNodePath path = input.join(ControlSchema.Safari.Volumes.Volume.Path.LABEL);
            return ImmutableList.of(
                    sync.setPath(input).build(),
                    exists.setPath(input).build(),
                    sync.setPath(path).build(),
                    getData.setPath(path).build());
        }
    }
}