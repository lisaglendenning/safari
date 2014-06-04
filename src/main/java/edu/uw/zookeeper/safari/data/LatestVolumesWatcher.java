package edu.uw.zookeeper.safari.data;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;

/**
 * Watches and queries the path and latest state for all volumes;
 */
public final class LatestVolumesWatcher extends CacheNodeCreatedListener {

    public static LatestVolumesWatcher create(
            Service service,
            ControlClientService control) {
        newVolumeLatestListener(control.materializer(), control.materializer().cache(), service, control.cacheEvents());
        newVolumeDeletedWatcher(service, control.notifications(), control.materializer());
        LatestVolumesWatcher instance = new LatestVolumesWatcher(
                newVolumeLatestWatcher(service, control.notifications(), control.materializer()),
                control.materializer(),
                service,
                control.cacheEvents(), 
                control.materializer().cache());
        service.addListener(instance, SameThreadExecutor.getInstance());
        if (service.isRunning()) {
            instance.starting();
            instance.running();
        }
        newVolumeDirectoryWatcher(service, control.notifications(), control.materializer());
        return instance;
    }
    
    public static RunnableWatcher<?> newVolumeDirectoryWatcher(
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,?,?> client) {
        final WatchMatcher matcher = WatchMatcher.exact(
                ControlSchema.Safari.Volumes.PATH,
                Watcher.Event.EventType.NodeCreated,
                Watcher.Event.EventType.NodeChildrenChanged);
        final FixedQuery<?> query = FixedQuery.forRequests(client, 
                Operations.Requests.sync().setPath(matcher.getPath()).build(),
                Operations.Requests.getChildren().setPath(matcher.getPath()).setWatch(true).build());
        return RunnableWatcher.newInstance(service, watch, matcher, new Runnable() {
            @Override
            public void run() {
                query.call();
            }
        });
    }
    
    public static PathToQueryWatcher<?,?> newVolumeDeletedWatcher(
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
        return PathToQueryWatcher.newInstance(service, watch, matcher, query);
    }
    
    public static PathToQueryWatcher<?,?> newVolumeLatestWatcher(
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
        return PathToQueryWatcher.newInstance(service, watch, matcher, query);
    }
    
    public static <O extends Operation.ProtocolResponse<?>> VolumeLatestListener<O> newVolumeLatestListener(
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        return VolumeLatestListener.newInstance(client, cache, service, watch);
    }
    
    private final Logger logger;
    private final PathToQuery<VolumePathQuery,?> query;
    private final PathToQueryWatcher<?,?> latestWatcher;

    protected LatestVolumesWatcher(
            PathToQueryWatcher<?,?> latestWatcher,
            ClientExecutor<? super Records.Request,?,?> client,
            Service service,
            WatchListeners watch,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache) {
        super(ControlSchema.Safari.Volumes.Volume.PATH, service, watch, cache);
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