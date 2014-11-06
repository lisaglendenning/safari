package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.ParentOfPath;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;


public final class SnapshotListener extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        public static Module create() {
            return new Module();
        }
        
        protected Module() {}
        
        @Provides @Singleton
        public SnapshotListener newSnapshotListener(
                DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
                SchemaClientService<StorageZNode<?>,?> client,
                ServiceMonitor monitor) {
            SnapshotListener instance = SnapshotListener.create(versions, client);
            monitor.add(instance);
            return instance;
        }

        @Override
        public Key<? extends Service> getKey() {
            return Key.get(SnapshotListener.class);
        }

        @Override
        protected void configure() {
        }
    }
    
    @SuppressWarnings("unchecked")
    public static SnapshotListener create(
            DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
            SchemaClientService<StorageZNode<?>,?> client) {
        final SnapshotListener service = new SnapshotListener(
                ImmutableList.<Service.Listener>of());
        final Watchers.MaybeErrorProcessor processor = Watchers.MaybeErrorProcessor.maybeNoNode();
        final Watchers.StopServiceOnFailure<Optional<Operation.Error>,SnapshotListener> callback = Watchers.StopServiceOnFailure.create(service);
        Watchers.FutureCallbackServiceListener.listen(
                Watchers.EventToQueryCallback.create(
                        client.materializer(), 
                        processor, 
                        callback), 
                service, 
                client.notifications(), 
                WatchMatcher.prefix(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.PATH, 
                        EnumSet.allOf(Watcher.Event.EventType.class)), 
                service.logger());
        SnapshotCommittedQuery.listen(
                processor, 
                callback, 
                client.materializer(), 
                client.cacheEvents(), 
                service, 
                service.logger());
        snapshotCommitWatcher(
                processor,
                callback,
                client.materializer(),
                client.materializer().cache(),
                client.cacheEvents(), 
                service,
                service.logger());
        Watchers.CacheNodeCreatedListener.listen(
                client.materializer().cache(),
                service,
                client.cacheEvents(),
                Watchers.FutureCallbackListener.create(
                    Watchers.EventToPathCallback.create(
                            Watchers.PathToQueryCallback.create(
                                        PathToQuery.forFunction(
                                                client.materializer(), 
                                                Functions.compose(
                                                        PathToRequests.forRequests(
                                                                Operations.Requests.sync(),
                                                                Operations.Requests.exists().setWatch(true)),
                                                        JoinToPath.forName(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.LABEL))), 
                                        processor, 
                                        callback)),
                                        WatchMatcher.exact(
                                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.PATH, 
                                                Watcher.Event.EventType.NodeCreated),
                                        service.logger()),
                                service.logger());
        DirectoryEntryListener.entryCreatedCallback(
                Watchers.EventToPathCallback.create(
                        Watchers.PathToQueryCallback.create(
                            PathToQuery.forFunction(
                                    client.materializer(), 
                                    Functions.compose(
                                            PathToRequests.forRequests(
                                                    Operations.Requests.sync(),
                                                    Operations.Requests.exists().setWatch(true)),
                                            JoinToPath.forName(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.LABEL))), 
                            processor, 
                            callback)), 
                versions);
        return service;
    }
    
    @SuppressWarnings("unchecked")
    protected static <O extends Operation.ProtocolResponse<?>, V> Watchers.CacheNodeCreatedListener<StorageZNode<?>> snapshotCommitWatcher(
            Processor<? super List<O>, V> processor,
            FutureCallback<? super V> callback,
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        // Other listeners assume that we see all sessions and the prefix before we see the commit value.
        ImmutableList.Builder<Function<ZNodePath, ? extends List<? extends Records.Request>>> queries = ImmutableList.builder();
        final PathToRequests getChildren = PathToRequests.forRequests(
                Operations.Requests.sync(), 
                Operations.Requests.getChildren());
        for (AbsoluteZNodePath path: ImmutableList.of(
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.PATH,
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.PATH)) {
            queries.add(Functions.compose(
                    getChildren, 
                    Functions.compose(
                            JoinToPath.forName(path.suffix(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.PATH)), 
                            ParentOfPath.create())));
        }
        final PathToRequests getData = PathToRequests.forRequests(
                Operations.Requests.sync(), 
                Operations.Requests.getData());
        queries.add(
                Functions.compose(
                    getData, 
                    Functions.compose(
                            JoinToPath.forName(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Prefix.LABEL), 
                            ParentOfPath.create())));
        queries.add(getData);
        return Watchers.CacheNodeCreatedListener.listen(
                cache,
                service,
                cacheEvents,
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToQueryCallback.create(
                                    PathToQuery.forFunction(
                                            client, 
                                            QueryList.create(queries.build())),
                                    processor, 
                                    callback)), 
                        WatchMatcher.exact(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH, 
                                Watcher.Event.EventType.NodeCreated), 
                        logger),
                logger);
    }
    
    protected SnapshotListener(
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
    }
    
    @Override
    protected Executor executor() {
        return MoreExecutors.directExecutor();
    }

    protected static final class QueryList implements Function<ZNodePath, List<? extends Records.Request>> {

        public static QueryList create(
                ImmutableList<Function<ZNodePath, ? extends List<? extends Records.Request>>> queries) {
            return new QueryList(queries);
        }
        
        private final ImmutableList<Function<ZNodePath, ? extends List<? extends Records.Request>>> queries;
        
        protected QueryList(ImmutableList<Function<ZNodePath, ? extends List<? extends Records.Request>>> queries) {
            this.queries = queries;
        }
        
        @Override
        public List<? extends Records.Request> apply(final ZNodePath input) {
            final ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
            for (Function<ZNodePath, ? extends List<? extends Records.Request>> query: queries) {
                requests.addAll(query.apply(input));
            }
            return requests.build();
        }
    }
    
    protected static final class SnapshotCommittedQuery implements FutureCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot> {
        
        @SuppressWarnings("unchecked")
        public static <O extends Operation.ProtocolResponse<?>, V> Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback,
                Materializer<StorageZNode<?>,O> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            ImmutableList.Builder<Function<ZNodePath, ? extends List<? extends Records.Request>>> queries = ImmutableList.builder();
            Operations.Requests.Sync sync = Operations.Requests.sync();
            Operations.Requests.Exists exists = Operations.Requests.exists().setWatch(true);
            for (AbsoluteZNodePath path: ImmutableList.of(
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Commit.PATH,
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Commit.PATH)) {
                queries.add(Functions.compose(
                        PathToRequests.forRequests(
                                sync, 
                                exists), 
                        JoinToPath.forName(
                                path.suffix(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.PATH))));
            }
            return SnapshotCommittedCallback.listen(
                    new SnapshotCommittedQuery(
                            Watchers.PathToQueryCallback.create(
                                    PathToQuery.forFunction(
                                            materializer, 
                                            QueryList.create(queries.build())), 
                                    processor, 
                                    callback)),
                    materializer.cache(),
                    cacheEvents,
                    service,
                    logger);
        }

        private final FutureCallback<ZNodePath> callback;
        
        protected SnapshotCommittedQuery(
                FutureCallback<ZNodePath> callback) {
            this.callback = callback;
        }
        
        @Override
        public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot result) {
            callback.onSuccess(result.path());
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).toString();
        }
    }
}
