package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.ParentOfPath;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
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
        SnapshotListener service = new SnapshotListener(
                ImmutableList.<Service.Listener>of());
        Watchers.MaybeErrorProcessor processor = Watchers.MaybeErrorProcessor.maybeNoNode();
        Watchers.StopServiceOnFailure<Optional<Operation.Error>> callback = Watchers.StopServiceOnFailure.create(service);
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
        for (Pair<? extends PathToQuery<?,?>, WatchMatcher> query: ImmutableList.of(
                Pair.create(
                        PathToQuery.forFunction(client.materializer(), 
                                SnapshotCommitQuery.create()), 
                        WatchMatcher.exact(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH, 
                                Watcher.Event.EventType.NodeCreated)),
                Pair.create(
                        PathToQuery.forRequests(
                                client.materializer(), 
                                Operations.Requests.sync(),
                                Operations.Requests.getChildren().setWatch(true)),
                        WatchMatcher.exact(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.PATH, 
                                Watcher.Event.EventType.NodeCreated)))) {
            Watchers.CacheNodeCreatedListener.listen(
                    client.materializer().cache(),
                    service,
                    client.cacheEvents(),
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToQueryCallback.create(
                                        query.first(), 
                                        processor, 
                                        callback)), 
                            query.second(), 
                            service.logger()),
                    service.logger());
        }
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
    
    protected SnapshotListener(
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
    }
    
    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }
    
    protected static final class SnapshotCommitQuery implements Function<ZNodePath, List<? extends Records.Request>> {

        @SuppressWarnings("unchecked")
        public static SnapshotCommitQuery create() {
            final ImmutableList.Builder<Function<ZNodePath, ? extends List<? extends Records.Request>>> queries = ImmutableList.builder();
            for (ZNodeLabel label: ImmutableList.of(
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL,
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.LABEL)) {
                queries.add(Functions.compose(
                        PathToRequests.forRequests(
                                Operations.Requests.sync(), 
                                Operations.Requests.getChildren().setWatch(true)), 
                        Functions.compose(JoinToPath.forName(label), ParentOfPath.create())));
            }
            queries.add(PathToRequests.forRequests(
                    Operations.Requests.sync(), 
                    Operations.Requests.getData()));
            return new SnapshotCommitQuery(queries.build());
        }
        
        private final ImmutableList<Function<ZNodePath, ? extends List<? extends Records.Request>>> queries;
        
        protected SnapshotCommitQuery(ImmutableList<Function<ZNodePath, ? extends List<? extends Records.Request>>> queries) {
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
}
