package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.client.ChildToPath;
import edu.uw.zookeeper.client.Watchers.FutureCallbackListener;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Garbage collects snapshot once all ephemerals and watches have been committed.
 * 
 * Assumes that snapshot entries are watched.
 */
public final class GarbageCollectSnapshot extends Watchers.CacheNodeCreatedListener<StorageZNode<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public GarbageCollectSnapshot getGarbageCollectSnapshot(
                final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> client,
                final RegionRoleService service) {
            return GarbageCollectSnapshot.listen(
                    client.materializer(), 
                    client.cacheEvents(),
                    service,
                    service.logger());
        }

        @Override
        public Key<?> getKey() {
            return Key.get(GarbageCollectSnapshot.class);
        }

        @Override
        protected void configure() {
        }
    }
    
    public static <O extends Operation.ProtocolResponse<?>> GarbageCollectSnapshot listen(
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        final ConcurrentMap<AbsoluteZNodePath, Promise<?>> tasks = new MapMaker().makeMap();
        final SnapshotCommittedCallback<SnapshotCallback<O>> callback = SnapshotCommittedCallback.create(
                SnapshotCallback.create(
                        tasks, 
                        Watchers.StopServiceOnFailure.create(service), 
                        materializer, 
                        logger));
        final GarbageCollectSnapshot instance = new GarbageCollectSnapshot(
                tasks,
                materializer.cache(), 
                service, 
                cacheEvents,
                FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        callback, 
                                        materializer.cache().cache())),
                        WatchMatcher.exact(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH,
                                Watcher.Event.EventType.NodeDataChanged),
                        logger), 
                logger);
        instance.listen();
        SessionsSnapshotCommitted.listen(
                callback, 
                materializer, 
                cacheEvents, 
                service, 
                logger);
        return instance;
    }
    
    private final ConcurrentMap<AbsoluteZNodePath, Promise<?>> tasks;

    protected GarbageCollectSnapshot(
            ConcurrentMap<AbsoluteZNodePath, Promise<?>> tasks,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            Service service,
            WatchListeners cacheEvents,
            WatchMatchListener watcher,
            Logger logger) {
        super(cache, service, cacheEvents, watcher, logger);
        this.tasks = tasks;
    }
    
    @Override
    public void stopping(Service.State from) {
        super.stopping(from);
        for (Promise<?> future: Iterables.consumingIterable(tasks.values())) {
            future.cancel(false);
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        for (Promise<?> future: Iterables.consumingIterable(tasks.values())) {
            future.setException(failure);
        }
    }
    
    protected static final class SessionsSnapshotCommitted<V extends StorageZNode.CommitZNode, T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit>> extends Watchers.SimpleForwardingCallback<V, T> {

        public static <T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit>> List<Watchers.CacheNodeCreatedListener<StorageZNode<?>>> listen(
                T callback,
                Materializer<StorageZNode<?>,?> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            ImmutableList.Builder<Watchers.CacheNodeCreatedListener<StorageZNode<?>>> listeners = ImmutableList.builder();
            for (Class<? extends StorageZNode.CommitZNode> type: ImmutableList.of(
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Commit.class,
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Commit.class)) {
                listeners.add(listen(type, callback, materializer, cacheEvents, service, logger));
            }
            return listeners.build();
        }
        
        protected static <V extends StorageZNode.CommitZNode, T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit>> Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
                Class<V> type,
                T callback,
                Materializer<StorageZNode<?>,?> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.listen(
                    materializer.cache(), 
                    service, 
                    cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                            create(callback), 
                                            materializer.cache().cache())), 
                            WatchMatcher.exact(
                                    materializer.schema().apply(type).path(), 
                                    Watcher.Event.EventType.NodeCreated), 
                            logger), 
                    logger);
        }
        
        protected static <V extends StorageZNode.CommitZNode, T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit>> SessionsSnapshotCommitted<V,T> create(T callback) {
            return new SessionsSnapshotCommitted<V,T>(callback);
        }
        
        protected SessionsSnapshotCommitted(T delegate) {
            super(delegate);
        }

        @Override
        public void onSuccess(V result) {
            if (result != null) {
                final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot snapshot = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot) result.parent().get().parent().get();
                delegate().onSuccess(snapshot.commit());
            }
        }
    }
    
    protected static final class SnapshotCallback<O extends Operation.ProtocolResponse<?>> extends Watchers.SimpleForwardingCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot,FutureCallback<?>> {
        
        public static <O extends Operation.ProtocolResponse<?>> SnapshotCallback<O> create(
                ConcurrentMap<AbsoluteZNodePath, Promise<?>> tasks,
                FutureCallback<?> callback,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger) {
            return new SnapshotCallback<O>(tasks, materializer, logger, callback);
        }
        
        private final Logger logger;
        private final Materializer<StorageZNode<?>,O> materializer;
        private final ConcurrentMap<AbsoluteZNodePath, Promise<?>> tasks;
        
        protected SnapshotCallback(
                ConcurrentMap<AbsoluteZNodePath, Promise<?>> tasks,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger,
                FutureCallback<?> delegate) {
            super(delegate);
            this.logger = logger;
            this.materializer = materializer;
            this.tasks = tasks;
        }
    
        @Override
        public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot result) {
            assert (result != null);
            assert (result.commit() != null);
            assert (result.commit().data().get() != null);
            assert (result.commit().data().get().booleanValue());
            if (((result.ephemerals() != null) && (result.ephemerals().commit() != null))
                    && ((result.watches() != null) && (result.watches().commit() != null))) {
                new Callback((AbsoluteZNodePath) result.path()).run();
            }
        }
        
        protected final class Callback extends ForwardingPromise<AbsoluteZNodePath> implements Runnable {
    
            private final DeleteCommittedSnapshot<?> delegate;
    
            protected Callback(AbsoluteZNodePath path) {
                this(DeleteCommittedSnapshot.create(path, materializer, logger));
            }
            
            protected Callback(DeleteCommittedSnapshot<?> delegate) {
                this.delegate = delegate;
            }
            
            @Override
            public void run() {
                if (isDone()) {
                    tasks.remove(delegate.path(), this);
                } else {
                    if (tasks.putIfAbsent(delegate.path(), this) == null) {
                        delegate.run();
                        addListener(this, MoreExecutors.directExecutor());
                    } else {
                        cancel(false);
                    }
                }
            }
    
            @Override
            protected Promise<AbsoluteZNodePath> delegate() {
                return delegate;
            }
        }
    }
    
    protected static final class DeleteCallback<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<O,O> {
    
        protected DeleteCallback() {}
        
        @Override
        public ListenableFuture<O> apply(O input) throws Exception {
            Records.Response response = input.record();
            if (response instanceof IMultiResponse) {
                Operations.maybeMultiError((IMultiResponse) response, KeeperException.Code.NONODE);
            } else {
                Operations.maybeError(response, KeeperException.Code.NONODE);
            }
            return Futures.immediateFuture(input);
        }
    }

    protected static abstract class GarbageCollectTask<O extends Operation.ProtocolResponse<?>> extends ForwardingPromise<AbsoluteZNodePath> implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureListChain<ListenableFuture<?>>>, Runnable {

        protected final Materializer<StorageZNode<?>,O> materializer;
        protected final AbsoluteZNodePath path;
        protected final Logger logger;
        protected final ChainedFutures.ChainedFuturesTask<AbsoluteZNodePath> task;
        protected final DeleteCallback<O> callback;

        protected GarbageCollectTask(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger,
                List<ListenableFuture<?>> chain) {
            this.path = path;
            this.materializer = materializer;
            this.logger = logger;
            this.callback = new DeleteCallback<O>();
            this.task = ChainedFutures.task(
                    ChainedFutures.<AbsoluteZNodePath>castLast(
                        ChainedFutures.apply(
                                this, 
                                ChainedFutures.list(chain), 
                                logger)));
        }
        
        public AbsoluteZNodePath path() {
            return path;
        }
        
        @Override
        public void run() {
            task.run();
        }
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
            input.getLast().get();
            return Optional.of(Futures.immediateFuture(path));
        }
        
        @Override
        protected Promise<AbsoluteZNodePath> delegate() {
            return task;
        }
        
        @Override
        protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
            return helper.addValue(ToStringListenableFuture.toString(delegate()));
        }
    }
    
    protected static final class DeleteCommittedSnapshot<O extends Operation.ProtocolResponse<?>> extends GarbageCollectTask<O> {

        public static <O extends Operation.ProtocolResponse<?>> DeleteCommittedSnapshot<O> create(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger) {
            return new DeleteCommittedSnapshot<O>(path, materializer, logger);
        }
        
        protected DeleteCommittedSnapshot(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger) {
            super(path, materializer, logger, Lists.<ListenableFuture<?>>newArrayListWithCapacity(3));
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                List<DeleteCommittedSessionsSnapshot<O>> futures = Lists.newArrayListWithCapacity(2);
                for (ZNodeLabel label: ImmutableList.of(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL,
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.LABEL)) {
                    futures.add(DeleteCommittedSessionsSnapshot.create(path.join(label).join(StorageZNode.SessionsZNode.LABEL), materializer, logger));
                }
                for (DeleteCommittedSessionsSnapshot<?> future: futures) {
                    future.run();
                }
                return Optional.of(Futures.allAsList(futures));
            }
            case 1:
            {
                List<AbsoluteZNodePath> paths = Lists.newArrayList();
                for (AbsoluteZNodePath path: (List<? extends AbsoluteZNodePath>) input.getLast().get()) {
                    path = (AbsoluteZNodePath) path.parent();
                    paths.add((AbsoluteZNodePath) path.join(StorageZNode.CommitZNode.LABEL));
                    paths.add(path);
                }
                paths.add(this.path.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Prefix.LABEL));
                paths.add(this.path.join(StorageZNode.CommitZNode.LABEL));
                paths.add(this.path);
                Operations.Requests.Delete delete = Operations.Requests.delete();
                List<Records.MultiOpRequest> deletes = Lists.newArrayListWithCapacity(paths.size());
                for (AbsoluteZNodePath path: paths) {
                    deletes.add(delete.setPath(path).build());
                }
                ListenableFuture<O> future = Futures.transform(
                        materializer.submit(
                            new IMultiRequest(deletes)),
                        callback);
                return Optional.of(future);
            }
            case 2: 
            {
                return super.apply(input);
            }
            default:
                break;
            }
            return Optional.absent();
        }
    }
    
    /**
     * Assumes that we have seen all sessions.
     */
    protected static final class DeleteCommittedSessionsSnapshot<O extends Operation.ProtocolResponse<?>> extends GarbageCollectTask<O> {

        public static <O extends Operation.ProtocolResponse<?>> DeleteCommittedSessionsSnapshot<O> create(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger) {
            return new DeleteCommittedSessionsSnapshot<O>(path, materializer, logger);
        }
        
        protected DeleteCommittedSessionsSnapshot(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger) {
            super(path, materializer, logger, Lists.<ListenableFuture<?>>newArrayListWithCapacity(3));
        }
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                List<DeleteCommittedSessionSnapshot<O>> futures;
                materializer.cache().lock().readLock().lock();
                try {
                    StorageZNode<?> sessions = materializer.cache().cache().get(path);
                    futures = Lists.newArrayListWithCapacity(sessions.size());
                    for (StorageZNode<?> session: sessions.values()) {
                        futures.add(
                                DeleteCommittedSessionSnapshot.create(
                                    (AbsoluteZNodePath) session.path(),
                                    materializer,
                                    logger));
                    }
                } finally {
                    materializer.cache().lock().readLock().unlock();
                }
                for (DeleteCommittedSessionSnapshot<?> future: futures) {
                    future.run();
                }
                return Optional.of(Futures.allAsList(futures));
            }
            case 1:
            {
                input.getLast().get();
                ListenableFuture<O> future = Futures.transform(
                        materializer.submit(
                                Operations.Requests.delete().setPath(path).build()), 
                        callback);
                return Optional.of(future);
            }
            case 2:
            {
                return super.apply(input);
            }
            default:
                break;
            }
            return Optional.absent();
        }
    }
    
    protected static final class DeleteCommittedSessionSnapshot<O extends Operation.ProtocolResponse<?>> extends GarbageCollectTask<O> {

        public static <O extends Operation.ProtocolResponse<?>> DeleteCommittedSessionSnapshot<O> create(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger) {
            return new DeleteCommittedSessionSnapshot<O>(path, materializer, logger);
        }
        
        protected DeleteCommittedSessionSnapshot(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger) {
            super(path, materializer, logger, Lists.<ListenableFuture<?>>newArrayListWithCapacity(4));
        }
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                    ZNodePath path = this.path.join(StorageZNode.ValuesZNode.LABEL);
                    return Optional.of(
                            SubmittedRequests.submitRequests(
                                materializer,
                                Operations.Requests.sync().setPath(path).build(),
                                Operations.Requests.getChildren().setPath(path).build()));
            }
            case 1:
            {
                ZNodePath path = ZNodePath.fromString(((Records.PathGetter) ((SubmittedRequests<?,?>) input.getLast()).getValue().get(1)).getPath());
                @SuppressWarnings("unchecked")
                O response = (O) ((List<?>) input.getLast().get()).get(1);
                ListenableFuture<? extends List<AbsoluteZNodePath>> future;
                if (!Operations.maybeError(response.record(), KeeperException.Code.NONODE).isPresent()) {
                    List<String> children = ((Records.ChildrenGetter) response.record()).getChildren();
                    List<DeleteCommittedSessionValue<?>> deletes = Lists.newArrayListWithCapacity(children.size());
                    Iterator<AbsoluteZNodePath> itr = Iterators.transform(children.iterator(), ChildToPath.forParent(path));
                    while (itr.hasNext()) {
                        DeleteCommittedSessionValue<?> delete = DeleteCommittedSessionValue.create(itr.next(), materializer, logger);
                        delete.run();
                        deletes.add(delete);
                    }
                    future = Futures.allAsList(deletes);
                } else {
                    future = Futures.immediateFuture(ImmutableList.<AbsoluteZNodePath>of());
                }
                return Optional.of(future);
            }
            case 2:
            {
                input.getLast().get();
                Operations.Requests.Delete delete = Operations.Requests.delete();
                ListenableFuture<O> future = Futures.transform(
                        materializer.submit(
                            new IMultiRequest(
                                ImmutableList.<Records.MultiOpRequest>of(
                                        delete.setPath(path.join(StorageZNode.ValuesZNode.LABEL)).build(),
                                        delete.setPath(path.join(StorageZNode.CommitZNode.LABEL)).build(), 
                                        delete.setPath(path).build()))),
                        callback);
                return Optional.of(future);
            }
            case 3:
            {
                return super.apply(input);
            }
            default:
                break;
            }
            return Optional.absent();
        }
    }
    
    protected static final class DeleteCommittedSessionValue<O extends Operation.ProtocolResponse<?>> extends GarbageCollectTask<O> {

        public static <O extends Operation.ProtocolResponse<?>> DeleteCommittedSessionValue<O> create(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger) {
            return new DeleteCommittedSessionValue<O>(path, materializer, logger);
        }
        
        protected DeleteCommittedSessionValue(
                AbsoluteZNodePath path,
                Materializer<StorageZNode<?>, O> materializer, 
                Logger logger) {
            super(path, materializer, logger, 
                    Lists.<ListenableFuture<?>>newArrayListWithCapacity(3));
        }

        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                return Optional.of(
                        SubmittedRequests.submitRequests(
                            materializer,
                            Operations.Requests.sync().setPath(path).build(),
                            Operations.Requests.getChildren().setPath(path).build()));
            }
            case 1:
            {
                ZNodePath path = ZNodePath.fromString(((Records.PathGetter) ((SubmittedRequests<?,?>) input.getLast()).getValue().get(1)).getPath());
                @SuppressWarnings("unchecked")
                O response = (O) ((List<?>) input.getLast().get()).get(1);
                ListenableFuture<O> future;
                if (!Operations.maybeError(response.record(), KeeperException.Code.NONODE).isPresent()) {
                    List<String> children = ((Records.ChildrenGetter) response.record()).getChildren();
                    List<Records.MultiOpRequest> deletes = Lists.newArrayListWithCapacity(children.size()+1);
                    Operations.Requests.Delete delete = Operations.Requests.delete();
                    Iterator<AbsoluteZNodePath> itr = Iterators.transform(children.iterator(), ChildToPath.forParent(path));
                    while (itr.hasNext()) {
                        AbsoluteZNodePath child = itr.next();
                        deletes.add(delete.setPath(child).build());
                    }
                    deletes.add(delete.setPath(path).build());
                    future = Futures.transform(materializer.submit(new IMultiRequest(deletes)), callback);
                } else {
                    future = Futures.immediateFuture(response);
                }
                return Optional.of(future);
            }
            case 2:
            {
                return super.apply(input);
            }
            default:
                break;
            }
            return Optional.absent();
        }
    }
}
