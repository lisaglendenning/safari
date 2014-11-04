package edu.uw.zookeeper.safari.storage.snapshot;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.collect.Sets;
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
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Commits snapshot ephemerals and watches when their descendants
 * have committed.
 */
public final class CommitSessionSnapshots<T extends FutureCallback<? super StorageZNode.SessionsZNode>> extends Watchers.SimpleForwardingCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot, T> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public CommitSessionSnapshots<?> getCommitSessionSnapshots(
                final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> client,
                final RegionRoleService service) {
            return CommitSessionSnapshots.listen(
                    client.materializer(), 
                    client.cacheEvents(),
                    service,
                    service.logger());
        }

        @Override
        public Key<?> getKey() {
            return Key.get(CommitSessionSnapshots.class);
        }

        @Override
        protected void configure() {
            bind(CommitSessionSnapshots.class).to(new TypeLiteral<CommitSessionSnapshots<?>>(){});
        }
    }
    
    public static CommitSessionSnapshots<SnapshotSessionsCallback<CreateCommit<?>>> listen(
            Materializer<StorageZNode<?>,?> materializer,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        final SnapshotSessionsCallback<CreateCommit<?>> callback = SnapshotSessionsCallback.create(
                materializer, 
                cacheEvents, 
                service, 
                logger);
        final CommitSessionSnapshots<SnapshotSessionsCallback<CreateCommit<?>>> instance = new CommitSessionSnapshots<SnapshotSessionsCallback<CreateCommit<?>>>(
                callback);
        EphemeralsCommitListener.listen(
                callback, 
                materializer.cache(), 
                cacheEvents, 
                service, 
                logger);
        SnapshotCommittedCallback.listen(
                instance,
                materializer.cache(), 
                cacheEvents, 
                service, 
                logger);
        return instance;
    }
    
    protected CommitSessionSnapshots(
            T delegate) {
        super(delegate);
    }
    
    @Override
    public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot result) {
        delegate().onSuccess(result.ephemerals().sessions());
    }
    
    protected static final class SnapshotSessionsCallback<T extends FutureCallback<? super ZNodePath>> extends Watchers.SimpleForwardingCallback<StorageZNode.SessionsZNode, T> {

        public static SnapshotSessionsCallback<CreateCommit<?>> create(
                Materializer<StorageZNode<?>,?> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return new SnapshotSessionsCallback<CreateCommit<?>>(
                    materializer.cache(),
                    cacheEvents,
                    service,
                    logger,
                    CreateCommit.create(
                            materializer, 
                            materializer.codec(),
                            Watchers.StopServiceOnFailure.create(service)));
        }
        
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final WatchListeners cacheEvents;
        private final Service service;
        private final Logger logger;
        
        protected SnapshotSessionsCallback(
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                WatchListeners cacheEvents,
                Service service,
                Logger logger,
                T delegate) {
            super(delegate);
            this.cache = cache;
            this.cacheEvents = cacheEvents;
            this.service = service;
            this.logger = logger;
        }
        
        @Override
        public void onSuccess(StorageZNode.SessionsZNode result) {
            if ((result != null) && !(result.parent().get().containsKey(StorageZNode.CommitZNode.LABEL))) {
                Futures.addCallback(
                        SessionCommitListener.listen(
                            result, 
                            cache, 
                            cacheEvents, 
                            service, 
                            logger), 
                        delegate());
            }
        }
    }
    
    protected static final class EphemeralsCommitListener<T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions>> extends Watchers.SimpleForwardingCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Commit, T> {

        public static <T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions>> Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
                T callback,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.listen(
                    cache, 
                    service, 
                    cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                            create(callback), 
                                            cache.cache())), 
                            WatchMatcher.exact(
                                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Commit.PATH, 
                                    Watcher.Event.EventType.NodeCreated), 
                            logger), 
                    logger);
        }
        
        protected static <T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions>> EphemeralsCommitListener<T> create(
                T callback) {
            return new EphemeralsCommitListener<T>(callback);
        }
        
        protected EphemeralsCommitListener(
                T delegate) {
            super(delegate);
        }
        
        @Override
        public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Commit result) {
            if (result != null) {
                delegate().onSuccess(result.ephemerals().snapshot().watches().sessions());
            }
        }
    }
    
    protected static final class CreateCommit<O extends Operation.ProtocolResponse<?>> extends Watchers.SimpleForwardingCallback<Object,FutureCallback<?>> {
        
        public static <O extends Operation.ProtocolResponse<?>> CreateCommit<O> create(
                final ClientExecutor<? super Records.Request, O, ?> client,
                final Serializers.ByteSerializer<Object> serializer,
                final FutureCallback<?> callback) {
            try {
            return new CreateCommit<O>(
                    new AsyncFunction<ZNodePath, O>() {
                        final Operations.Requests.Create create = Operations.Requests.create().setData(serializer.toBytes(Boolean.TRUE));
                        @Override
                        public ListenableFuture<O> apply(ZNodePath input) {
                            return client.submit(create.setPath(input.join(StorageSchema.CommitZNode.LABEL)).build());
                        };
                    }, 
                    callback);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        
        private final AsyncFunction<ZNodePath,O> commit;

        protected CreateCommit(
                final AsyncFunction<ZNodePath,O> commit,
                final FutureCallback<?> delegate) {
            super(delegate);
            this.commit = commit;
        }
        
        @Override
        public void onSuccess(Object result) {
            if (result instanceof ZNodePath) {
                try {
                    Futures.addCallback(
                            commit.apply((ZNodePath) result), 
                            this);
                } catch (Exception e) {
                    onFailure(e);
                }
            } else {
                try {
                    Operations.maybeError(
                            ((Operation.ProtocolResponse<?>) result).record(), 
                            KeeperException.Code.NONODE, 
                            KeeperException.Code.NODEEXISTS);
                } catch (KeeperException e) {
                    onFailure(e);
                }
            }
        }
    }
    
    /**
     * Sets a promise when all sessions have committed.
     */
    protected static final class SessionCommitListener<V> extends ToStringListenableFuture<V> implements FutureCallback<ZNodePath>, Runnable {
        
        /**
         * Assumes cache is locked.
         * Assumes that we have seen all sessions.
         */
        public static <T extends StorageZNode<?>> SessionCommitListener<ZNodePath> listen(
                T sessions,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            final Promise<ZNodePath> promise = SettableFuturePromise.create();
            final Set<ZNodePath> uncommitted = Sets.newHashSetWithExpectedSize(sessions.size());
            for (StorageZNode<?> node: sessions.values()) {
                uncommitted.add(node.path());
            }
            final SessionCommitListener<ZNodePath> callback = create(
                    sessions.parent().get().path(),
                    uncommitted,
                    promise);
            final Watchers.CacheNodeCreatedListener<StorageZNode<?>> listener = Watchers.CacheNodeCreatedListener.create(
                    cache, 
                    service, 
                    cacheEvents,
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    callback), 
                            WatchMatcher.exact(
                                    sessions.path().join(ZNodeLabel.fromString(StorageZNode.SessionZNode.LABEL)).join(StorageZNode.CommitZNode.LABEL), 
                                    Watcher.Event.EventType.NodeCreated), 
                            logger),
                    logger);
            listener.starting();
            listener.running();
            callback.run();
            callback.addListener(new Runnable() {
                @Override
                public void run() {
                    listener.stopping(listener.state());
                    listener.terminated(Service.State.STOPPING);
                }
            }, MoreExecutors.directExecutor());
            return callback;
        }
        
        protected static <V> SessionCommitListener<V> create(
                V result,
                Set<ZNodePath> uncommitted,
                Promise<V> promise) {
            return new SessionCommitListener<V>(result, uncommitted, promise);
        }
        
        private final V result;
        private final Set<ZNodePath> uncommitted;
        private final Promise<V> promise;
        
        protected SessionCommitListener(
                V result,
                Set<ZNodePath> uncommitted,
                Promise<V> promise) {
            this.result = result;
            this.uncommitted = Collections.synchronizedSet(uncommitted);
            this.promise = promise;
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            uncommitted.remove(((AbsoluteZNodePath) result).parent());
            run();
        }

        @Override
        public void onFailure(Throwable t) {
            delegate().setException(t);
        }
        
        @Override
        public void run() {
            if (uncommitted.isEmpty()) {
                delegate().set(result);
            }
        }

        @Override
        protected Promise<V> delegate() {
            return promise;
        }
    }
}
