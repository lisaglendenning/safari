package edu.uw.zookeeper.safari.storage.snapshot;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.client.Watchers.FutureCallbackListener;

import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.Error;
import edu.uw.zookeeper.protocol.proto.IDeleteRequest;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Assumes that snapshot entries are watched.
 */
public final class CleanSnapshot<O extends Operation.ProtocolResponse<?>> implements FutureCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot> {

    public static <O extends Operation.ProtocolResponse<?>> Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        final DeleteCallback<O> delete = DeleteCallback.create(materializer,
                Watchers.StopServiceOnFailure.create(service));
        final CleanSnapshot<O> instance = new CleanSnapshot<O>(delete);
        final ImmutableMap<Class<? extends StorageZNode<?>>, FutureCallback<? extends StorageZNode<?>>> callbacks = 
                ImmutableMap.<Class<? extends StorageZNode<?>>, FutureCallback<? extends StorageZNode<?>>>builder()
                    .putAll(SnapshotChildrenListener.listen(delete, materializer, cacheEvents, service, logger)).build();
        SnapshotCommittedListener.listen(
                new FutureCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot>(){
                    @Override
                    public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot result) {
                        instance.onSuccess(result);
                        for (StorageZNode<?> child: result.values()) {
                            @SuppressWarnings("unchecked")
                            FutureCallback<StorageZNode<?>> callback = (FutureCallback<StorageZNode<?>>) callbacks.get(child.getClass());
                            if (callback != null) {
                                callback.onSuccess(child);
                            }
                        }
                    }
                    @Override
                    public void onFailure(Throwable t) {
                        instance.onFailure(t);
                    }
                }, 
                materializer.cache(), 
                cacheEvents, 
                service, 
                logger);
        return Watchers.CacheNodeCreatedListener.listen(
                materializer.cache(), 
                service, 
                cacheEvents, 
                FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        instance, 
                                        materializer.cache().cache())),
                        WatchMatcher.exact(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.PATH,
                                Watcher.Event.EventType.NodeChildrenChanged),
                        logger), 
                logger);
    }

    private final Operations.Requests.Delete delete;
    private final FutureCallback<? super IMultiRequest> callback;
    
    protected CleanSnapshot(
            FutureCallback<? super IMultiRequest> callback) {
        this.callback = callback;
        this.delete = Operations.Requests.delete();
    }


    @Override
    public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot result) {
        if ((result != null) && (result.size() == 1) && (result.commit() != null) && (result.commit().data().get() != null) && result.commit().data().get().booleanValue()) {
            callback.onSuccess(
                    new IMultiRequest(
                            ImmutableList.<Records.MultiOpRequest>of(
                                    delete.setPath(result.path().join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.LABEL)).build(), 
                                    delete.setPath(result.path()).build())));
        }
    }

    @Override
    public void onFailure(Throwable t) {
        callback.onFailure(t);
    }
    
    protected static final class DeleteCallback<O extends Operation.ProtocolResponse<?>> implements FutureCallback<Records.Request>, AsyncFunction<O, Optional<Operation.Error>> {
        
        public static <O extends Operation.ProtocolResponse<?>> DeleteCallback<O> create(
                ClientExecutor<? super Records.Request, O, ?> client,
                FutureCallback<? super Optional<Operation.Error>> callback) {
            return new DeleteCallback<O>(client, callback);
        }
        
        private final ClientExecutor<? super Records.Request, O, ?> client;
        private final FutureCallback<? super Optional<Operation.Error>> callback;

        protected DeleteCallback(
                ClientExecutor<? super Records.Request, O, ?> client,
                FutureCallback<? super Optional<Operation.Error>> callback) {
            this.client = client;
            this.callback = callback;
        }

        @Override
        public ListenableFuture<Optional<Error>> apply(O input)
                throws Exception {
            Optional<Operation.Error> error;
            if (input.record() instanceof IMultiResponse) {
                error = Operations.maybeMultiError((IMultiResponse) input.record(), KeeperException.Code.NONODE, KeeperException.Code.NOTEMPTY);
            } else {
                error = Operations.maybeError(input.record(), KeeperException.Code.NONODE, KeeperException.Code.NOTEMPTY);
            }
            return Futures.immediateFuture(error);
        }

        @Override
        public void onSuccess(Records.Request result) {
            Futures.addCallback(
                    Futures.transform(
                            client.submit(result), 
                            this),
                    callback);
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
    
    /**
     * Deletes snapshot children when they are empty.
     */
    protected static final class SnapshotChildrenListener<T extends StorageZNode<?>> implements FutureCallback<T> {

        public static ImmutableMap<Class<? extends StorageZNode<?>>, SnapshotChildrenListener<?>> listen(
                FutureCallback<? super IDeleteRequest> callback,
                Materializer<StorageZNode<?>,?> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            ImmutableMap.Builder<Class<? extends StorageZNode<?>>, SnapshotChildrenListener<?>> listeners = ImmutableMap.builder();
            for (Class<? extends StorageZNode<?>> type: ImmutableList.of(
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.class,
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.class)) {
                listeners.put(type, listen(type, callback, materializer, cacheEvents, service, logger));
            }
            return listeners.build();
        }
        
        public static <T extends StorageZNode<?>> SnapshotChildrenListener<T> listen(
                Class<T> type,
                FutureCallback<? super IDeleteRequest> callback,
                Materializer<StorageZNode<?>,?> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            final SnapshotChildrenListener<T> instance = new SnapshotChildrenListener<T>(callback);
            Watchers.CacheNodeCreatedListener.listen(
                    materializer.cache(), 
                    service, 
                    cacheEvents,
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                            instance, 
                                            materializer.cache().cache())), 
                            WatchMatcher.exact(
                                    materializer.schema().apply(type).path(), 
                                    Watcher.Event.EventType.NodeCreated, 
                                    Watcher.Event.EventType.NodeChildrenChanged), 
                            logger),
                    logger);
            return instance;
        }
        
        private final Operations.Requests.Delete delete;
        private final FutureCallback<? super IDeleteRequest> callback;
        
        public SnapshotChildrenListener(
                FutureCallback<? super IDeleteRequest> callback) {
            this.callback = callback;
            this.delete = Operations.Requests.delete();
        }
        
        @Override
        public void onSuccess(T result) {
            if (result.isEmpty()) {
                final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot snapshot = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot) result.parent().get();
                if ((snapshot.commit() != null) && (snapshot.commit().data().get() != null) && snapshot.commit().data().get().booleanValue()) {
                    callback.onSuccess(delete.setPath(result.path()).build());
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
    
    /**
     * Callback on snapshot commit.
     */
    protected static final class SnapshotCommittedListener implements FutureCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit> {

        public static Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
                FutureCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot> callback,
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
                                            new SnapshotCommittedListener(callback), 
                                            cache.cache())), 
                            WatchMatcher.exact(
                                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH, 
                                    Watcher.Event.EventType.NodeDataChanged), 
                            logger),
                    logger);
        }
        
        private final FutureCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot> callback;
        
        protected SnapshotCommittedListener(
                FutureCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot> callback) {
            this.callback = callback;
        }
        
        @Override
        public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit result) {
            if (result.data().get().booleanValue()) {
                callback.onSuccess(result.snapshot());
            }
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
}
