package edu.uw.zookeeper.safari.storage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.client.Watchers.CacheNodeCreatedListener;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class CleanSnapshot extends CacheNodeCreatedListener<StorageZNode<?>> {

    public static CleanSnapshot listen(
            Service service, 
            StorageClientService storage) {
        CleanSnapshot instance = new CleanSnapshot(storage, service);
        instance.listen();
        return instance;
    }
    
    private final Map<AbsoluteZNodePath, ListenableFuture<?>> snapshots;
    private final StorageClientService storage;
    
    protected CleanSnapshot(
            StorageClientService storage,
            Service service) {
        super(storage.materializer().cache(), 
                service, 
                storage.cacheEvents(),
                WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Snapshot.Commit.PATH,
                        Watcher.Event.EventType.NodeDataChanged));
        this.storage = storage;
        this.snapshots = Maps.newHashMap();
    }

    @Override
    public synchronized void handleWatchEvent(final WatchEvent event) {
        final AbsoluteZNodePath path = (AbsoluteZNodePath) ((AbsoluteZNodePath) event.getPath()).parent();
        if (!snapshots.containsKey(path)) {
            Optional<SnapshotWatcher> watcher;
            cache.lock().readLock().lock();
            try {
                if (((Boolean) cache.cache().get(event.getPath()).data().get()).booleanValue()) {
                    watcher = Optional.of(new SnapshotWatcher(path));
                } else {
                    watcher = Optional.absent();
                }
            } finally {
                cache.lock().readLock().unlock();
            }
            if (watcher.isPresent()) {
                final Promise<AbsoluteZNodePath> promise = SettableFuturePromise.create();
                snapshots.put(path, promise);
                ChainedFutures.run(
                        ChainedFutures.process(
                                ChainedFutures.chain(
                                        watcher.get(), 
                                        Lists.<ListenableFuture<?>>newLinkedList()),
                                new Processor<List<ListenableFuture<?>>, AbsoluteZNodePath>() {
                                    @Override
                                    public AbsoluteZNodePath apply(
                                            List<ListenableFuture<?>> input)
                                            throws Exception {
                                        input.get(input.size()-1).get();
                                        return path;
                                    }
                                }),
                        promise)
                .addListener(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (CleanSnapshot.this) {
                            if (snapshots.get(path) == promise) {
                                snapshots.remove(path);
                            }
                        }
                    }
                }, SameThreadExecutor.getInstance());
            }
        }
    }
    
    @Override
    public synchronized void stopping(Service.State from) {
        super.stopping(from);
        for (Map.Entry<AbsoluteZNodePath, ListenableFuture<?>> entry: Iterables.consumingIterable(snapshots.entrySet())) {
            entry.getValue().cancel(false);
        }
    }

    protected final class SnapshotWatcher implements Function<List<ListenableFuture<?>>, Optional<? extends ListenableFuture<?>>> { 
        
        private final AbsoluteZNodePath path;
        private final Operations.Requests.Delete delete;
        
        public SnapshotWatcher(AbsoluteZNodePath path) {
            this.path = path;
            this.delete = Operations.Requests.delete();
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Optional<? extends ListenableFuture<?>> apply(List<ListenableFuture<?>> input) {
            Optional<? extends ListenableFuture<?>> last = input.isEmpty() ? Optional.<ListenableFuture<?>>absent() : Optional.of(input.get(input.size()-1));
            if (!last.isPresent()) {
                return Optional.of(
                        new EmptySnapshot(
                                path.join(StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.LABEL),
                                SettableFuturePromise.<AbsoluteZNodePath>create()));
            } else if (last.get() instanceof EmptySnapshot) {
                AbsoluteZNodePath path;
                try {
                    path = ((EmptySnapshot) last.get()).get();
                } catch (ExecutionException e) {
                    return Optional.absent();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
                if (path.label().equals(StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.LABEL)) {
                    return Optional.of(
                            SubmittedRequest.submit(
                                    storage.materializer(), 
                                    delete.setPath(path).build()));
                } else {
                    assert (path.label().equals(StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.LABEL));
                    ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
                    for (ZNodePath p: ImmutableList.of(
                            path, 
                            this.path.join(StorageSchema.Safari.Volumes.Volume.Snapshot.Commit.LABEL),
                            this.path)) {
                        requests.add(delete.setPath(p).build());
                    }
                    return Optional.of(SubmittedRequests.submit(storage.materializer(), requests.build()));
                }
            } else if (last.get() instanceof SubmittedRequest) {
                Records.Response response;
                try {
                    response = ((SubmittedRequest<?,?>) last.get()).get().record();
                } catch (ExecutionException e) {
                    return Optional.absent();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
                try {
                    Operations.maybeError(response, KeeperException.Code.NONODE);
                } catch (KeeperException e) {
                    return Optional.of(Futures.immediateFailedFuture(e));
                }
                return Optional.of(
                        new EmptySnapshot(
                                path.join(StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.LABEL),
                                SettableFuturePromise.<AbsoluteZNodePath>create()));
            } else {
                List<? extends Operation.ProtocolResponse<?>> responses;
                try {
                    responses = (List<? extends Operation.ProtocolResponse<?>>) last.get().get();
                } catch (ExecutionException e) {
                    return Optional.absent();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
                try {
                    for (Operation.ProtocolResponse<?> response: responses) {
                        Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                    }
                } catch (KeeperException e) {
                    return Optional.of(Futures.immediateFailedFuture(e));
                }
            }
            return Optional.absent();
        }
        
        protected final class EmptySnapshot extends PromiseTask<AbsoluteZNodePath, AbsoluteZNodePath> implements Runnable {
    
            private final FixedQuery<?> query;
            private final AbstractWatchListener listener;
            
            @SuppressWarnings("unchecked")
            public EmptySnapshot(
                    AbsoluteZNodePath path,
                    Promise<AbsoluteZNodePath> promise) {
                super(path, promise);
                this.query = FixedQuery.forIterable(
                        storage.materializer(), 
                        PathToRequests.forRequests(
                                Operations.Requests.sync(), 
                                Operations.Requests.getChildren().setWatch(true))
                                .apply(path));
                AbstractWatchListener listener = 
                        Watchers.RunnableWatcher.create(
                                this,
                                CleanSnapshot.this.service, 
                                storage.notifications(), 
                                WatchMatcher.exact(
                                        path, 
                                        Watcher.Event.EventType.NodeChildrenChanged));
                this.listener = Watchers.StopOnDeleteListener.create(
                        listener,
                        path);
                listener.listen();
                this.listener.listen();
                addListener(this, SameThreadExecutor.getInstance());
            }
            
            public boolean isEmpty() {
                cache.lock().readLock().lock();
                try {
                    StorageZNode<?> node = cache.cache().get(task());
                    return ((node == null) || node.isEmpty());
                } finally {
                    cache.lock().readLock().unlock();
                }
            }
            
            @Override
            public void run() {
                if (isDone()) {
                    if (isCancelled()) {
                        listener.stopping(listener.state());
                    }
                } else {
                    new Query();
                }
            }
            
            protected final class Query extends Watchers.Query {

                public Query() {
                    super(query, EmptySnapshot.this.listener);
                }
                
                @Override
                public Optional<Operation.Error> call() throws Exception {
                    Optional<Operation.Error> error = super.call();
                    if (isEmpty()) {
                        set(task());
                    }
                    return error;
                }
            }
        }
    }
}
