package edu.uw.zookeeper.safari.storage;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.client.Watchers.CacheNodeCreatedListener;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class CleanSnapshot extends CacheNodeCreatedListener<StorageZNode<?>> {

    public static CleanSnapshot listen(
            Service service, 
            StorageClientService storage) {
        CleanSnapshot instance = new CleanSnapshot(storage, service);
        instance.listen();
        return instance;
    }
    
    protected final StorageClientService storage;
    
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
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        cache.lock().readLock().lock();
        try {
            if (((Boolean) cache.cache().get(event.getPath()).data().get()).booleanValue()) {
                new SnapshotWatcher((AbsoluteZNodePath) ((AbsoluteZNodePath) event.getPath()).parent());
            }
        } finally {
            cache.lock().readLock().unlock();
        }
    }

    protected class SnapshotWatcher implements Runnable { 
        
        protected final AbsoluteZNodePath path;
        protected final ImmutableList<AbstractWatchListener> listeners;
        
        public SnapshotWatcher(AbsoluteZNodePath path) {
            this.path = path;
            ImmutableList.Builder<AbstractWatchListener> listeners = ImmutableList.builder();
            for (Class<? extends StorageZNode<?>> type: ImmutableList.of(
                    StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.class,
                    StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.class)) {
                listeners.add(new EmptySnapshot(type));
            }
            this.listeners = listeners.build();
            for (AbstractWatchListener listener: this.listeners) {
                listener.listen();
            }
        }
        
        @Override
        public void run() {
            ImmutableList<Records.Request> requests = ImmutableList.of();
            storage.materializer().cache().lock().readLock().lock();
            try {
                StorageZNode<?> snapshot = storage.materializer().cache().cache().get(path);
                if (snapshot != null) {
                    boolean isEmpty = true;
                    for (StorageZNode<?> node: snapshot.values()) {
                        if (!node.isEmpty()) {
                            isEmpty = false;
                            break;
                        }
                    }
                    if (isEmpty) {
                        Operations.Requests.Delete delete = Operations.Requests.delete();
                        ImmutableList.Builder<Records.Request> builder = ImmutableList.builder();
                        for (NameTrie.Node<?> node: storage.materializer().schema().get().get(path).values()) {
                            builder.add(delete.setPath(node.path()).build());
                        }
                        builder.add(delete.setPath(path).build());
                        requests = builder.build();
                    }
                }
            } finally {
                storage.materializer().cache().lock().readLock().unlock();
            }
            if (!requests.isEmpty()) {
                new DeleteCallback(SubmittedRequests.submit(storage.materializer(), requests));
            }
        }
        
        protected class EmptySnapshot extends AbstractWatchListener implements Runnable {
    
            public EmptySnapshot(Class<? extends StorageZNode<?>> type) {
                super(CleanSnapshot.this.service, 
                        storage.cacheEvents(), 
                        WatchMatcher.exact(
                                storage.materializer().schema().apply(type).path(), 
                                Watcher.Event.EventType.NodeChildrenChanged));
            }
            
            @Override
            public void run() {
                SnapshotWatcher.this.run();
            }
    
            @Override
            public void handleWatchEvent(WatchEvent event) {
                run();
            }

            @Override
            public void running() {
                super.running();
                final ZNodePath path = getWatchMatcher().getPath();
                @SuppressWarnings("unchecked")
                final FixedQuery<?> query = FixedQuery.forIterable(
                        storage.materializer(), 
                        PathToRequests.forRequests(
                                Operations.Requests.sync(), 
                                Operations.Requests.getChildren().setWatch(true))
                                .apply(path));
                Watchers.StopOnDeleteListener.listen(
                        Watchers.RunnableWatcher.listen(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        new Query(query);
                                    }
                                },
                                service, 
                                storage.notifications(), 
                                WatchMatcher.exact(
                                        path, 
                                        Watcher.Event.EventType.NodeChildrenChanged)),
                        path);
            }
            
            protected class Query extends edu.uw.zookeeper.client.Query {

                public Query(FixedQuery<?> query) {
                    super(query, EmptySnapshot.this);
                }
                
                @Override
                public Optional<Operation.Error> call() throws Exception {
                    Optional<Operation.Error> error = super.call();
                    EmptySnapshot.this.run();
                    return error;
                }
            }
        }
        
        protected class DeleteCallback extends ForwardingListenableFuture<List<Message.ServerResponse<?>>> implements Runnable {

            protected final SubmittedRequests<Records.Request, Message.ServerResponse<?>> future;
            
            public DeleteCallback(
                    SubmittedRequests<Records.Request, Message.ServerResponse<?>> future) {
                this.future = future;
                addListener(this, SameThreadExecutor.getInstance());
            }
            
            @Override
            protected ListenableFuture<List<Message.ServerResponse<?>>> delegate() {
                return future;
            }

            @Override
            public void run() {
                if (isDone()) {
                    try {
                        List<Message.ServerResponse<?>> responses = get();
                        for (Message.ServerResponse<?> response: responses) {
                            Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                        }
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    } catch (Exception e) {
                    }
                    for (AbstractWatchListener listener: listeners) {
                        listener.stopping(listener.state());
                    }
                }
            }
        }
    }
}
