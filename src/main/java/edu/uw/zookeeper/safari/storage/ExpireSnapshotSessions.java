package edu.uw.zookeeper.safari.storage;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers.CacheNodeCreatedListener;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Fetches session state but assumes that some other component is fetching snapshot state.
 */
public class ExpireSnapshotSessions extends AbstractWatchListener {
    
    public static ExpireSnapshotSessions listen(
            Service service, 
            StorageClientService storage) {
        ExpireSnapshotSessions instance = new ExpireSnapshotSessions(service, storage);
        instance.listen();
        return instance;
    }

    protected final StorageClientService storage;
    
    protected ExpireSnapshotSessions(
            Service service, 
            StorageClientService storage) {
        super(service, 
                storage.cacheEvents(), 
                WatchMatcher.exact(
                        StorageSchema.Safari.Sessions.Session.PATH, 
                        Watcher.Event.EventType.NodeDeleted));
        this.storage = storage;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        ImmutableList<AbsoluteZNodePath> expired = ImmutableList.of();
        final ZNodeLabel label = ((AbsoluteZNodePath) event.getPath()).label();
        storage.materializer().cache().lock().readLock().lock();
        try {
            for (StorageZNode<?> volume: storage.materializer().cache().cache().get(StorageSchema.Safari.Volumes.PATH).values()) {
                StorageSchema.Safari.Volumes.Volume.Snapshot snapshot = ((StorageSchema.Safari.Volumes.Volume) volume).snapshot();
                if ((snapshot != null) && (snapshot.commit() != null)) {
                    for (StorageZNode<?> node: snapshot.values()) {
                        StorageZNode<?> session = node.get(label);
                        if (session != null) {
                            expired = ImmutableList.<AbsoluteZNodePath>builder()
                            .addAll(expired)
                            .add((AbsoluteZNodePath) session.path()).build();
                        }
                    }
                }
            }
        } finally {
            storage.materializer().cache().lock().readLock().unlock();
        }
        if (!expired.isEmpty()) {
            for (AbsoluteZNodePath path: expired) {
                delete(path);
            }
        }
    }

    @Override
    public void starting() {
        super.starting();
        
        for (Class<? extends StorageZNode.SessionZNode<?>> type: ImmutableList.<Class<? extends StorageZNode.SessionZNode<?>>>of(
                StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.Session.class,
                StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.Session.class)) {
            new CheckSession(type).listen();
        }
    }
    
    protected ListenableFuture<AbsoluteZNodePath> delete(AbsoluteZNodePath path) {
        // TODO handle errors
        return DeleteSubtree.deleteAll(path, storage.materializer());
    }
    
    protected class CheckSession extends CacheNodeCreatedListener<StorageZNode<?>> {

        protected final PathToQuery<?,Message.ServerResponse<?>> query;
        
        @SuppressWarnings("unchecked")
        public CheckSession(
                Class<? extends StorageZNode.SessionZNode<?>> type) {
            super(storage.materializer().schema().apply(type).path(),
                    storage.materializer().cache(),
                    ExpireSnapshotSessions.this.service, 
                    ExpireSnapshotSessions.this.watch);
            this.query = PathToQuery.forRequests(storage.materializer(), 
                    Operations.Requests.sync(), 
                    Operations.Requests.exists());
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            cache.lock().readLock().lock();
            try {
                final AbsoluteZNodePath path = StorageSchema.Safari.Sessions.Session.pathOf(((StorageZNode.SessionZNode<?>) cache.cache().get(event.getPath())).name().longValue());
                if (!cache.cache().containsKey(path)) {
                    new Callback((AbsoluteZNodePath) event.getPath(), path);
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        protected class Callback extends ForwardingListenableFuture<List<Message.ServerResponse<?>>> implements Runnable {

            protected final AbsoluteZNodePath eventPath;
            protected final ListenableFuture<List<Message.ServerResponse<?>>> future;
            
            public Callback(AbsoluteZNodePath eventPath, AbsoluteZNodePath path) {
                this.eventPath = eventPath;
                this.future = Futures.allAsList(query.apply(path).call());
                addListener(this, SameThreadExecutor.getInstance());
            }
            
            @Override
            public void run() {
                if (isDone()) {
                    Optional<Operation.Error> error = null;
                    try {
                        List<Message.ServerResponse<?>> responses = get();
                        for (Message.ServerResponse<?> response: responses) {
                            error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                        }
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    } catch (Exception e) {
                        stopping(state());
                        return;
                    }
                    if (error.isPresent()) {
                        delete(eventPath);
                    }
                }
            }

            @Override
            protected ListenableFuture<List<Message.ServerResponse<?>>> delegate() {
                return future;
            }
            
        }
    }
}
