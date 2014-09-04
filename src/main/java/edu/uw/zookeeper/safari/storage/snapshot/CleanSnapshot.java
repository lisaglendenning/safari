package edu.uw.zookeeper.safari.storage.snapshot;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IDeleteRequest;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Assumes that snapshot entries are watched.
 */
public final class CleanSnapshot<O extends Operation.ProtocolResponse<?>> extends Watchers.CacheNodeCreatedListener<StorageZNode<?>> {

    public static <O extends Operation.ProtocolResponse<?>> CleanSnapshot<O> listen(
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service) {
        CleanSnapshot<O> instance = new CleanSnapshot<O>(materializer, cacheEvents, service);
        instance.listen();
        ImmutableList.Builder<WatchMatchListener> listeners = ImmutableList.<WatchMatchListener>builder().add(instance);
        for (ValueNode<ZNodeSchema> node: materializer.schema().get().get(instance.getWatchMatcher().getPath()).values()) {
            if (!node.isEmpty()) {
                Watchers.CacheNodeCreatedListener<StorageZNode<?>> listener = Watchers.CacheNodeCreatedListener.create(
                        materializer.cache(), 
                        service, 
                        cacheEvents, 
                        instance.new SnapshotChildrenListener(node.path()), 
                        instance.logger());
                listeners.add(listener);
                listener.listen();
            }
        }
        Watchers.CacheNodeCreatedListener.create(
                materializer.cache(), 
                service, 
                cacheEvents, 
                SnapshotCommittedListener.create(listeners.build(), materializer.cache(), instance.logger()),
                instance.logger());
        return instance;
    }
    
    private final DeleteSnapshot<O> delete;
    private final Materializer<StorageZNode<?>,O> materializer;
    
    protected CleanSnapshot(
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service) {
        super(materializer.cache(), 
                service, 
                cacheEvents,
                WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.PATH,
                        Watcher.Event.EventType.NodeChildrenChanged));
        this.materializer = materializer;
        this.delete = DeleteSnapshot.forClient(materializer);
    }

    @Override
    public void handleWatchEvent(final WatchEvent event) {
        super.handleWatchEvent(event);
        final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot snapshot = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot) cache.cache().get(event.getPath());
        if ((snapshot.size() == 1) && (snapshot.commit() != null) && (snapshot.commit().data().get() != null) && snapshot.commit().data().get().booleanValue()) {
            new DeleteCallback<IMultiRequest>(delete.apply(event.getPath()));
        }
    }
    
    @Override
    public synchronized void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        Services.stop(service);
    }
    
    protected static final class DeleteSnapshot<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<ZNodePath, O> {
        
        public static <O extends Operation.ProtocolResponse<?>> DeleteSnapshot<O> forClient(
                ClientExecutor<? super Records.Request, O, ?> client) {
            return new DeleteSnapshot<O>(client);
        }
        
        private final ClientExecutor<? super Records.Request, O, ?> client;
        private final Operations.Requests.Delete delete;
        
        protected DeleteSnapshot(ClientExecutor<? super Records.Request, O, ?> client) {
            this.client = client;
            this.delete = Operations.Requests.delete();
        }
        
        @Override
        public SubmittedRequest<IMultiRequest, O> apply(ZNodePath input) {
            return SubmittedRequest.submit(
                    client,
                    new IMultiRequest(ImmutableList.<Records.MultiOpRequest>of(
                            delete.setPath(input.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.LABEL)).build(), 
                            delete.setPath(input).build())));
        }
 
        @Override
        public String toString() {
            return Objects.toStringHelper(this).toString();
        }
    }
    
    protected final class DeleteCallback<I extends Records.Request> extends ToStringListenableFuture<O> implements Runnable {
        
        private final SubmittedRequest<I,O> request;

        public DeleteCallback(I request) {
            this(SubmittedRequest.submit(materializer, request));
        }
        
        public DeleteCallback(SubmittedRequest<I,O> request) {
            this.request = request;
            addListener(this, SameThreadExecutor.getInstance());
        }

        @Override
        public void run() {
            if (isDone()) {
                try {
                    if (!isCancelled()) {
                        Operation.ProtocolResponse<?> response = get();
                        if (response.record() instanceof IMultiResponse) {
                            Operations.maybeMultiError((IMultiResponse) response.record(), KeeperException.Code.NONODE, KeeperException.Code.NOTEMPTY);
                        } else {
                            Operations.maybeError(response.record(), KeeperException.Code.NONODE, KeeperException.Code.NOTEMPTY);
                        }
                    }
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (Exception e) {
                    CleanSnapshot.this.failed(state(), e);
                }
            }
        }

        @Override
        protected ListenableFuture<O> delegate() {
            return request;
        }
    }
    
    /**
     * Deletes snapshot children when they are empty.
     */
    protected final class SnapshotChildrenListener extends LoggingWatchMatchListener {

        private final Operations.Requests.Delete delete;
        
        public SnapshotChildrenListener(ZNodePath path) {
            super(WatchMatcher.exact(
                        path, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeChildrenChanged), 
                    CleanSnapshot.this.logger());
            this.delete = Operations.Requests.delete();
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            final StorageZNode<?> node = materializer.cache().cache().get(event.getPath());
            if (node.isEmpty()) {
                final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot snapshot = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot) node.parent().get();
                if ((snapshot.commit() != null) && (snapshot.commit().data().get() != null) && snapshot.commit().data().get().booleanValue()) {
                    new DeleteCallback<IDeleteRequest>(delete.setPath(event.getPath()).build());
                }
            }
        }
    }
    
    /**
     * Replays events that may have been waiting for snapshot commit.
     */
    protected static final class SnapshotCommittedListener extends LoggingWatchMatchListener {

        public static SnapshotCommittedListener create(
                ImmutableList<WatchMatchListener> listeners,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Logger logger) {
            return new SnapshotCommittedListener(listeners, cache, logger);
        }
        
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final ImmutableList<WatchMatchListener> listeners;
        
        protected SnapshotCommittedListener(
                ImmutableList<WatchMatchListener> listeners,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Logger logger) {
            super(WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH, 
                        Watcher.Event.EventType.NodeDataChanged), 
                    logger);
            this.cache = cache;
            this.listeners = listeners;
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            for (WatchMatchListener listener: listeners) {
                ZNodePath path = listener.getWatchMatcher().getPath();
                if (cache.cache().containsKey(path)) {
                    listener.handleWatchEvent(NodeWatchEvent.nodeChildrenChanged(path));
                }
            }
        }
    }
}
