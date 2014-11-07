package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Functions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.EscapedConverter;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class SequentialEphemeralIterator extends Watchers.CacheNodeCreatedListener<StorageZNode<?>> {
    
    public static <O extends Operation.ProtocolResponse<?>> SequentialEphemeralIterator create(
            ZNodePath ephemerals,
            SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> trie,
            Set<AbsoluteZNodePath> leaves,
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        final Map<ZNodePath, CommittedChildIterator> iterators = 
                Collections.synchronizedMap(
                        Maps.<ZNodePath, CommittedChildIterator>newHashMapWithExpectedSize(
                                leaves.size()));
        for (AbsoluteZNodePath path: leaves) {
            SequentialNode<AbsoluteZNodePath> parent = trie.get(path).parent().get();
            ZNodePath k = parent.path();
            if (!iterators.containsKey(k)) {
                iterators.put(k, CommittedChildIterator.create(parent, materializer));
            }
        }
        SequentialEphemeralIterator instance = new SequentialEphemeralIterator(
                iterators,
                materializer.cache(), 
                service, 
                cacheEvents, 
                SequentialEphemeralListener.create(
                        ephemerals, 
                        materializer.cache().cache(), 
                        iterators, 
                        logger), 
                logger);
        return instance;
    }

    private final Map<ZNodePath, CommittedChildIterator> iterators;
    
    protected SequentialEphemeralIterator(
            Map<ZNodePath, CommittedChildIterator> iterators,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            Service service,
            WatchListeners cacheEvents,
            WatchMatchListener watcher,
            Logger logger) {
        super(cache, 
                service, 
                cacheEvents,
                watcher, 
                logger);
        this.iterators = iterators;
    }
    
    /**
     * @return synchronized map
     */
    public Map<ZNodePath, CommittedChildIterator> iterators() {
        return iterators;
    }

    @Override
    public void stopping(Service.State from) {
        super.stopping(from);
        Exception t = new CancellationException();
        synchronized (iterators) {
            for (CommittedChildIterator v: Iterables.consumingIterable(iterators.values())) {
                v.onFailure(t);
            }
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        synchronized (iterators) {
            for (CommittedChildIterator v: Iterables.consumingIterable(iterators.values())) {
                v.onFailure(failure);
            }
        }
    }
    
    protected static final class SequentialEphemeralListener extends LoggingWatchMatchListener {

        public static SequentialEphemeralListener create(
                ZNodePath ephemerals,
                NameTrie<StorageZNode<?>> cache,
                Map<ZNodePath, ? extends FutureCallback<ZNodePath>> callbacks,
                Logger logger) {
            return new SequentialEphemeralListener(
                    cache,
                    callbacks,
                    WatchMatcher.exact(
                            ephemerals.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral.PATH.suffix(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.PATH)),
                            Watcher.Event.EventType.NodeChildrenChanged,
                            Watcher.Event.EventType.NodeCreated,
                            Watcher.Event.EventType.NodeDeleted),
                    logger);
        }
        
        private final NameTrie<StorageZNode<?>> cache;
        private final Map<ZNodePath, ? extends FutureCallback<ZNodePath>> callbacks;
        
        protected SequentialEphemeralListener(
                NameTrie<StorageZNode<?>> cache,
                Map<ZNodePath, ? extends FutureCallback<ZNodePath>> callbacks,
                WatchMatcher matcher, 
                Logger logger) {
            super(matcher, logger);
            this.cache = cache;
            this.callbacks = callbacks;
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            StorageZNode.EscapedNamedZNode<?> node = (StorageZNode.EscapedNamedZNode<?>) cache.get(event.getPath());
            String name;
            if (node != null) {
                name = node.name();
            } else {
                name = EscapedConverter.getInstance().reverse().convert(event.getPath().label().toString());
            }
            ZNodePath path = ((AbsoluteZNodePath) ZNodePath.root().join(ZNodeName.fromString(name))).parent();
            FutureCallback<ZNodePath> callback = callbacks.get(path);
            if (callback != null) {
                callback.onSuccess(event.getPath());
            }
        }
    }
    
    /**
     * Sets a watch on the next uncommitted sequential child.
     * 
     * Iterator functions are not thread safe.
     */
    public static final class CommittedChildIterator extends AbstractIterator<CommittedChildIterator.NextChild<?>> implements PeekingIterator<CommittedChildIterator.NextChild<?>>, FutureCallback<ZNodePath> {

        public static <O extends Operation.ProtocolResponse<?>> CommittedChildIterator create(
                SequentialNode<AbsoluteZNodePath> parent,
                Materializer<StorageZNode<?>,O> materializer) {
            return new CommittedChildIterator(
                    WatchCommit.getCommitData(materializer),
                    parent, 
                    materializer.cache());
        }

        private final AsyncFunction<? super ZNodePath,?> watchCommit;
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final SequentialNode<AbsoluteZNodePath> parent;
        private final Iterator<SequentialNode<AbsoluteZNodePath>> children;
        private final Deque<NextChild<Promise<AbsoluteZNodePath>>> pending;
        
        protected CommittedChildIterator(
                AsyncFunction<? super ZNodePath,?> watchCommit,
                SequentialNode<AbsoluteZNodePath> parent,
                LockableZNodeCache<StorageZNode<?>,?,?> cache) {
            this.cache = cache;
            this.parent = parent;
            this.watchCommit = watchCommit;
            this.children = parent.values().iterator();
            this.pending = Queues.newArrayDeque();
        }
        
        public SequentialNode<AbsoluteZNodePath> parent() {
            return parent;
        }
        
        @Override
        public void onSuccess(final ZNodePath result) {
            cache.lock().readLock().lock();
            try {
                synchronized (this) {
                    for (NextChild<Promise<AbsoluteZNodePath>> p: pending) {
                        if (p.getNode().getValue().equals(result)) {
                            StorageZNode<?> node = cache.cache().get(result);
                            if (node == null) {
                                p.getCommitted().setException(new KeeperException.NoNodeException());
                            } else {
                                if (node.containsKey(StorageZNode.CommitZNode.LABEL)) {
                                    p.getCommitted().set(p.getNode().getValue());
                                }
                            }
                            break;
                        }
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        @Override
        public void onFailure(Throwable t) {
            synchronized (this) {
                NextChild<Promise<AbsoluteZNodePath>> p;
                while ((p = pending.poll()) != null) {
                    if (t instanceof CancellationException) {
                        p.getCommitted().cancel(false);
                    } else {
                        p.getCommitted().setException(t);
                    }
                }
            }
        }

        @Override
        protected NextChild<?> computeNext() {
            cache.lock().readLock().lock();
            try {
                synchronized (this) {
                    if (!children.hasNext()) {
                        return endOfData();
                    }
                    SequentialNode<AbsoluteZNodePath> next = children.next();
                    StorageZNode<?> node = cache.cache().get(next.getValue());
                    if (node == null) {
                        return NextChild.create(next, Futures.<AbsoluteZNodePath>immediateFailedFuture(new KeeperException.NoNodeException()));
                    } else {
                        if (node.containsKey(StorageZNode.CommitZNode.LABEL)) {
                            return NextChild.create(next, Futures.immediateFuture((AbsoluteZNodePath) next.path()));
                        } else {
                            Promise<AbsoluteZNodePath> future = SettableFuturePromise.create();
                            NextChild<Promise<AbsoluteZNodePath>> child = NextChild.create(next, future);
                            pending.add(child);
                            new WatchChild(child).run();
                            return child;
                        }
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        public static final class NextChild<V extends ListenableFuture<AbsoluteZNodePath>> extends AbstractPair<SequentialNode<AbsoluteZNodePath>, V> {

            public static <V extends ListenableFuture<AbsoluteZNodePath>> NextChild<V> create(SequentialNode<AbsoluteZNodePath> node,
                    V committed) {
                return new NextChild<V>(node, committed);
            }
            
            protected NextChild(SequentialNode<AbsoluteZNodePath> node,
                    V committed) {
                super(node, committed);
            }
            
            public SequentialNode<AbsoluteZNodePath> getNode() {
                return first;
            }
            
            public V getCommitted() {
                return second;
            }
        }
        
        protected final class WatchChild implements Runnable, FutureCallback<Object> {
            
            private final NextChild<Promise<AbsoluteZNodePath>> child;
            
            protected WatchChild(
                    NextChild<Promise<AbsoluteZNodePath>> child) {
                this.child = child;
            }
            
            @Override
            public void run() {
                if (child.getCommitted().isDone()) {
                    synchronized (CommittedChildIterator.this) {
                        pending.remove(child);
                    }
                } else {
                    try {
                        Futures.addCallback(
                                watchCommit.apply(child.getNode().getValue()), 
                                this);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                    child.getCommitted().addListener(this, MoreExecutors.directExecutor());
                }
            }

            @Override
            public void onSuccess(Object result) {
            }

            @Override
            public void onFailure(Throwable t) {
                child.getCommitted().setException(t);
            }
        }
        
        protected static final class WatchCommit<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<ZNodePath, List<O>> {

            public static <O extends Operation.ProtocolResponse<?>> WatchCommit<O> getCommitData(
                    ClientExecutor<? super Records.Request,O,?> client) {
                @SuppressWarnings("unchecked")
                PathToQuery<?,O> query = PathToQuery.forFunction(
                        client,
                        Functions.compose(
                                PathToRequests.forRequests(
                                        Operations.Requests.sync(), 
                                        Operations.Requests.getData().setWatch(true)), 
                                JoinToPath.forName(
                                        StorageZNode.CommitZNode.LABEL)));
                return new WatchCommit<O>(query);
            }
            
            private final PathToQuery<?,O> query;
            
            protected WatchCommit(
                    PathToQuery<?,O> query) {
                this.query = query;
            }
            
            @Override
            public ListenableFuture<List<O>> apply(ZNodePath input) throws Exception {
                return Futures.allAsList(query.apply(input).call());
            }
        }
    }
}
