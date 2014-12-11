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
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Processor;
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
import edu.uw.zookeeper.safari.storage.schema.EscapedConverter;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;


public final class SequentialEphemeralSnapshotIterator extends Watchers.CacheNodeCreatedListener<StorageZNode<?>> {
    
    public static <O extends Operation.ProtocolResponse<?>> SequentialEphemeralSnapshotIterator listen(
            ZNodePath ephemerals,
            SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> trie,
            Set<AbsoluteZNodePath> leaves,
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        // initialize iterators for each parent of sequential ephemerals
        final Map<ZNodePath, SequentialChildIterator<?>> iterators = 
                Collections.synchronizedMap(
                        Maps.<ZNodePath, SequentialChildIterator<?>>newHashMapWithExpectedSize(
                                leaves.size()));
        for (AbsoluteZNodePath path: leaves) {
            SequentialNode<AbsoluteZNodePath> parent = trie.get(path).parent().get();
            ZNodePath k = parent.path();
            if (!iterators.containsKey(k)) {
                iterators.put(k, SequentialChildIterator.create(parent, materializer));
            }
        }
        SequentialEphemeralSnapshotIterator instance = new SequentialEphemeralSnapshotIterator(
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
        instance.listen();
        return instance;
    }

    private final Map<ZNodePath, SequentialChildIterator<?>> iterators;
    
    protected SequentialEphemeralSnapshotIterator(
            Map<ZNodePath, SequentialChildIterator<?>> iterators,
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
    public Map<ZNodePath, SequentialChildIterator<?>> iterators() {
        return iterators;
    }

    @Override
    public void stopping(Service.State from) {
        super.stopping(from);
        Exception t = new CancellationException();
        synchronized (iterators) {
            for (SequentialChildIterator<?> v: Iterables.consumingIterable(iterators.values())) {
                v.onFailure(t);
            }
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        synchronized (iterators) {
            for (SequentialChildIterator<?> v: Iterables.consumingIterable(iterators.values())) {
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
     * Assumes another listener will query on commit notification.
     * 
     * Iterator functions are not thread safe.
     */
    public static final class SequentialChildIterator<O extends Operation.ProtocolResponse<?>> extends AbstractIterator<SequentialChildIterator.NextChild<?>> implements PeekingIterator<SequentialChildIterator.NextChild<?>>, FutureCallback<ZNodePath> {

        public static <O extends Operation.ProtocolResponse<?>> SequentialChildIterator<O> create(
                SequentialNode<AbsoluteZNodePath> parent,
                Materializer<StorageZNode<?>,O> materializer) {
            @SuppressWarnings("unchecked")
            final PathToQuery<?,O> query = PathToQuery.forFunction(
                    materializer,
                    Functions.compose(
                            PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.getData().setWatch(true)), 
                            JoinToPath.forName(
                                    StorageZNode.CommitZNode.LABEL)));
            return new SequentialChildIterator<O>(
                    query,
                    parent, 
                    materializer.cache());
        }

        private static final Predicate<Map<?,?>> IS_SEQUENTIAL_EPHEMERAL_CHILD = new Predicate<Map<?,?>>() {
            @Override
            public boolean apply(Map<?,?> input) {
                return input.isEmpty();
            }
        };

        private final Processor<? super List<O>, Optional<Operation.Error>> processor;
        private final PathToQuery<?,O> query;
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final SequentialNode<AbsoluteZNodePath> parent;
        private final Iterator<SequentialNode<AbsoluteZNodePath>> children;
        private final Deque<NextChild<Promise<AbsoluteZNodePath>>> pending;
        
        protected SequentialChildIterator(
                PathToQuery<?,O> query,
                SequentialNode<AbsoluteZNodePath> parent,
                LockableZNodeCache<StorageZNode<?>,?,?> cache) {
            this.cache = cache;
            this.parent = parent;
            this.query = query;
            this.processor = Watchers.MaybeErrorProcessor.maybeNoNode();
            this.children = Iterators.filter(parent.values().iterator(), IS_SEQUENTIAL_EPHEMERAL_CHILD);
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
                            new WatchChild(child).run();
                            return child;
                        }
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(parent).toString();
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
        
        protected final class WatchChild extends Watchers.SimpleForwardingCallback<Optional<Operation.Error>, Watchers.SetExceptionCallback<Object, AbsoluteZNodePath>> implements Runnable {
            
            private final NextChild<Promise<AbsoluteZNodePath>> child;
            
            protected WatchChild(
                    NextChild<Promise<AbsoluteZNodePath>> child) {
                super(Watchers.SetExceptionCallback.create(child.getCommitted()));
                this.child = child;
            }
            
            @Override
            public void run() {
                if (child.getCommitted().isDone()) {
                    synchronized (SequentialChildIterator.this) {
                        pending.remove(child);
                    }
                } else {
                    synchronized (SequentialChildIterator.this) {
                        pending.add(child);
                    }
                    try {
                        Watchers.Query.call(processor, this, query.apply(child.getNode().getValue()));
                    } catch (Exception e) {
                        onFailure(e);
                    } finally {
                        child.getCommitted().addListener(this, MoreExecutors.directExecutor());
                    }
                }
            }

            @Override
            public void onSuccess(Optional<Operation.Error> result) {
                // TODO
            }
        }
    }
}
