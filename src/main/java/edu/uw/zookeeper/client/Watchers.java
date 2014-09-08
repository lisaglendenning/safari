package edu.uw.zookeeper.client;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NameType;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.schema.SafariZNode;

public abstract class Watchers {
    
    @SuppressWarnings("unchecked")
    public static <O extends Operation.ProtocolResponse<?>> Watchers.RunnableServiceListener<?> watchChildren(
            ZNodePath path,
            ClientExecutor<? super Records.Request,O,?> client,
            Service service,
            WatchListeners notifications) {
        final Watchers.FixedQueryRunnable<O,?> query = Watchers.FixedQueryRunnable.create(
                FixedQuery.forIterable(
                    client, 
                    PathToRequests.forRequests(
                            Operations.Requests.sync(), 
                            Operations.Requests.getChildren().setWatch(true))
                            .apply(path)),
                Watchers.MaybeErrorProcessor.maybeNoNode(),
                Watchers.StopServiceOnFailure.create(service));
        return Watchers.RunnableServiceListener.listen(
                query, 
                service, 
                notifications,
                WatchMatcher.exact(
                        path, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeChildrenChanged, 
                        Watcher.Event.EventType.NodeDeleted));
    }
    
    public static class SetExceptionCallback<T,V> extends ToStringListenableFuture<V> implements FutureCallback<T> {

        public static <T,V> SetExceptionCallback<T,V> create(
                Promise<V> delegate) {
            return new SetExceptionCallback<T,V>(delegate);
        }
        
        private final Promise<V> delegate;
        
        protected SetExceptionCallback(
                Promise<V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSuccess(T result) {
        }

        @Override
        public void onFailure(Throwable t) {
            delegate().setException(t);
        }

        @Override
        protected Promise<V> delegate() {
            return delegate;
        }
    }
    
    public static final class UnsubscribeWhenDone<V> extends SimpleToStringListenableFuture<V> implements Runnable {

        public static <T extends WatchMatchListener, V> T subscribe(
                ListenableFuture<V> delegate,
                T listener,
                WatchListeners watch) {
            watch.subscribe(listener);
            listen(delegate, listener, watch);
            return listener;
        }
        
        public static <V> UnsubscribeWhenDone<V> listen(
                ListenableFuture<V> delegate,
                WatchMatchListener listener,
                WatchListeners watch) {
            UnsubscribeWhenDone<V> callback = new UnsubscribeWhenDone<V>(delegate, watch, listener);
            callback.run();
            return callback;
        }
        
        private final WatchListeners watch;
        private final WatchMatchListener listener;
        
        private UnsubscribeWhenDone(
                ListenableFuture<V> delegate,
                WatchListeners watch,
                WatchMatchListener listener) {
            super(delegate);
            this.watch = watch;
            this.listener = listener;
        }
        
        @Override
        public void run() {
            if (isDone()) {
                watch.unsubscribe(listener);
            } else {
                addListener(this, MoreExecutors.directExecutor());
            }
        }
    }
    
    public static class StopServiceOnFailure<V> implements FutureCallback<V> {

        public static <V> StopServiceOnFailure<V> create(Service service) {
            return new StopServiceOnFailure<V>(service);
        }
        
        protected final Service service;

        protected StopServiceOnFailure(Service service) {
            this.service = service;
        }
        
        @Override
        public void onSuccess(V result) {
        }

        @Override
        public void onFailure(Throwable t) {
            Services.stop(service);
        }
    }
    
    public static class FailWatchListener<V> implements FutureCallback<V> {

        public static <V> FailWatchListener<V> create(WatchMatchServiceListener listener) {
            return new FailWatchListener<V>(listener);
        }
        
        protected final WatchMatchServiceListener listener;

        protected FailWatchListener(WatchMatchServiceListener listener) {
            this.listener = listener;
        }
        
        @Override
        public void onSuccess(V result) {
        }

        @Override
        public void onFailure(Throwable t) {
            listener.failed(listener.state(), t);
        }
    }
    
    public static class FutureCallbackListener<T extends FutureCallback<? super WatchEvent>> extends LoggingWatchMatchListener {

        public static <T extends FutureCallback<? super WatchEvent>> FutureCallbackListener<T> create(
                T callback, 
                WatchMatcher matcher) {
            return new FutureCallbackListener<T>(callback, matcher);
        }

        public static <T extends FutureCallback<? super WatchEvent>> FutureCallbackListener<T> create(
                T callback, 
                WatchMatcher matcher, 
                Logger logger) {
            return new FutureCallbackListener<T>(callback, matcher, logger);
        }
        
        protected final T callback;
        
        protected FutureCallbackListener(
                T callback, 
                WatchMatcher matcher) {
            super(matcher);
            this.callback = callback;
        }

        protected FutureCallbackListener(
                T callback, 
                WatchMatcher matcher, 
                Logger logger) {
            super(matcher, logger);
            this.callback = callback;
        }
        
        public T callback() {
            return callback;
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            callback.onSuccess(event);
        }
        
        @Override
        protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
            return super.toString(helper.addValue(callback));
        }
    }
    
    public static class FutureCallbackServiceListener<T extends FutureCallback<? super WatchEvent>> extends WatchMatchServiceListener {

        public static <T extends FutureCallback<? super WatchEvent>> FutureCallbackServiceListener<T> listen(
                T callback,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher,
                Logger logger) {
            FutureCallbackServiceListener<T> instance = create(callback, service, watch, matcher, logger);
            instance.listen();
            return instance;
        }
        
        public static <T extends FutureCallback<? super WatchEvent>> FutureCallbackServiceListener<T> create(
                T callback,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher,
                Logger logger) {
            return new FutureCallbackServiceListener<T>(callback, service, watch, matcher, logger);
        }
        
        protected final T callback;

        protected FutureCallbackServiceListener(
                T callback,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher) {
            super(service, watch, matcher);
            this.callback = callback;
        }
        
        protected FutureCallbackServiceListener(
                T callback,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher,
                Logger logger) {
            super(service, watch, matcher, logger);
            this.callback = callback;
        }

        protected FutureCallbackServiceListener(
                T callback,
                Service service,
                WatchListeners watch,
                WatchMatchListener watcher) {
            super(service, watch, watcher);
            this.callback = callback;
        }

        protected FutureCallbackServiceListener(
                T callback, 
                Service service,
                WatchListeners watch,
                WatchMatchListener watcher,
                Logger logger) {
            super(service, watch, watcher, logger);
            this.callback = callback;
        }
        
        public T callback() {
            return callback;
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            callback.onSuccess(event);
        }
        
        @Override
        public void failed(Service.State from, Throwable failure) {
            super.failed(from, failure);
            callback.onFailure(failure);
        }
        
        @Override
        protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
            return super.toString(helper.addValue(callback));
        }
    }
    
    public static class RunnableListener<T extends Runnable> extends LoggingWatchMatchListener implements Runnable {

        public static <T extends Runnable> RunnableListener<T> create(
                T runnable,
                WatchMatcher matcher, 
                Logger logger) {
            return new RunnableListener<T>(runnable, matcher, logger);
        }
        
        protected final T runnable;
    
        protected RunnableListener(
                T runnable,
                WatchMatcher matcher) {
            super(matcher);
            this.runnable = runnable;
        }

        protected RunnableListener(
                T runnable,
                WatchMatcher matcher, 
                Logger logger) {
            super(matcher, logger);
            this.runnable = runnable;
        }

        @Override
        public void run() {
            runnable.run();
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            run();
        }
        
        @Override
        protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
            return super.toString(helper.addValue(runnable));
        }
    }

    public static class RunnableServiceListener<T extends Runnable> extends WatchMatchServiceListener implements Runnable {
    
        public static <T extends Runnable> RunnableServiceListener<T> listen(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher) {
            RunnableServiceListener<T> listener = create(runnable, service, watch, matcher);
            listener.listen();
            return listener;
        }
    
        public static <T extends Runnable> RunnableServiceListener<T> create(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher) {
            return new RunnableServiceListener<T>(runnable, service, watch, matcher);
        }
        
        protected final T runnable;
    
        public RunnableServiceListener(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher) {
            super(service, watch, matcher);
            this.runnable = runnable;
        }
        
        public RunnableServiceListener(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher,
                Logger logger) {
            super(service, watch, matcher, logger);
            this.runnable = runnable;
        }

        public RunnableServiceListener(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatchListener watcher) {
            super(service, watch, watcher);
            this.runnable = runnable;
        }

        public RunnableServiceListener(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatchListener watcher,
                Logger logger) {
            super(service, watch, watcher, logger);
            this.runnable = runnable;
        }
        
        public T runnable() {
            return runnable;
        }
        
        @Override
        public void run() {
            if (isRunning()) {
                runnable.run();
            }
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            run();
        }
    
        @Override
        public void running() {
            super.running();
            run();
        }
        
        @Override
        protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
            return super.toString(helper.addValue(runnable));
        }
    }
    
    public static class PathToQueryCallback<O extends Operation.ProtocolResponse<?>, V> implements FutureCallback<ZNodePath> {

        public static <O extends Operation.ProtocolResponse<?>, V> PathToQueryCallback<O,V> create(
                PathToQuery<?,O> query,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback) {
            return new PathToQueryCallback<O,V>(query, processor, callback);
        }
        
        protected final PathToQuery<?,O> query;
        protected final Processor<? super List<O>, V> processor;
        protected final FutureCallback<? super V> callback;

        protected PathToQueryCallback(
                PathToQuery<?,O> query,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback) {
            this.query = query;
            this.processor = processor;
            this.callback = callback;
        }

        @Override
        public void onSuccess(ZNodePath result) {
            Watchers.Query.call(
                    processor,
                    callback,
                    query.apply(result)).run();
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(query).toString();
        }
    }
    
    public static class EventToPathCallback<T extends FutureCallback<? super ZNodePath>> implements FutureCallback<WatchEvent> {

        public static <T extends FutureCallback<? super ZNodePath>> EventToPathCallback<T> create(
                T callback) {
            return new EventToPathCallback<T>(callback);
        }
        
        protected final T callback;

        protected EventToPathCallback(
                T callback) {
            this.callback = callback;
        }
        
        public T callback() {
            return callback;
        }

        @Override
        public void onSuccess(WatchEvent result) {
            callback.onSuccess(result.getPath());
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
        
        @Override
        public String toString() {
            return callback.toString();
        }
    }
    
    public static class EventToQueryCallback<O extends Operation.ProtocolResponse<?>, V> implements FutureCallback<WatchEvent> {

        public static <O extends Operation.ProtocolResponse<?>, V> EventToQueryCallback<O,V> create(
                ClientExecutor<? super Records.Request, O, ?> client,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback) {
            return new EventToQueryCallback<O,V>(queries(client), processor, callback);
        }
        
        @SuppressWarnings("unchecked")
        public static <O extends Operation.ProtocolResponse<?>> ImmutableMap<Watcher.Event.EventType, PathToQuery<?,O>> queries(
                ClientExecutor<? super Records.Request, O, ?> client) {
            ImmutableMap.Builder<Watcher.Event.EventType, PathToQuery<?,O>> queries = ImmutableMap.builder();
            for (Watcher.Event.EventType type: Watcher.Event.EventType.values()) {
                final Optional<? extends Operations.Builder<? extends Records.Request>> query;
                switch (type) {
                case NodeChildrenChanged:
                    query = Optional.of(
                                Operations.Requests.getChildren().setWatch(true));
                    break;
                case NodeDeleted:
                    query = Optional.of(
                                Operations.Requests.exists());
                break;
                case NodeCreated:
                    query = Optional.of(
                            Operations.Requests.exists().setWatch(true));
                    break;
                case NodeDataChanged:
                    query = Optional.of(
                                Operations.Requests.getData().setWatch(true));
                    break;
                default:
                    query = Optional.absent();
                    break;
                }
                if (query.isPresent()) {
                    queries.put(type, PathToQuery.forRequests(
                            client,
                            Operations.Requests.sync(), 
                            query.get()));
                }
            }
            return queries.build();
        }

        protected final ImmutableMap<Watcher.Event.EventType, PathToQuery<?,O>> queries;
        protected final Processor<? super List<O>, V> processor;
        protected final FutureCallback<? super V> callback;

        protected EventToQueryCallback(
                ImmutableMap<Watcher.Event.EventType, PathToQuery<?,O>> queries,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback) {
            this.queries = queries;
            this.processor = processor;
            this.callback = callback;
        }

        @Override
        public void onSuccess(WatchEvent result) {
            Optional<? extends PathToQuery<?,O>> query = 
                    Optional.fromNullable(queries.get(result.getEventType()));
            if (query.isPresent()) {
                Watchers.Query.call(
                        processor,
                        callback,
                        query.get().apply(result.getPath())).run();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(queries).toString();
        }
    }
    
    public static class FixedQueryRunnable<O extends Operation.ProtocolResponse<?>, V> implements Runnable {

        public static <O extends Operation.ProtocolResponse<?>, V> FixedQueryRunnable<O,V> create(
                FixedQuery<O> query,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback) {
            return new FixedQueryRunnable<O,V>(query, processor, callback);
        }
        
        protected final FixedQuery<O> query;
        protected final Processor<? super List<O>, V> processor;
        protected final FutureCallback<? super V> callback;

        protected FixedQueryRunnable(
                FixedQuery<O> query,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback) {
            this.query = query;
            this.processor = processor;
            this.callback = callback;
        }
        
        @Override
        public void run() {
            Watchers.Query.call(
                    processor,
                    callback,
                    query).run();
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(query).toString();
        }
    }
    
    public static class DispatchingWatchMatchListener extends LoggingWatchMatchListener {

        public static DispatchingWatchMatchListener create(
                Iterable<? extends WatchMatchListener> listeners,
                WatchMatcher matcher, 
                Logger logger) {
            return new DispatchingWatchMatchListener(
                    Lists.newCopyOnWriteArrayList(listeners), matcher, logger);
        }
        
        public static boolean deliver(WatchEvent event, WatchMatchListener listener) {
            if (listener.getWatchMatcher().getEventType().contains(event.getEventType())) {
                listener.handleWatchEvent(event);
                return true;
            } else {
                return false;
            }
        }
        
        protected final CopyOnWriteArrayList<WatchMatchListener> listeners;
        
        protected DispatchingWatchMatchListener(
                CopyOnWriteArrayList<WatchMatchListener> listeners,
                WatchMatcher matcher, 
                Logger logger) {
            super(matcher, logger);
            this.listeners = listeners;
        }

        public CopyOnWriteArrayList<WatchMatchListener> getListeners() {
            return listeners;
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            for (WatchMatchListener listener: listeners) {
                deliver(event, listener);
            }
        }

        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            super.handleAutomatonTransition(transition);
            for (WatchMatchListener listener: listeners) {
                listener.handleAutomatonTransition(transition);
            }
        }

        protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
            return super.toString(helper).addValue(listeners);
        }
    }

    public static final class PathToNodeCallback<U extends SafariZNode<U,?>, V extends U, T extends FutureCallback<? super V>> implements FutureCallback<ZNodePath> {

        public static <U extends SafariZNode<U,?>, V extends U, T extends FutureCallback<? super V>> PathToNodeCallback<U,V,T> create(
                T callback,
                NameTrie<U> trie) {
            return new PathToNodeCallback<U,V,T>(trie, callback);
        }
        
        private final NameTrie<U> trie;
        private final T callback;
        
        private PathToNodeCallback(
                NameTrie<U> trie,
                T callback) {
            this.trie = trie;
            this.callback = callback;
        }
        
        public T callback() {
            return callback;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void onSuccess(ZNodePath result) {
            callback.onSuccess((V) trie.get(result)); 
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
        
        @Override
        public String toString() {
            return callback.toString();
        }
    }
    
    /**
     * Should be robust to repeat events
     */
    public static class CacheNodeCreatedListener<E extends Materializer.MaterializedNode<E,?>> extends WatchMatchServiceListener {

        public static <E extends Materializer.MaterializedNode<E,?>> CacheNodeCreatedListener<E> listen(
                LockableZNodeCache<E,?,?> cache,
                Service service,
                WatchListeners cacheEvents,
                WatchMatchListener watcher,
                Logger logger) {
            CacheNodeCreatedListener<E> instance = create(
                    cache, service, cacheEvents, watcher, logger);
            instance.listen();
            return instance;
        }
        
        public static <E extends Materializer.MaterializedNode<E,?>> CacheNodeCreatedListener<E> create(
                LockableZNodeCache<E,?,?> cache,
                Service service,
                WatchListeners cacheEvents,
                WatchMatchListener watcher,
                Logger logger) {
            return new CacheNodeCreatedListener<E>(
                    cache, service, cacheEvents, watcher, logger);
        }
        
        protected final LockableZNodeCache<E,?,?> cache;
        
        protected CacheNodeCreatedListener(
                ZNodePath path,
                LockableZNodeCache<E,?,?> cache,
                Service service,
                WatchListeners cacheEvents) {
            this(cache, 
                    service, 
                    cacheEvents, 
                    WatchMatcher.exact(
                            path,
                            Watcher.Event.EventType.NodeCreated));
        }
        
        protected CacheNodeCreatedListener(
                LockableZNodeCache<E,?,?> cache,
                Service service,
                WatchListeners cacheEvents,
                WatchMatcher match,
                Logger logger) {
            super(service, cacheEvents, match, logger);
            this.cache = cache;
        }
        
        protected CacheNodeCreatedListener(
                LockableZNodeCache<E,?,?> cache,
                Service service,
                WatchListeners cacheEvents,
                WatchMatcher match) {
            super(service, cacheEvents, match);
            this.cache = cache;
        }
        
        protected CacheNodeCreatedListener(
                LockableZNodeCache<E,?,?> cache,
                Service service,
                WatchListeners cacheEvents,
                WatchMatchListener watcher,
                Logger logger) {
            super(service, cacheEvents, watcher, logger);
            this.cache = cache;
        }
        
        @Override
        public void running() {
            super.running();
            cache.lock().readLock().lock();
            try {
                Iterator<? extends WatchEvent> events = replay();
                while (events.hasNext()) {
                    handleWatchEvent(events.next());
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        protected Iterator<? extends WatchEvent> replay() {
            return MatchingNodeIterator.create(
                    getWatchMatcher(), 
                    MatchingTrieIterator.forRoot(
                            getWatchMatcher().getPath(), 
                            getWatchMatcher().getPathType(), 
                            cache.cache().root()));
        }
        
        public static final class MatchingNodeIterator<E extends Materializer.MaterializedNode<E,?>> extends AbstractIterator<NodeWatchEvent> {

            public static <E extends Materializer.MaterializedNode<E,?>> MatchingNodeIterator<E> create(
                    WatchMatcher match,
                    Iterator<E> nodes) {
                return new MatchingNodeIterator<E>(nodes, match);
            }
            
            protected final WatchMatcher match;
            protected final Iterator<E> nodes;
            
            public MatchingNodeIterator(
                    Iterator<E> nodes,
                    WatchMatcher match) {
                this.match = match;
                this.nodes = nodes;
            }
            
            @Override
            protected NodeWatchEvent computeNext() {
                while (nodes.hasNext()) {
                    final E node = nodes.next();
                    if (match.getEventType().contains(Watcher.Event.EventType.NodeDataChanged)) {
                        Optional<? extends NodeWatchEvent> event;
                        if (node.data().stamp() > 0L) {
                            event = Optional.of(NodeWatchEvent.nodeDataChanged(node.path()));
                        } else if (match.getEventType().contains(Watcher.Event.EventType.NodeCreated)) {
                            event = Optional.of(NodeWatchEvent.nodeCreated(node.path()));
                        } else {
                            event = Optional.absent();
                        }
                        if (event.isPresent()) {
                            return event.get();
                        }
                    } else if (match.getEventType().contains(Watcher.Event.EventType.NodeCreated)) {
                        return NodeWatchEvent.nodeCreated(node.path());
                    } else if (match.getEventType().contains(Watcher.Event.EventType.NodeChildrenChanged)) {
                        if (!node.schema().isEmpty()) {
                            return NodeWatchEvent.nodeChildrenChanged(node.path());
                        }
                    }
                }
                return endOfData();
            }
        }
    }
    
    public static class MatchingTrieIterator<E extends Materializer.MaterializedNode<E,?>> extends AbstractIterator<E> {

        public static <E extends Materializer.MaterializedNode<E,?>> MatchingTrieIterator<E> forRoot(
                ZNodePath path, 
                WatchMatcher.PathMatchType pathType,
                E root) {
            return new MatchingTrieIterator<E>(path, pathType, root);
        }
        
        protected final ImmutableList<ZNodeLabel> labels;
        protected final WatchMatcher.PathMatchType pathType;
        protected final LinkedList<Pair<ImmutableList<ZNodeLabel>,E>> pending;
        
        public MatchingTrieIterator(
                ZNodePath path, 
                WatchMatcher.PathMatchType pathType,
                E root) {
            this.labels = ImmutableList.copyOf(path);
            this.pathType = pathType;
            this.pending = Lists.newLinkedList();
            enqueue(ImmutableList.copyOf(root.path()), root);
        }
        
        protected void enqueue(ImmutableList<ZNodeLabel> labels, E node) {
            pending.add(Pair.create(labels, node));
        }

        @Override
        protected E computeNext() {
            Pair<ImmutableList<ZNodeLabel>,E> next;
            while ((next  = pending.poll()) != null) {
                if (next.first().size() < labels.size()) {
                    ZNodeLabel label = labels.get(next.first().size());
                    E child = next.second().get(label);
                    if (child != null) {
                        enqueue(ImmutableList.<ZNodeLabel>builder().addAll(next.first()).add((ZNodeLabel) child.parent().name()).build(), child);
                    } else {
                        ValueNode<ZNodeSchema> schema = next.second().schema().get(label);
                        if ((schema != null) && (schema.get().getNameType() == NameType.PATTERN)) {
                            for (Map.Entry<ZNodeName, E> e: next.second().entrySet()) {
                                if (e.getValue().schema() == schema) {
                                    enqueue(ImmutableList.<ZNodeLabel>builder().addAll(next.first()).add((ZNodeLabel) e.getKey()).build(), e.getValue());
                                }
                            }
                        }
                    }
                } else {
                    if (pathType == WatchMatcher.PathMatchType.PREFIX) {
                        for (Map.Entry<ZNodeName, E> e: next.second().entrySet()) {
                            enqueue(ImmutableList.<ZNodeLabel>builder().addAll(next.first()).add((ZNodeLabel) e.getKey()).build(), e.getValue());
                        }
                    }
                    return next.second();
                }
            }
            return endOfData();
        }
    }
    
    public static class MaybeErrorProcessor implements Processor<List<? extends Operation.ProtocolResponse<?>>, Optional<Operation.Error>> {
    
        public static MaybeErrorProcessor maybeNoNode() {
            return maybeError(KeeperException.Code.NONODE);
        }
        
        public static MaybeErrorProcessor maybeError(final KeeperException.Code...codes) {
            return new MaybeErrorProcessor(codes);
        }
        
        protected final KeeperException.Code[] codes;
        
        public MaybeErrorProcessor(KeeperException.Code[] codes) {
            this.codes = codes;
        }
        
        @Override
        public Optional<Operation.Error> apply(
                final List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
            Optional<Operation.Error> error = Optional.absent();
            for (Operation.ProtocolResponse<?> response: input) {
                error = Operations.maybeError(response.record(), codes);
            }
            return error;
        }
    }

    public static class Query<O extends Operation.ProtocolResponse<?>, V> extends SimpleToStringListenableFuture<List<O>> implements Runnable {

        public static <O extends Operation.ProtocolResponse<?>, V> Query<O,V> call(
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback,
                FixedQuery<O> query) {
            return create(processor, callback, Futures.allAsList(query.call()));
        }

        public static <O extends Operation.ProtocolResponse<?>, V> Query<O,V> create(
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback,
                ListenableFuture<List<O>> future) {
            return new Query<O,V>(processor, callback, future);
        }
        
        protected final Processor<? super List<O>, V> processor;
        protected final FutureCallback<? super V> callback;

        protected Query(
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback,
                ListenableFuture<List<O>> future) {
            super(future);
            this.processor = processor;
            this.callback = callback;
        }
        
        @Override
        public void run() {
            if (isDone()) {
                V result;
                try {
                    result = processor.apply(get());
                } catch (Exception e) {
                    callback.onFailure(e);
                    return;
                }
                callback.onSuccess(result);
            } else {
                addListener(this, MoreExecutors.directExecutor());
            }
        }
    }
    
    private Watchers() {}
}
