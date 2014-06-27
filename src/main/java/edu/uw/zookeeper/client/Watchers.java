package edu.uw.zookeeper.client;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Call;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class Watchers {

    public static <O extends Operation.ProtocolResponse<?>> RunnableWatcher<Call<FixedQuery<O>>> childrenWatcher(
            final ZNodePath path,
            final ClientExecutor<? super Records.Request,O,?> client,
            final Service service,
            final WatchListeners watch) {
        @SuppressWarnings("unchecked")
        final FixedQuery<O> query = FixedQuery.forIterable(client,
                PathToRequests.forRequests(
                        Operations.Requests.sync(),
                        Operations.Requests.getChildren().setWatch(true)).apply(path));
        RunnableWatcher<Call<FixedQuery<O>>> watcher = RunnableWatcher.listen(
                Call.create(query),service, watch,
                WatchMatcher.exact(
                        path, 
                        Watcher.Event.EventType.NodeChildrenChanged));
        StopOnDeleteListener.listen(watcher, path);
        return watcher;
    }
    
    public static class RunnableWatcher<T extends Runnable> extends AbstractWatchListener implements Runnable {

        public static <T extends Runnable> RunnableWatcher<T> listen(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher) {
            RunnableWatcher<T> listener = create(runnable, service, watch, matcher);
            listener.listen();
            return listener;
        }

        public static <T extends Runnable> RunnableWatcher<T> create(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher) {
            return new RunnableWatcher<T>(runnable, service, watch, matcher);
        }
        
        protected final T runnable;

        public RunnableWatcher(
                T runnable,
                Service service,
                WatchListeners watch,
                WatchMatcher matcher) {
            super(service, watch, matcher);
            this.runnable = runnable;
        }
        
        public T runnable() {
            return runnable;
        }
        
        @Override
        public void run() {
            runnable.run();
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            if (service.isRunning()) {
                run();
            }
        }

        @Override
        public void running() {
            run();
        }
    }

    public static class AbsenceWatcher<V, O extends Operation.ProtocolResponse<?>> extends AbstractWatchListener implements Runnable {

        public static <V, O extends Operation.ProtocolResponse<?>> ListenableFuture<V> listen(
                final ZNodePath path,
                final Supplier<V> value,
                final ClientExecutor<? super Records.Request,O,?> client,
                final Service service, 
                final WatchListeners watch) {
            Promise<V> promise = SettableFuturePromise.<V>create();
            AbsenceWatcher<V,O> listener = new AbsenceWatcher<V,O>(
                    path, 
                    client,
                    value,
                    promise,
                    service, 
                    watch);
            listener.listen();
            return promise;
        }
        
        protected final FixedQuery<O> query;
        protected final Promise<V> promise;
        protected final Supplier<V> value;
        
        protected AbsenceWatcher(
                ZNodePath path,
                ClientExecutor<? super Records.Request,O,?> client,
                Supplier<V> value,
                Promise<V> promise,
                Service service, 
                WatchListeners watch) {
            super(service, watch, 
                    WatchMatcher.exact(
                            path, 
                            Watcher.Event.EventType.NodeDeleted));
            this.value = value;
            this.promise = promise;
            @SuppressWarnings("unchecked")
            final List<Records.Request> requests = PathToRequests.forRequests(
                    Operations.Requests.sync(),
                    Operations.Requests.exists().setWatch(true)).apply(path);
            this.query = FixedQuery.forIterable(client, requests);
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            done();
        }

        @Override
        public void run() {
            if (promise.isDone()) {
                stopping(service.state());
            } else {
                new QueryCallback();
            }
        }
        
        @Override
        public void starting() {
            super.starting();
            promise.addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public void running() {
            run();
        }
        
        protected synchronized void done() {
            if (!promise.isDone()) {
                promise.set(value.get());
            }
        }
        
        protected class QueryCallback extends ToStringListenableFuture<List<O>> implements Runnable {

            public QueryCallback() {
                super(Futures.allAsList(query.call()));
                addListener(this, SameThreadExecutor.getInstance());
            }

            @Override
            public void run() {
                if (isDone()) {
                    List<O> responses;
                    try {
                        responses = get();
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    } catch (ExecutionException e) {
                        promise.setException(e);
                        return;
                    }
                    Optional<Operation.Error> error = null;
                    for (O response: responses) {
                        try {
                            error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                        } catch (KeeperException e) {
                            promise.setException(e);
                            return;
                        }
                    }
                    if (error.isPresent()) {
                        done();
                    }
                }
            }
        }
    }
    
    public static class PathToQueryWatcher<I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> extends AbstractWatchListener {

        public static <I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> PathToQueryWatcher<I,O> listen(
                Service service,
                WatchListeners watch,
                WatchMatcher matcher,
                PathToQuery<I,O> query) {
            PathToQueryWatcher<I,O> listener = create(service, watch, matcher, query);
            listener.listen();
            return listener;
        }

        public static <I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> PathToQueryWatcher<I,O> create(
                Service service,
                WatchListeners watch,
                WatchMatcher matcher,
                PathToQuery<I,O> query) {
            return new PathToQueryWatcher<I,O>(service, watch, matcher, query);
        }
        
        protected final PathToQuery<I,O> query;

        public PathToQueryWatcher(
                Service service,
                WatchListeners watch,
                WatchMatcher matcher,
                PathToQuery<I,O> query) {
            super(service, watch, matcher);
            this.query = query;
        }
        
        public PathToQuery<I,O> query() {
            return query;
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            if (service.isRunning()) {
                query.apply(event.getPath()).call();
            }
        }
    }
    
    /**
     * Should be robust to repeat events
     */
    public abstract static class CacheNodeCreatedListener<E extends Materializer.MaterializedNode<E,?>> extends AbstractWatchListener {

        protected final LockableZNodeCache<E,?,?> cache;
        
        protected CacheNodeCreatedListener(
                ZNodePath path,
                LockableZNodeCache<E,?,?> cache,
                Service service,
                WatchListeners watch) {
            this(cache, service, watch, 
                    WatchMatcher.exact(
                            path,
                            Watcher.Event.EventType.NodeCreated));
        }
        
        protected CacheNodeCreatedListener(
                LockableZNodeCache<E,?,?> cache,
                Service service,
                WatchListeners watch,
                WatchMatcher match) {
            super(service, watch, match);
            this.cache = cache;
        }

        @Override
        public void running() {
            cache.lock().readLock().lock();
            try {
                // replay creation events for existing nodes
                // TODO optimize
                for (E node: cache.cache()) {
                    if (!node.path().toString().matches(matcher.getPath().toString())) {
                        if (matcher.getPathType() != WatchMatcher.PathMatchType.PREFIX) {
                            continue;
                        }
                        boolean matches = true;
                        Iterator<ZNodeLabel> match = matcher.getPath().iterator();
                        Iterator<ZNodeLabel> labels = matcher.getPath().iterator();
                        while (match.hasNext() && labels.hasNext()) {
                            if (!labels.next().toString().matches(match.next().toString())) {
                                matches = false;
                                break;
                            }
                        }
                        if (!matches || match.hasNext()) {
                            continue;
                        }
                    }
                    if (getWatchMatcher().getEventType().contains(Watcher.Event.EventType.NodeCreated)) {
                        handleWatchEvent(NodeWatchEvent.nodeCreated(node.path()));
                    }
                    if (getWatchMatcher().getEventType().contains(Watcher.Event.EventType.NodeDataChanged)) {
                        if (node.data().stamp() > 0L) {
                            handleWatchEvent(NodeWatchEvent.nodeDataChanged(node.path()));
                        }
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
    }
    
    public static class ReplayEvent extends AbstractWatchListener {

        public static ReplayEvent forListener(AbstractWatchListener listener, WatchEvent toReplay, WatchMatcher matcher) {
            return create(listener, toReplay, listener.getService(), listener.getWatch(), matcher);
        }

        public static ReplayEvent create(AbstractWatchListener listener, WatchEvent toReplay, Service service, WatchListeners watch, WatchMatcher matcher) {
            return new ReplayEvent(listener, toReplay, service, watch, matcher);
        }
        
        protected final WatchMatchListener listener;
        protected final WatchEvent toReplay;
        
        protected ReplayEvent(
                WatchMatchListener listener,
                WatchEvent toReplay,
                Service service, 
                WatchListeners watch,
                WatchMatcher matcher) {
            super(service, watch, matcher);
            this.listener = listener;
            this.toReplay = toReplay;
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            listener.handleWatchEvent(toReplay);
            stopping(service.state());
        }
    }
    
    public static class StopOnDeleteListener extends AbstractWatchListener {

        public static StopOnDeleteListener listen(
                AbstractWatchListener listener,
                ZNodePath path) {
            StopOnDeleteListener instance = create(listener, path);
            instance.listen();
            return instance;
        }
        
        public static StopOnDeleteListener create(
                AbstractWatchListener listener,
                ZNodePath path) {
            return new StopOnDeleteListener(listener, path);
        }
        
        protected final AbstractWatchListener listener;
        
        protected StopOnDeleteListener(
                AbstractWatchListener listener,
                ZNodePath path) {
            super(listener.getService(), listener.getWatch(),
                    WatchMatcher.exact(path, 
                            Watcher.Event.EventType.NodeDeleted));
            this.listener = listener;
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            stopping(state());
        }
        
        @Override
        public void stopping(Service.State from) {
            super.stopping(from);
            listener.stopping(from);
        }
    }
    
    private Watchers() {}
}
