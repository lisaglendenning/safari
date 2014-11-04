package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;

import javax.annotation.Nullable;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.WatchMatchServiceListener;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.client.Watchers.FutureCallbackListener;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.ForwardingPromise.SimpleForwardingPromise;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.SchemaElementLookup;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.Request;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public abstract class RecreateSessionValues<T extends ClientExecutor<? super Records.Request,?,?> & Connection.Listener<? super Operation.Response>, V extends StorageZNode.EscapedNamedZNode<?>> implements FutureCallback<V> {

    protected static void listen(
            final ImmutableList<? extends WatchMatchServiceListener> listeners,
            final Promise<?> committed,
            final Service service) {
        ServiceListeners.listen(listeners, committed, service);
    }
    
    protected static Promise<ZNodePath> committed(
            RecreateSessionValues<?,?> instance,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            WatchListeners cacheEvents,
            Logger logger) {
        ZNodePath path = instance.getPath().parent();
        for (int i=0; i<3; ++i) {
            path = ((AbsoluteZNodePath) path).parent();
        }
        return SetOnCommit.listen(path, cache, cacheEvents, logger);
    }
    
    protected static <T extends ClientExecutor<? super Records.Request,?,?> & Connection.Listener<? super Operation.Response>, V extends StorageZNode.EscapedNamedZNode<?>> Watchers.CacheNodeCreatedListener<StorageZNode<?>> listener(
            RecreateSessionValues<?,V> instance,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        return Watchers.CacheNodeCreatedListener.create(
                cache, 
                service, 
                cacheEvents, 
                FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        instance, 
                                        cache.cache())),
                        WatchMatcher.exact(
                                instance.getPath(),
                                Watcher.Event.EventType.NodeDataChanged),
                            logger), 
                logger);
    }
    
    @SuppressWarnings("unchecked")
    protected static <O extends Operation.ProtocolResponse<?>, V extends StorageZNode.EscapedNamedZNode<?>> ImmutableList<Watchers.CacheNodeCreatedListener<StorageZNode<?>>> listeners(
            final RecreateSessionValues<?,V> instance,
            final Materializer<StorageZNode<?>,O> materializer,
            final WatchListeners cacheEvents,
            final Service service,
            final Logger logger) {
        final Predicate<Long> isLocal = new Predicate<Long>() {
            @Override
            public boolean apply(Long input) {
                return (instance.getExecutor(input) != null);
            }
        };
        final Watchers.CacheNodeCreatedListener<StorageZNode<?>> listener = listener(
                instance, materializer.cache(), cacheEvents, service, logger);
        final FutureCallback<Object> callback = Watchers.StopServiceOnFailure.create(service);
        ZNodeLabel label = null;
        for (ZNodeName k: materializer.schema().apply(instance.getType()).keySet()) {
            if (!k.equals(StorageZNode.CommitZNode.LABEL)) {
                assert (label == null);
                label = (ZNodeLabel) k;
            }
        }
        final FilteredCallback<StorageZNode<?>,V> getValue = FilteredCallback.getUncommittedLocalValue(
                isLocal, 
                label,
                materializer,
                callback);
        final Watchers.CacheNodeCreatedListener<StorageZNode<?>> getValueListener =
                FilteredCallback.listener(
                    WatchMatcher.exact(
                            instance.getPath(), 
                            Watcher.Event.EventType.NodeCreated), 
                    getValue, 
                    materializer.cache(), 
                    cacheEvents, 
                    service, 
                    logger);
        final Watchers.CacheNodeCreatedListener<StorageZNode<?>> getSessionListener =
            GetLocalSessionValues.listener(
                    ((AbsoluteZNodePath) instance.getPath().parent()).parent(), 
                    GetLocalSessionValues.getUncommittedAndLocal(isLocal, callback, materializer), 
                    materializer.cache(), 
                    cacheEvents, 
                    service, 
                    logger);
        final Watchers.CacheNodeCreatedListener<StorageZNode<?>> sessionChangeListener =
                SessionChangeListener.listener(
                        SessionChangeListener.getUpdated(
                                ((AbsoluteZNodePath) getSessionListener.getWatchMatcher().getPath()).parent(), 
                                materializer.cache().cache(), 
                                ReplaySessionValues.getUncommittedAndLocal(
                                        isLocal, 
                                        getValue, 
                                        materializer.cache().cache(), 
                                        materializer)), 
                        materializer.cache(), 
                        cacheEvents, 
                        service, 
                        logger);
        return ImmutableList.<Watchers.CacheNodeCreatedListener<StorageZNode<?>>>builder()
                .addAll(CommitSessionSnapshot.create(instance.getPath(), isLocal, materializer, service, cacheEvents, logger))
                .add(listener, getValueListener, getSessionListener, sessionChangeListener).build();
    }
    
    private final Class<V> type;
    private final AbsoluteZNodePath path;
    private final FutureCallback<?> callback;
    private final Function<? super Long, ? extends T> executors;
    
    protected RecreateSessionValues(
            ZNodePath snapshot,
            Class<V> type,
            SchemaElementLookup schema,
            FutureCallback<?> callback,
            Function<? super Long, ? extends T> executors) {
        super();
        this.type = type;
        this.path = (AbsoluteZNodePath) snapshot.join(schema.apply(type).path().suffix(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.PATH));
        this.callback = callback;
        this.executors = executors;
    }
    
    public Class<V> getType() {
        return type;
    }
    
    public AbsoluteZNodePath getPath() {
        return path;
    }
    
    public @Nullable T getExecutor(Long session) {
        return executors.apply(session);
    }

    @Override
    public void onFailure(Throwable t) {
        callback.onFailure(t);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(getPath()).toString();
    }
    
    protected static final class SetOnCommit<T extends StorageZNode.CommitZNode, V> extends SimpleForwardingPromise<V> implements FutureCallback<T> {

        public static <T extends StorageZNode.CommitZNode> Promise<ZNodePath> listen(
                ZNodePath path,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                WatchListeners cacheEvents,
                Logger logger) {
            Watchers.PathToNodeCallback<StorageZNode<?>, T, SetOnCommit<T,ZNodePath>> callback = 
                    Watchers.PathToNodeCallback.create(SetOnCommit.<T,ZNodePath>create(path), cache.cache());
            Watchers.FutureCallbackListener<?> listener = Watchers.FutureCallbackListener.create(
                    Watchers.EventToPathCallback.create(callback), 
                    WatchMatcher.exact(
                            path.join(StorageZNode.CommitZNode.LABEL), 
                            Watcher.Event.EventType.NodeCreated), 
                    logger);
            Watchers.UnsubscribeWhenDone.subscribe(callback.callback(), listener, cacheEvents);
            // replay
            cache.lock().readLock().lock();
            try {
                callback.onSuccess(listener.getWatchMatcher().getPath());
            } finally {
                cache.lock().readLock().unlock();
            }
            return callback.callback();
        }
        
        protected static <T extends StorageZNode.CommitZNode, V> SetOnCommit<T,V> create(
                V value) {
            return new SetOnCommit<T,V>(value, SettableFuturePromise.<V>create());
        }
        
        private final V value;
        
        protected SetOnCommit(
                V value,
                Promise<V> delegate) {
            super(delegate);
            this.value = value;
        }
        
        @Override
        public void onSuccess(T result) {
            if ((result != null) && !isDone()) {
                set(value);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
    
    protected static final class ServiceListeners extends Service.Listener implements Runnable {
        
        public static void listen(
                ImmutableList<? extends WatchMatchServiceListener> listeners, 
                Promise<?> promise,
                Service service) {
            Promise<Service.State> state = SettableFuturePromise.create();
            ServiceListeners instance = new ServiceListeners(state, listeners, promise);
            synchronized (instance) {
                service.addListener(instance, MoreExecutors.directExecutor());
                switch (service.state()) {
                case STARTING:
                    instance.starting();
                    break;
                case RUNNING:
                    instance.running();
                    break;
                case STOPPING:
                    instance.stopping(Service.State.STOPPING);
                    break;
                case TERMINATED:
                    instance.terminated(Service.State.TERMINATED);
                    break;
                case FAILED:
                    instance.failed(Service.State.FAILED, service.failureCause());
                    break;
                default:
                    throw new AssertionError();
                }
            }
            instance.run();
        }

        private final Promise<Service.State> state;
        private final ImmutableList<? extends WatchMatchServiceListener> listeners;
        private final Promise<?> promise;
        
        protected ServiceListeners(
                Promise<Service.State> state,
                ImmutableList<? extends WatchMatchServiceListener> listeners, 
                Promise<?> promise) {
            super();
            this.state = state;
            this.listeners = listeners;
            this.promise = promise;
        }
        
        @Override
        public synchronized void starting() {
            if (this.state.set(Service.State.STARTING)) {
                for (WatchMatchServiceListener listener: listeners) {
                    listener.starting();
                    listener.running();
                }
            }
        }
        
        @Override
        public synchronized void running() {
            if (this.state.set(Service.State.RUNNING)) {
                for (WatchMatchServiceListener listener: listeners) {
                    listener.starting();
                    listener.running();
                }
            }
        }
        
        @Override
        public synchronized void stopping(Service.State from) {
            this.state.set(Service.State.STOPPING);
            promise.cancel(false);
        }
        
        @Override
        public synchronized void terminated(Service.State state) {
            this.state.set(Service.State.TERMINATED);
            promise.cancel(false);
        }

        @Override
        public synchronized void failed(Service.State state, Throwable failure) {
            this.state.set(Service.State.FAILED);
            promise.setException(failure);
        }
        
        @Override
        public void run() {
            if (promise.isDone()) {
                if (!state.isDone()) {
                    state.cancel(false);
                }
                Service.State state = this.state.isCancelled() ? Service.State.NEW : Futures.getUnchecked(this.state);
                Optional<? extends Exception> exception = Optional.absent();
                if (promise.isCancelled()) {
                    exception = Optional.of(new CancellationException());
                } else {
                    try {
                        promise.get();
                    } catch (Exception e) {
                        exception = Optional.of(e);
                    }
                }
                for (WatchMatchServiceListener listener: listeners.reverse()) {
                    if (exception.isPresent() && !(exception.get() instanceof CancellationException)) {
                        listener.failed(state, exception.get());
                    } else {
                        switch (state) {
                        case STARTING:
                        case RUNNING:
                            listener.stopping(state);
                            listener.terminated(state);
                            break;
                        default:
                            break;
                        }
                    }
                }
            } else {
                promise.addListener(this, MoreExecutors.directExecutor());
            }
        }
    }

    protected static final class FilteredCallback<U extends NameTrie.Node<U>, T extends U> implements FutureCallback<T> {

        public static <T extends StorageZNode<?>> Watchers.CacheNodeCreatedListener<StorageZNode<?>> listener(
                WatchMatcher matcher,
                FilteredCallback<StorageZNode<?>,T> callback,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.create(
                    cache, service, cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                        callback, 
                                        cache.cache())),
                            matcher, 
                            logger), logger);
        }
        
        public static <T extends StorageZNode.EscapedNamedZNode<?>, O extends Operation.ProtocolResponse<?>> FilteredCallback<StorageZNode<?>,T> getUncommittedLocalValue(
                final Predicate<? super Long> isLocal,
                final ZNodeLabel label,
                final ClientExecutor<? super Records.Request,O,?> client,
                final FutureCallback<?> callback) {
            return create(
                    new Predicate<T>() {
                        @Override
                        public boolean apply(T input) {
                            if (input.containsKey(StorageZNode.CommitZNode.LABEL)) {
                                return false;
                            }
                            StorageZNode.SessionZNode<?> session = (StorageZNode.SessionZNode<?>) input.parent().get().parent().get();
                            return !session.containsKey(StorageZNode.CommitZNode.LABEL) && isLocal.apply(Long.valueOf(session.name().longValue()));
                        }
                    },
                    label,
                    client,
                    callback);
        }
        
        public static <U extends Materializer.MaterializedNode<U,?>, T extends U, O extends Operation.ProtocolResponse<?>> FilteredCallback<U,T> create(
                final Predicate<? super T> filter,
                final ZNodeLabel label,
                final ClientExecutor<? super Records.Request,O,?> client,
                final FutureCallback<?> callback) {
            return create(
                    new Predicate<T>() {
                        @Override
                        public boolean apply(T input) {
                            return (input.data().stamp() < 0L) && filter.apply(input);
                        }
                    },
                    GetCommitThenData.create(label, callback, client));
        }

        protected static <U extends NameTrie.Node<U>, T extends U> FilteredCallback<U,T> create(
                Predicate<? super T> filter,
                FutureCallback<ZNodePath> delegate) {
            return new FilteredCallback<U,T>(
                    filter, 
                    delegate);
        }
        
        private final Predicate<? super T> filter;
        private final FutureCallback<ZNodePath> delegate;
        
        protected FilteredCallback(
                Predicate<? super T> filter,
                FutureCallback<ZNodePath> delegate) {
            this.filter = filter;
            this.delegate = delegate;
        }
    
        @Override
        public void onSuccess(final T result) {
            if (filter.apply(result)) {
                delegate.onSuccess(result.path());
            }
        }

        @Override
        public void onFailure(Throwable t) {
            delegate.onFailure(t);
        }

        /**
         * Not threadsafe.
         * Ignores duplicate requests
         */
        protected static final class GetCommitThenData<O extends Operation.ProtocolResponse<?>> implements FutureCallback<ZNodePath> {

            public static <O extends Operation.ProtocolResponse<?>> GetCommitThenData<O> create(
                    ZNodeLabel label,
                    FutureCallback<?> callback,
                    ClientExecutor<? super Records.Request, O, ?> client) {
                return new GetCommitThenData<O>(label, callback, client);
            }
            
            private final FutureCallback<?> callback;
            private final Set<ZNodePath> pending;
            private final PathToQuery<?,O> getCommit;
            private final PathToQuery<?,O> getValue;
            
            @SuppressWarnings("unchecked")
            protected GetCommitThenData(
                    final ZNodeLabel label,
                    final FutureCallback<?> callback,
                    final ClientExecutor<? super Records.Request, O, ?> client) {
                this.callback = callback;
                final PathToRequests getData = PathToRequests.forRequests(
                        Operations.Requests.sync(), 
                        Operations.Requests.getData());
                this.getValue = PathToQuery.forFunction(
                        client, 
                        new Function<ZNodePath, List<Records.Request>>() {
                            @Override
                            public List<Records.Request> apply(ZNodePath input) {
                                return ImmutableList.<Records.Request>builder().addAll(getData.apply(input.join(label))).addAll(getData.apply(input)).build();
                            }
                        });
                this.getCommit = PathToQuery.forFunction(
                        client,
                        Functions.compose(
                                getData, 
                                JoinToPath.forName(StorageZNode.CommitZNode.LABEL)));
                this.pending = Sets.newHashSet();
            }
            
            @Override
            public synchronized void onSuccess(ZNodePath result) {
                if (pending.add(result)) {
                    new GetCommit(result);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                callback.onFailure(t);
            }

            private abstract class GetData extends SimpleToStringListenableFuture<List<O>> implements Runnable {
                protected final ZNodePath path;
                
                private GetData(
                        ZNodePath path,
                        PathToQuery<?,O> query) {
                    super(Futures.allAsList(query.apply(path).call()));
                    this.path = path;
                    addListener(this, MoreExecutors.directExecutor());
                }
            }
            
            private final class GetCommit extends GetData {
                
                private GetCommit(ZNodePath path) {
                    super(path, getCommit);
                }

                @Override
                public void run() {
                    if (isDone()) {
                        Optional<Operation.Error> error = null;
                        try {
                            for (O response: get()) {
                                error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                            }
                            if (error.isPresent()) {
                                new GetValue(path);
                                return;
                            }
                        } catch (Exception e) {
                            onFailure(e);
                        }
                        synchronized (GetCommitThenData.this) {
                            pending.remove(path);
                        }
                    }
                }
            }
            
            private final class GetValue extends GetData {
                
                private GetValue(ZNodePath path) {
                    super(path, getValue);
                }

                @Override
                public void run() {
                    if (isDone()) {
                        try {
                            for (O response: get()) {
                                Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                            }
                        } catch (Exception e) {
                            onFailure(e);
                        } finally {
                            synchronized (GetCommitThenData.this) {
                                pending.remove(path);
                            }
                        }
                    }
                }
            }
        }
    }

    protected static abstract class GetSessionValues<O extends Operation.ProtocolResponse<?>, T extends StorageZNode.SessionZNode<?>, V> implements FutureCallback<T> {
        
        protected static <T extends StorageZNode.SessionZNode<?>> Predicate<T> isUncommittedAndLocal(
                final Predicate<? super Long> isLocal) {
            return new Predicate<T>() {
                @Override
                public boolean apply(T input) {
                    return !input.containsKey(StorageZNode.CommitZNode.LABEL) && isLocal.apply(Long.valueOf(input.name().longValue()));
                }
            };
        }
        
        protected final Predicate<? super T> filter;
        protected final Watchers.MaybeErrorProcessor processor;
        protected final PathToQuery<?,O> query;
        protected final FutureCallback<? super V> callback;
        
        protected GetSessionValues(
                Predicate<? super T> filter,
                FutureCallback<? super V> callback,
                PathToQuery<?,O> query) {
            this.callback = callback;
            this.filter = filter;
            this.processor = Watchers.MaybeErrorProcessor.maybeNoNode();
            this.query = query;
        }

        @Override
        public void onFailure(final Throwable t) {
            callback.onFailure(t);
        }
        
        protected static final class Query implements Function<ZNodePath, List<Records.Request>> {
            private final PathToRequests getChildren;
            
            @SuppressWarnings("unchecked")
            protected Query() {
                this.getChildren = PathToRequests.forRequests(
                        Operations.Requests.sync(), 
                        Operations.Requests.getChildren());
            }

            @Override
            public List<Request> apply(ZNodePath input) {
                return ImmutableList.<Records.Request>builder().addAll(getChildren.apply(input)).addAll(getChildren.apply(input.join(StorageZNode.ValuesZNode.LABEL))).build();
            }
        }
    }
    
    protected static final class GetLocalSessionValues<O extends Operation.ProtocolResponse<?>, T extends StorageZNode.SessionZNode<?>> extends GetSessionValues<O,T,Optional<Operation.Error>> {

        public static <O extends Operation.ProtocolResponse<?>, T extends StorageZNode.SessionZNode<?>> GetLocalSessionValues<O,T> getUncommittedAndLocal(
                Predicate<? super Long> isLocal,
                FutureCallback<? super Optional<Operation.Error>> callback,
                ClientExecutor<? super Records.Request, O, ?> client) {
            return new GetLocalSessionValues<O,T>(
                    isUncommittedAndLocal(isLocal), 
                    callback,
                    client);
        }

        public static <T extends StorageZNode.SessionZNode<?>> Watchers.CacheNodeCreatedListener<StorageZNode<?>> listener(
                ZNodePath path,
                GetLocalSessionValues<?,T> callback,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.create(
                    cache, service, cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                        callback, 
                                        cache.cache())),
                            WatchMatcher.exact(
                                    path, 
                                    Watcher.Event.EventType.NodeCreated), 
                            logger), logger);
        }
        
        protected GetLocalSessionValues(
                Predicate<? super T> filter,
                FutureCallback<? super Optional<Operation.Error>> callback,
                ClientExecutor<? super Records.Request, O, ?> client) {
            super(filter, callback, PathToQuery.forFunction(client, new Query()));
        }
        
        @Override
        public void onSuccess(final T result) {
            if (filter.apply(result)) {
                Watchers.Query.call(
                        processor, 
                        callback, 
                        query.apply(result.path()));
            }
        }
    }

    protected static final class ReplaySessionValues<O extends Operation.ProtocolResponse<?>, T extends StorageZNode.SessionZNode<?>, U extends StorageZNode.EscapedNamedZNode<?>> extends GetSessionValues<O,T,U> {

        public static <O extends Operation.ProtocolResponse<?>, T extends StorageZNode.SessionZNode<?>, U extends StorageZNode.EscapedNamedZNode<?>> ReplaySessionValues<O,T,U> getUncommittedAndLocal(
                Predicate<? super Long> isLocal,
                FutureCallback<U> callback,
                NameTrie<StorageZNode<?>> trie,
                ClientExecutor<? super Records.Request, O, ?> client) {
            return new ReplaySessionValues<O,T,U>(
                    trie, 
                    isUncommittedAndLocal(isLocal), 
                    callback, 
                    client);
        }
        
        private final NameTrie<StorageZNode<?>> trie;
        
        protected ReplaySessionValues(
                NameTrie<StorageZNode<?>> trie,
                Predicate<? super T> filter,
                FutureCallback<U> callback,
                ClientExecutor<? super Records.Request, O, ?> client) {
            super(filter, callback, 
                    PathToQuery.forFunction(
                        client, 
                        new Query()));
            this.trie = trie;
        }
        
        @Override
        public void onSuccess(final T result) {
            if (filter.apply(result)) {
                final StorageZNode<?> values = ((StorageZNode<?>) result).get(StorageZNode.ValuesZNode.LABEL);
                final ZNodePath path;
                final ImmutableSet<ZNodeName> keys;
                if (values != null) {
                    path = values.path();
                    keys = ImmutableSet.copyOf(values.keySet());
                } else {
                    path = result.path().join(StorageZNode.ValuesZNode.LABEL);
                    keys = ImmutableSet.of();
                }
                Watchers.Query.call(
                        Processors.bridge(
                                processor, 
                                IfPresent.create(ImmutableSet.<ZNodeName>of(), keys)), 
                        new ReplayExistingValues(path), 
                        query.apply(result.path()));
            }
        }

        @Override
        public void onFailure(final Throwable t) {
            callback.onFailure(t);
        }
        
        private static final class IfPresent<V> extends AbstractPair<V,V> implements Processor<Optional<?>,V> {

            public static <V> IfPresent<V> create(
                    V first,
                    V second) {
                return new IfPresent<V>(first, second);
            }
            
            private IfPresent(
                    V first,
                    V second) {
                super(first, second);
            }

            @Override
            public V apply(Optional<?> input) throws Exception {
                return input.isPresent() ? first : second;
            }
        }
        
        // Only replay children that were already cached before the getChildren query
        // Note that this won't work if this query is concurrent with another getChildren query,
        // which may cause duplicate callbacks
        private final class ReplayExistingValues implements FutureCallback<Set<ZNodeName>> {

            private final ZNodePath path;
            
            private ReplayExistingValues(
                    ZNodePath path) {
                this.path = path;
            }
            
            @Override
            public void onSuccess(Set<ZNodeName> result) {
                StorageZNode<?> values = trie.get(path);
                if (values != null) {
                    for (ZNodeName label: result) {
                        @SuppressWarnings("unchecked")
                        U value = (U) values.get(label);
                        if (value != null) {
                            callback.onSuccess(value);
                        }
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                ReplaySessionValues.this.onFailure(t);
            }
        }
    }
    
    protected static final class SessionChangeListener<T extends StorageZNode<?>> implements FutureCallback<StorageSchema.Safari.Sessions.Session> {

        public static <T extends StorageZNode<?>> SessionChangeListener<T> getUpdated(
                final ZNodePath sessions,
                final NameTrie<StorageZNode<?>> trie,
                final FutureCallback<? super T> callback) {
            return create(
                    sessions,
                    trie,
                    new Predicate<StorageSchema.Safari.Sessions.Session>() {
                        @Override
                        public boolean apply(StorageSchema.Safari.Sessions.Session input) {
                            return (input.stat().stamp() > 0L) && (input.stat().get().getVersion() > 1);
                        }
                    },
                    callback);
        }


        public static Watchers.CacheNodeCreatedListener<StorageZNode<?>> listener(
                SessionChangeListener<?> callback,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.create(
                    cache, 
                    service, 
                    cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                        callback, 
                                        cache.cache())),
                            WatchMatcher.exact(
                                    StorageSchema.Safari.Sessions.Session.PATH, 
                                    Watcher.Event.EventType.NodeDataChanged), 
                            logger), 
                    logger);
        }
        
        public static <T extends StorageZNode<?>> SessionChangeListener<T> create(
                final ZNodePath sessions,
                final NameTrie<StorageZNode<?>> trie,
                final Predicate<? super StorageSchema.Safari.Sessions.Session> filter,
                FutureCallback<? super T> callback) {
            return new SessionChangeListener<T>(sessions, trie, filter, callback);
        }
        
        private final ZNodePath sessions;
        private final NameTrie<StorageZNode<?>> trie;
        private final Predicate<? super StorageSchema.Safari.Sessions.Session> filter;
        private final FutureCallback<? super T> callback;
        
        protected SessionChangeListener(
                ZNodePath sessions,
                NameTrie<StorageZNode<?>> trie,
                Predicate<? super StorageSchema.Safari.Sessions.Session> filter,
                FutureCallback<? super T> callback) {
            this.filter = filter;
            this.callback = callback;
            this.sessions = sessions;
            this.trie = trie;
        }

        @Override
        public void onSuccess(StorageSchema.Safari.Sessions.Session result) {
            if (filter.apply(result)) {
                @SuppressWarnings("unchecked")
                T session = (T) trie.get(sessions).get(result.parent().name());
                if (session != null) {
                    callback.onSuccess(session);
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
}
