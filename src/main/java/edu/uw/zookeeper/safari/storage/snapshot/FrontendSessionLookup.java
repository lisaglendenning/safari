package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Supplier;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class FrontendSessionLookup extends Watchers.StopServiceOnFailure<WatchEvent,Service> implements AsyncFunction<Long, Long> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}
        
        @Provides @Singleton
        public FrontendSessionLookup getFrontendSessionLookup(
                SchemaClientService<StorageZNode<?>,?> client,
                SnapshotListener service) {
            return FrontendSessionLookup.listen(client, service);
        }

        @Override
        public Key<?> getKey() {
            return Key.get(FrontendSessionLookup.class);
        }

        @Override
        protected void configure() {
            bind(new TypeLiteral<AsyncFunction<Long, Long>>(){}).annotatedWith(Snapshot.class).to(FrontendSessionLookup.class);
        }
    }

    public static <O extends Operation.ProtocolResponse<?>> FrontendSessionLookup listen(
            SchemaClientService<StorageZNode<?>,O> client,
            Service service) {
        final Set<Object> pending = Collections.synchronizedSet(
                Sets.<Object>newHashSet());
        final FrontendSessionLookup instance = FrontendSessionLookup.create(
                IsEmpty.create(pending),
                service);
        final Processor<List<O>,WatchEvent> processor = Processors.bridge(
                Watchers.MaybeErrorProcessor.maybeNoNode(),
                new Processor<Object, WatchEvent>() {
                    final WatchEvent event = NodeWatchEvent.nodeChildrenChanged(
                            StorageSchema.Safari.Sessions.PATH);
                    @Override
                    public WatchEvent apply(Object input) throws Exception {
                        return event;
                    }
                });
        Watchers.FutureCallbackServiceListener.listen(
                Watchers.EventToPathCallback.create(
                        SessionCallbacks.create(
                                client.materializer(), 
                                instance, 
                                processor, 
                                pending)),
                service, 
                client.notifications(), 
                WatchMatcher.exact(
                        StorageSchema.Safari.Sessions.Session.PATH, 
                        Watcher.Event.EventType.NodeDataChanged), 
                instance.logger());
        Watchers.CacheNodeCreatedListener.listen(
                client.materializer().cache(), 
                service, 
                client.cacheEvents(),
                Watchers.FutureCallbackListener.create(
                        instance.new SessionCallback(
                                Watchers.EventToPathCallback.create(
                                    SessionCallbacks.create(
                                            client.materializer(), 
                                            instance, 
                                            processor, 
                                            pending)),
                                client.materializer().cache().cache()), 
                        WatchMatcher.exact(
                                StorageSchema.Safari.Sessions.Session.PATH,
                                Watcher.Event.EventType.NodeCreated,  
                                Watcher.Event.EventType.NodeDataChanged, 
                                Watcher.Event.EventType.NodeDeleted), 
                        instance.logger()),
                instance.logger());
        Watchers.RunnableServiceListener.listen(
                SessionsCallbacks.create(
                        client.materializer(), 
                        instance, 
                        processor,
                        pending), 
                service,
                client.notifications(),
                WatchMatcher.exact(
                        StorageSchema.Safari.Sessions.PATH, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeChildrenChanged));
        return instance;
    }
    
    protected static FrontendSessionLookup create(
            Supplier<Boolean> isReady,
            Service service) {
        return new FrontendSessionLookup(isReady, service);
    }

    private final Supplier<Boolean> isReady;
    private final BiMap<Long, Long> values;
    private final ConcurrentMap<Long, Promise<Long>> lookups;
    
    protected FrontendSessionLookup(
            Supplier<Boolean> isReady,
            Service service) {
        super(service);
        this.isReady = isReady;
        this.values = HashBiMap.create();
        this.lookups = new MapMaker().makeMap();
    }
    
    @Override
    public synchronized ListenableFuture<Long> apply(Long input) throws Exception {
        if (isReady.get().booleanValue()) {
            return Futures.immediateFuture(values.get(input));
        } else {
            Promise<Long> promise = lookups.get(input);
            if (promise == null) {
                promise = SettableFuturePromise.create();
                if (lookups.putIfAbsent(input, promise) != null) {
                    return apply(input);
                }
            }
            return promise;
        }
    }

    @Override
    public synchronized void onSuccess(WatchEvent result) {
        if (isReady.get().booleanValue()) {
            for (Map.Entry<Long, Promise<Long>> entry: Iterables.consumingIterable(lookups.entrySet())) {
                entry.getValue().set(values.get(entry.getKey()));
            }
        }
    }
    
    @Override
    public synchronized void onFailure(Throwable failure) {
        for (Map.Entry<Long, Promise<Long>> entry: Iterables.consumingIterable(lookups.entrySet())) {
            entry.getValue().setException(failure);
        }
        super.onFailure(failure);
    }
    
    protected final class SessionCallback implements FutureCallback<WatchEvent> {

        private final NameTrie<StorageZNode<?>> trie;
        private final FutureCallback<WatchEvent> callback;
        
        public SessionCallback(
                FutureCallback<WatchEvent> callback,
                NameTrie<StorageZNode<?>> trie) {
            this.callback = callback;
            this.trie = trie;
        }

        @Override
        public void onSuccess(WatchEvent result) {
            synchronized (FrontendSessionLookup.this) {
                switch (result.getEventType()) {
                case NodeCreated:
                {
                    callback.onSuccess(result);
                    break;
                }
                case NodeDataChanged:
                {
                    StorageSchema.Safari.Sessions.Session node = (StorageSchema.Safari.Sessions.Session) trie.get(result.getPath());
                    Long backend = Long.valueOf(node.data().get().getSessionId().longValue());
                    if (!values.containsKey(backend)) {
                        values.put(backend, Long.valueOf(node.name().longValue()));
                    }
                    break;
                }
                case NodeDeleted:
                {
                    Long frontend = Long.valueOf(StorageZNode.SessionZNode.SessionIdHex.valueOf(result.getPath().label().toString()).longValue());
                    values.inverse().remove(frontend);
                    break;
                }
                default:
                    break;
                }
                
                FrontendSessionLookup.this.onSuccess(result);
            }
        }
        
        @Override
        public void onFailure(Throwable failure) {
            FrontendSessionLookup.this.onFailure(failure);
        }
    }
    
    protected static final class IsEmpty implements Supplier<Boolean> {

        public static IsEmpty create(
                Collection<?> delegate) {
            return new IsEmpty(delegate);
        }
        
        private final Collection<?> delegate;
        
        protected IsEmpty(
                Collection<?> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public Boolean get() {
            return Boolean.valueOf(delegate.isEmpty());
        }
    }

    protected static abstract class AbstractCallbacks<O extends Operation.ProtocolResponse<?>, T> {

        protected final Processor<? super List<O>, ? extends T> processor;
        protected final FutureCallback<? super T> callback;
        protected final Set<? super Callback> pending;
        
        protected AbstractCallbacks(
                FutureCallback<? super T> callback,
                Processor<? super List<O>, ? extends T> processor,
                Set<? super Callback> pending) {
            this.callback = callback;
            this.processor = processor;
            this.pending = pending;
        }
        
        protected final class Callback implements FutureCallback<T> {

            protected Callback() {
                pending.add(this);
            }
            
            @Override
            public void onSuccess(T result) {
                pending.remove(this);
                AbstractCallbacks.this.callback.onSuccess(result);
            }

            @Override
            public void onFailure(Throwable t) {
                pending.remove(this);
                AbstractCallbacks.this.callback.onFailure(t);
            }
        }
    }
    
    protected static final class SessionCallbacks<O extends Operation.ProtocolResponse<?>, T> extends AbstractCallbacks<O,T> implements FutureCallback<ZNodePath> {
        
        @SuppressWarnings("unchecked")
        public static <O extends Operation.ProtocolResponse<?>, T> SessionCallbacks<O,T> create(
                ClientExecutor<? super Records.Request,O,?> client,
                FutureCallback<? super T> callback,
                Processor<? super List<O>, ? extends T> processor,
                Set<? super AbstractCallbacks<O,T>.Callback> pending) {
            return create(
                    PathToQuery.forRequests(
                        client, 
                        Operations.Requests.sync(), 
                        Operations.Requests.getData().setWatch(true)),
                    callback, 
                    processor, 
                    pending);
        }
        
        protected static <O extends Operation.ProtocolResponse<?>, T> SessionCallbacks<O,T> create(
                PathToQuery<?,O> query,
                FutureCallback<? super T> callback,
                Processor<? super List<O>, ? extends T> processor,
                Set<? super SessionsCallbacks<O,T>.Callback> pending) {
            return new SessionCallbacks<O,T>(query, callback, processor, pending);
        }
        
        private final PathToQuery<?,O> query;
        
        protected SessionCallbacks(
                PathToQuery<?,O> query,
                FutureCallback<? super T> callback,
                Processor<? super List<O>, ? extends T> processor,
                Set<? super Callback> pending) {
            super(callback, processor, pending);
            this.query = query;
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            Watchers.Query.call(processor, new Callback(), query.apply(result)).run();
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
    
    protected static final class SessionsCallbacks<O extends Operation.ProtocolResponse<?>, T> extends AbstractCallbacks<O,T> implements Runnable {
        
        @SuppressWarnings("unchecked")
        public static <O extends Operation.ProtocolResponse<?>, T> SessionsCallbacks<O,T> create(
                ClientExecutor<? super Records.Request,O,?> client,
                FutureCallback<? super T> callback,
                Processor<? super List<O>, ? extends T> processor,
                Set<? super AbstractCallbacks<O,T>.Callback> pending) {
            return create(
                    FixedQuery.forIterable(
                        client, 
                        PathToRequests.forRequests(
                                Operations.Requests.sync(), 
                                Operations.Requests.getChildren().setWatch(true))
                                .apply(StorageSchema.Safari.Sessions.PATH)),
                    callback, 
                    processor, 
                    pending);
        }
        
        protected static <O extends Operation.ProtocolResponse<?>, T> SessionsCallbacks<O,T> create(
                FixedQuery<O> query,
                FutureCallback<? super T> callback,
                Processor<? super List<O>, ? extends T> processor,
                Set<? super SessionsCallbacks<O,T>.Callback> pending) {
            return new SessionsCallbacks<O,T>(query, callback, processor, pending);
        }
        
        private final FixedQuery<O> query;
        
        protected SessionsCallbacks(
                FixedQuery<O> query,
                FutureCallback<? super T> callback,
                Processor<? super List<O>, ? extends T> processor,
                Set<? super Callback> pending) {
            super(callback, processor, pending);
            this.query = query;
        }
        
        @Override
        public void run() {
            Watchers.Query.call(processor, new Callback(), query).run();
        }
    }
}
