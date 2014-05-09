package edu.uw.zookeeper.safari.frontend;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.NotificationListener;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.SharedLookup;
import edu.uw.zookeeper.safari.frontend.ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;

public class ClientPeerConnectionExecutors {

    public static ClientPeerConnectionExecutors newInstance(
            final Session frontend,
            final boolean renew,
            final NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> notifications,
            final Supplier<Set<Identifier>> regions,
            final AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers) {
        return new ClientPeerConnectionExecutors(
                frontend, renew, notifications, regions, dispatchers,
                new MapMaker().<Identifier, ClientPeerConnectionExecutor>makeMap());
    }
    
    protected final Logger logger;
    protected final Supplier<Set<Identifier>> regions;
    protected final CachedLookup<Identifier, ClientPeerConnectionExecutor> cache;
    protected final Pair<Session,Boolean> frontend;
    protected final NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> notifications;

    protected ClientPeerConnectionExecutors(
            final Session frontend,
            final boolean renew,
            final NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> notifications,
            final Supplier<Set<Identifier>> regions,
            final AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            final ConcurrentMap<Identifier, ClientPeerConnectionExecutor> cache) {
        this.logger = LogManager.getLogger(getClass());
        this.frontend = Pair.create(frontend, renew);
        this.regions = regions;
        this.notifications = notifications;
        this.cache = CachedLookup.create(
                cache, 
                CachedFunction.<Identifier, ClientPeerConnectionExecutor>create(
                        new Function<Identifier, ClientPeerConnectionExecutor>() {
                            @Override
                            public ClientPeerConnectionExecutor apply(Identifier region) {
                                ClientPeerConnectionExecutor value = cache.get(region);
                                if ((value != null) && (value.state().compareTo(Actor.State.TERMINATED) < 0)) {
                                    return value;
                                } else {
                                    return null;
                                }
                            }
                       }, 
                       SharedLookup.create(
                               new AsyncFunction<Identifier, ClientPeerConnectionExecutor>() {
                                   @Override
                                   public ListenableFuture<ClientPeerConnectionExecutor> apply(Identifier region) throws Exception {
                                       ClientPeerConnectionExecutor prev = cache.get(region);
                                       ConnectMessage.Request backendRequest;
                                       if (prev != null) {
                                           backendRequest = ConnectMessage.Request.RenewRequest.newInstance(
                                                       prev.backend(), 0L);
                                       } else {
                                           if (renew) {
                                               backendRequest = ConnectMessage.Request.RenewRequest.newInstance(
                                                       Session.create(0L, frontend.parameters()), 0L);
                                           } else {
                                               backendRequest = ConnectMessage.Request.NewRequest.newInstance(
                                                       frontend.parameters().timeOut(), 0L);
                                           }
                                       }
                                       MessageSessionOpenRequest request = MessageSessionOpenRequest.of(frontend.id(), backendRequest);
                                       ListenableFuture<ClientPeerConnectionDispatcher> dispatcher = dispatchers.apply(region);
                                       ListenableFuture<ClientPeerConnectionExecutor> future = ClientPeerConnectionExecutor.connect(
                                               frontend, request, dispatcher);
                                       return Futures.transform(future, ClientPeerConnectionExecutors.this.new Callback(), SameThreadExecutor.getInstance());
                                   }
                               }),
                       logger));
    }
    
    public ConcurrentMap<Identifier, ClientPeerConnectionExecutor> asCache() {
        return cache.asCache();
    }

    public AsyncFunction<Identifier, ClientPeerConnectionExecutor> asLookup() {
        return cache.asLookup();
    }
    
    public ListenableFuture<Set<ClientPeerConnectionExecutor>> connect() {
        Connect task = new Connect();
        task.run();
        return task;
    }
    
    public void stop() {
        for (ClientPeerConnectionExecutor executor: Iterables.consumingIterable(asCache().values())) {
            executor.unsubscribe(notifications);
        }
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("frontend", Session.toString(frontend.first().id())).toString();
    }
    
    protected class Callback implements Function<ClientPeerConnectionExecutor, ClientPeerConnectionExecutor> {
        
        public Callback() {}
        
        @Override
        public ClientPeerConnectionExecutor apply(ClientPeerConnectionExecutor input) {
            logger.trace("Connected {} ({})", input, this);
            Identifier region = input.dispatcher().identifier();
            ClientPeerConnectionExecutor prev = asCache().put(region, input);
            if (prev != null) {
                assert (prev.state() == Actor.State.TERMINATED);
                prev.stop();
                prev.unsubscribe(notifications);
            }
            input.subscribe(notifications);
            return input;
        }
        
        @Override
        public String toString() {
            return ClientPeerConnectionExecutors.this.toString();
        }
    }
    
    protected class Connect extends ForwardingPromise<Set<ClientPeerConnectionExecutor>> implements Runnable {

        protected final Map<Identifier, ListenableFuture<ClientPeerConnectionExecutor>> futures;
        protected final Promise<Set<ClientPeerConnectionExecutor>> promise;
        
        public Connect() {
            this.promise = LoggingPromise.create(logger, SettableFuturePromise.<Set<ClientPeerConnectionExecutor>>create());
            this.futures = Maps.newHashMap();
        }
        
        @Override
        public synchronized void run() {
            Set<Identifier> difference = Sets.difference(regions.get(), futures.keySet()).immutableCopy();
            if (difference.isEmpty()) {
                ImmutableSet.Builder<ClientPeerConnectionExecutor> results = ImmutableSet.builder();
                Iterator<Map.Entry<Identifier, ListenableFuture<ClientPeerConnectionExecutor>>> entries = futures.entrySet().iterator();
                while (entries.hasNext()) {
                    Map.Entry<Identifier, ListenableFuture<ClientPeerConnectionExecutor>> entry = entries.next();
                    if (entry.getValue().isDone()) {
                        try {
                            results.add(entry.getValue().get());
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        } catch (ExecutionException e) {
                            // FIXME
                            throw new UnsupportedOperationException(e);
                        }
                    } else {
                        return;
                    }
                }
                set(results.build());
            } else {
                for (Identifier region: difference) {
                    ListenableFuture<ClientPeerConnectionExecutor> future;
                    try {
                        future = asLookup().apply(region);
                    } catch (Exception e) {
                        // FIXME
                        throw new UnsupportedOperationException(e);
                    }
                    futures.put(region, future);
                }
                for (Identifier region: difference) {
                    futures.get(region).addListener(this, SameThreadExecutor.getInstance());
                }
            }
        }

        @Override
        protected Promise<Set<ClientPeerConnectionExecutor>> delegate() {
            return promise;
        }
    }
}
