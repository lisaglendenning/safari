package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.SharedLookup;
import edu.uw.zookeeper.safari.frontend.ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;

public class ClientPeerConnectionExecutorsListener implements Function<ClientPeerConnectionExecutor, ClientPeerConnectionExecutor> {

    public static ClientPeerConnectionExecutorsListener newInstance(
            FrontendSessionExecutor frontend,
            AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            Executor executor) {
        return new ClientPeerConnectionExecutorsListener(
                frontend, dispatchers, executor,
                new MapMaker().<Identifier, ClientPeerConnectionExecutor>makeMap());
    }
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
    protected final CachedLookup<Identifier, ClientPeerConnectionExecutor> cache;
    protected final FrontendSessionExecutor frontend;

    protected ClientPeerConnectionExecutorsListener(
            final FrontendSessionExecutor frontend,
            final AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            final Executor executor,
            final ConcurrentMap<Identifier, ClientPeerConnectionExecutor> cache) {
        this.logger = LogManager.getLogger(getClass());
        this.frontend = frontend;
        this.cache = CachedLookup.create(
                cache, 
                CachedFunction.<Identifier, ClientPeerConnectionExecutor>create(
                        new Function<Identifier, ClientPeerConnectionExecutor>() {
                            @Override
                            public ClientPeerConnectionExecutor apply(Identifier ensemble) {
                                ClientPeerConnectionExecutor value = cache.get(ensemble);
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
                                           backendRequest = ConnectMessage.Request.NewRequest.newInstance(
                                                   frontend.session().parameters().timeOut(), 0L);
                                       }
                                       MessageSessionOpenRequest request = MessageSessionOpenRequest.of(frontend.session().id(), backendRequest);
                                       ListenableFuture<ClientPeerConnectionDispatcher> dispatcher = dispatchers.apply(region);
                                       ListenableFuture<ClientPeerConnectionExecutor> future = ClientPeerConnectionExecutor.connect(
                                               frontend.session(), request, executor, dispatcher);
                                       return Futures.transform(future, ClientPeerConnectionExecutorsListener.this, SAME_THREAD_EXECUTOR);
                                   }
                               })));
    }
    
    public ConcurrentMap<Identifier, ClientPeerConnectionExecutor> asCache() {
        return cache.asCache();
    }

    public AsyncFunction<Identifier, ClientPeerConnectionExecutor> asLookup() {
        return cache.asLookup();
    }
    
    @Override
    public ClientPeerConnectionExecutor apply(ClientPeerConnectionExecutor input) {
        logger.trace("{} ({})", input, this);
        Identifier region = input.dispatcher().getIdentifier();
        ClientPeerConnectionExecutor prev = asCache().put(region, input);
        if (prev != null) {
            assert (prev.state() == Actor.State.TERMINATED);
            prev.stop();
            prev.unsubscribe(frontend.notifications());
        }
        input.subscribe(frontend.notifications());
        return input;
    }
    
    public void stop() {
        for (ClientPeerConnectionExecutor executor: Iterables.consumingIterable(asCache().values())) {
            executor.unsubscribe(frontend.notifications());
        }
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("frontend", frontend).toString();
    }
}
