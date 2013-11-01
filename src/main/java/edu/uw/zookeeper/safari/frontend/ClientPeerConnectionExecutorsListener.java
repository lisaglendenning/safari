package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.NotificationListener;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.SharedLookup;
import edu.uw.zookeeper.safari.frontend.ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public class ClientPeerConnectionExecutorsListener implements Function<ClientPeerConnectionExecutor, ClientPeerConnectionExecutor>, NotificationListener<ShardedResponseMessage<IWatcherEvent>> {

    public static ClientPeerConnectionExecutorsListener newInstance(
            FrontendSessionExecutor frontend,
            AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            Executor executor) {
        return new ClientPeerConnectionExecutorsListener(
                frontend, dispatchers, executor,
                new MapMaker().<Identifier, ClientPeerConnectionExecutor>makeMap());
    }
    
    protected final CachedLookup<Identifier, ClientPeerConnectionExecutor> cache;
    protected final FrontendSessionExecutor frontend;

    protected ClientPeerConnectionExecutorsListener(
            final FrontendSessionExecutor frontend,
            final AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            final Executor executor,
            final ConcurrentMap<Identifier, ClientPeerConnectionExecutor> cache) {
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
                                       return Futures.transform(future, ClientPeerConnectionExecutorsListener.this, FrontendSessionExecutor.SAME_THREAD_EXECUTOR);
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
        Identifier region = input.dispatcher().getIdentifier();
        ClientPeerConnectionExecutor prev = asCache().put(region, input);
        if (prev != null) {
            assert (prev.state() == Actor.State.TERMINATED);
            prev.stop();
            prev.unsubscribe(this);
        }
        input.subscribe(this);
        return input;
    }
    
    @Override
    public void handleNotification(ShardedResponseMessage<IWatcherEvent> result) {
        frontend.onSuccess(result);
    }
    
    public void stop() {
        for (ClientPeerConnectionExecutor executor: Iterables.consumingIterable(asCache().values())) {
            executor.unsubscribe(this);
        }
    }
}