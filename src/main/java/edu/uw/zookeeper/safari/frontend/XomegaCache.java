package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageType;
import edu.uw.zookeeper.safari.peer.protocol.MessageXomega;

/**
 * Assumes we only need to remember one preceding xomega per volume,
 * and that updates are received in version order per volume.
 */
public final class XomegaCache extends LoggingServiceListener<ClientPeerConnections> implements AsyncFunction<VersionedId, Long>, ConnectionFactory.ConnectionsListener<ClientPeerConnection<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        public Module() {}

        @Provides @Singleton
        public XomegaCache newXomegaCache(
                final ClientPeerConnections connections) {
            return XomegaCache.listen(connections);
        }
        
        @Override
        protected void configure() {
            bind(Key.get(new TypeLiteral<AsyncFunction<VersionedId, Long>>(){}, Frontend.class)).to(XomegaCache.class);
        }

        @Override
        public Key<?> getKey() {
            return Key.get(XomegaCache.class);
        }
    }
    
    public static XomegaCache listen(
            final ClientPeerConnections connections) {
        final ConcurrentMap<Identifier, Pair<UnsignedLong, ?>> cache = new MapMaker().makeMap();
        XomegaCache instance = new XomegaCache(cache, connections);
        return Services.listen(instance, connections);
    }
    
    private final ConcurrentMap<Identifier, Pair<UnsignedLong, ?>> cache;
    private final ConcurrentMap<ClientPeerConnection<?>, PeerListener> listeners;
    
    protected XomegaCache(
            ConcurrentMap<Identifier, Pair<UnsignedLong, ?>> cache,
            ClientPeerConnections connections) {
        super(connections);
        this.cache = cache;
        this.listeners = new MapMaker().makeMap();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ListenableFuture<Long> apply(VersionedId input) throws Exception {
        Pair<UnsignedLong, ?> value = cache.get(input.getValue());
        if (value == null) {
            value = Pair.create(input.getVersion(), SettableFuturePromise.<Long>create());
            if (cache.putIfAbsent(input.getValue(), value) != null) {
                // try again
                return apply(input);
            }
        } else {
            int cmp = value.first().compareTo(input.getVersion());
            if (cmp < 0) {
                assert (value.second() instanceof Long);
                Pair<UnsignedLong, ?> updated = Pair.create(input.getVersion(), SettableFuturePromise.<Long>create());
                if (cache.replace(input.getValue(), value, updated)) {
                    value = updated;
                } else {
                    // try again
                    return apply(input);
                }
            } else {
                assert (cmp == 0);
            }
        }
        if (value.second() instanceof Long) {
            return Futures.immediateFuture((Long) value.second());
        } else {
            return (ListenableFuture<Long>) value.second();
        }
    }

    @Override
    public void handleConnectionOpen(ClientPeerConnection<?> connection) {
        new PeerListener(connection);
    }
    
    @Override
    public void starting() {
        super.starting();
        delegate().subscribe(this);
        for (ClientPeerConnection<?> connection: delegate()) {
            handleConnectionOpen(connection);
        }
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        stop();
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        stop();
    }
    
    protected void stop() {
        delegate().unsubscribe(this);
        for (PeerListener listener: Iterables.consumingIterable(listeners.values())) {
            listener.stop();
        }
    }

    @SuppressWarnings("rawtypes")
    private final class PeerListener implements Connection.Listener<MessagePacket> {

        private final ClientPeerConnection<?> connection;
        
        private PeerListener(
                ClientPeerConnection<?> connection) {
            this.connection = connection;
            if (listeners.putIfAbsent(connection, this) == null) {
                connection.subscribe(this);
            }
        }
        
        public void stop() {
            connection.unsubscribe(this);
            listeners.remove(connection, this);
        }
        
        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> state) {
            switch (state.to()) {
            case CONNECTION_CLOSING:
            case CONNECTION_CLOSED:
                stop();
                break;
            default:
                break;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void handleConnectionRead(final MessagePacket message) {
            if (message.getType() == MessageType.MESSAGE_TYPE_XOMEGA) {
                final MessageXomega body = (MessageXomega) message.getBody();
                Pair<UnsignedLong, ?> value = cache.get(body.getVersion().getValue());
                if (value == null) {
                    
                } else {
                    int cmp = value.first().compareTo(body.getVersion().getVersion());
                    if (cmp == 0) {
                        if (value.second() instanceof Long) {
                            // already known
                            assert (body.getXomega().equals(value.second()));
                            return;
                        }
                    } else if (cmp > 0) {
                        return;
                    } else {
                        // we don't handle out of order updates
                        assert (value.second() instanceof Long);
                    }
                    if (!cache.replace(body.getVersion().getValue(), value, Pair.create(
                            body.getVersion().getVersion(), body.getXomega()))) {
                        // try again
                        handleConnectionRead(message);
                    } else if (cmp == 0) {
                        ((Promise<Long>) value.second()).set(body.getXomega());
                    }
                }
            }
        }
    }
}
