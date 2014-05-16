package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.ForwardingServiceListener;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.safari.Identifier;

public abstract class PeerConnections<C extends PeerConnection<?,?>> extends ServiceListenersService implements ConnectionFactory<C> {

    protected final Identifier identifier;
    protected final TimeValue timeOut;
    protected final ScheduledExecutorService executor;
    @SuppressWarnings("rawtypes")
    protected final ConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections;
    protected final ConcurrentMap<Identifier, C> peers;
    protected final IConcurrentSet<ConnectionsListener<? super C>> listeners;
    
    @SuppressWarnings("rawtypes")
    protected PeerConnections(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections,
            Iterable<? extends Service.Listener> listeners) {
        super(ImmutableList.<Service.Listener>builder().addAll(listeners).add(ForwardingServiceListener.forService(connections)).build());
        this.listeners = new StrongConcurrentSet<ConnectionsListener<? super C>>();
        this.identifier = identifier;
        this.timeOut = timeOut;
        this.executor = executor;
        this.connections = connections;
        this.peers = new MapMaker().makeMap();
    }
    
    public Identifier identifier() {
        return identifier;
    }
    
    @SuppressWarnings("rawtypes")
    public ConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections() {
        return connections;
    }

    public C get(Identifier peer) {
        C connection = peers.get(peer);
        switch (connection.state()) {
        case CONNECTION_CLOSING:
        case CONNECTION_CLOSED:
            remove(connection);
            return null;
        default:
            return connection;
        }
    }
    
    public Set<Map.Entry<Identifier, C>> entrySet() {
        return peers.entrySet();
    }

    @Override
    public void subscribe(ConnectionsListener<? super C> listener) {
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(ConnectionsListener<? super C> listener) {
        return listeners.remove(listener);
    }
    
    @Override
    public Iterator<C> iterator() {
        return peers.values().iterator();
    }

    public C put(C v) {
        C prev = peers.put(v.remoteAddress().getIdentifier(), v);
        new ConnectionListener(v);
        if (prev != null) {
            prev.close();
        }
        for (ConnectionsListener<? super C> l: listeners) {
            l.handleConnectionOpen(v);
        }
        return prev;
    }

    public C putIfAbsent(C v) {
        C prev = peers.putIfAbsent(v.remoteAddress().getIdentifier(), v);
        if (prev != null) {
            v.close();
        } else {
            new ConnectionListener(v);
            for (ConnectionsListener<? super C> l: listeners) {
                l.handleConnectionOpen(v);
            }
        }
        return prev;
    }
    
    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }

    protected boolean remove(C connection) {
        boolean removed = peers.remove(connection.remoteAddress().getIdentifier(), connection);
        if (removed) {
            logger.info("Removed {}", connection);
        }
        return removed;
    }

    protected class ConnectionListener extends Factories.Holder<C> implements Connection.Listener<Object> {
    
        public ConnectionListener(C connection) {
            super(checkNotNull(connection));
            connection.subscribe(this);
            if (Connection.State.CONNECTION_CLOSED == connection.state()) {
                handleConnectionState(Automaton.Transition.create(Connection.State.CONNECTION_OPENING, Connection.State.CONNECTION_CLOSED));
            }
        }
        
        @Override
        public void handleConnectionState(
                Automaton.Transition<Connection.State> transition) {
            if (Connection.State.CONNECTION_CLOSED == transition.to()) {
                get().unsubscribe(this);
                remove(get());
            }
        }

        @Override
        public void handleConnectionRead(Object message) {
        }
    }
}