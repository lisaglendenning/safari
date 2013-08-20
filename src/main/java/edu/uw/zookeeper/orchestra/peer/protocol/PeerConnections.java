package edu.uw.zookeeper.orchestra.peer.protocol;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.orchestra.common.Identifier;

public abstract class PeerConnections<V extends PeerConnection<Connection<? super MessagePacket>>> extends ForwardingService implements ConnectionFactory<V> {

    protected final Identifier identifier;
    protected final TimeValue timeOut;
    protected final ScheduledExecutorService executor;
    protected final ConnectionFactory<? extends Connection<? super MessagePacket>> connections;
    protected final ConcurrentMap<Identifier, V> peers;
    
    protected PeerConnections(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ConnectionFactory<? extends Connection<? super MessagePacket>> connections) {
        this.identifier = identifier;
        this.timeOut = timeOut;
        this.executor = executor;
        this.connections = connections;
        this.peers = new MapMaker().makeMap();
    }
    
    public Identifier identifier() {
        return identifier;
    }
    
    public ConnectionFactory<?> connections() {
        return connections;
    }

    public V get(Identifier peer) {
        V connection = peers.get(peer);
        switch (connection.state()) {
        case CONNECTION_CLOSING:
        case CONNECTION_CLOSED:
            peers.remove(peer, connection);
            return null;
        default:
            return connection;
        }
    }
    
    public Set<Map.Entry<Identifier, V>> entrySet() {
        return peers.entrySet();
    }

    @Override
    public Iterator<V> iterator() {
        return peers.values().iterator();
    }

    @Override
    public void post(Object event) {
        connections.post(event);
    }

    @Override
    public void register(Object handler) {
        connections.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        connections.unregister(handler);
    }
    
    public V put(V v) {
        V prev = peers.put(v.remoteAddress().getIdentifier(), v);
        new RemoveOnClose(v);
        if (prev != null) {
            prev.close();
        }
        post(v);
        return prev;
    }

    public V putIfAbsent(V v) {
        V prev = peers.putIfAbsent(v.remoteAddress().getIdentifier(), v);
        if (prev != null) {
            v.close();
        } else {
            new RemoveOnClose(v);
            post(v);
        }
        return prev;
    }

    protected class RemoveOnClose {
        
        protected final V instance;
        
        public RemoveOnClose(V instance) {
            this.instance = instance;
            instance.register(this);
        }
    
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    instance.unregister(this);
                } catch (IllegalArgumentException e) {}
                peers.remove(instance.remoteAddress().getIdentifier(), instance);
            }
        }
    }

    @Override
    protected Service delegate() {
        return connections;
    }
}