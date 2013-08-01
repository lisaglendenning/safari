package edu.uw.zookeeper.orchestra.peer;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.util.Automaton;

public class PeerConnections<C extends Connection<? super MessagePacket>, V extends PeerConnection<Connection<? super MessagePacket>>> extends AbstractIdleService implements ConnectionFactory<V> {
    
    protected final ConnectionFactory<C> connections;
    protected final ConcurrentMap<Identifier, V> peers;
    
    public PeerConnections(
            ConnectionFactory<C> connections) {
        this.connections = connections;
        this.peers = new MapMaker().makeMap();
    }
    
    public ConnectionFactory<C> connections() {
        return connections;
    }

    public V get(Identifier peer) {
        return peers.get(peer);
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
        connections().post(event);
    }

    @Override
    public void register(Object handler) {
        connections().register(handler);
    }

    @Override
    public void unregister(Object handler) {
        connections().unregister(handler);
    }
    
    protected V put(Identifier id, V v) {
        V prev = peers.put(id, v);
        new RemoveOnClose(id, v);
        if (prev != null) {
            prev.close();
        }
        post(v);
        return prev;
    }

    protected V putIfAbsent(Identifier id, V v) {
        V prev = peers.putIfAbsent(id, v);
        if (prev != null) {
            v.close();
        } else {
            new RemoveOnClose(id, v);
            post(v);
        }
        return prev;
    }
    
    @Override
    protected void startUp() throws Exception {
        connections().start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        connections().stop().get();
    }

    protected class RemoveOnClose {
        
        protected final Identifier identifier;
        protected final V instance;
        
        public RemoveOnClose(Identifier identifier, V instance) {
            this.identifier = identifier;
            this.instance = instance;
            instance.register(this);
        }
    
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    instance.unregister(this);
                } catch (IllegalArgumentException e) {}
                peers.remove(identifier, instance);
            }
        }
    }
}