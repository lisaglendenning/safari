package edu.uw.zookeeper.safari.peer.protocol;

import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.IdentifierSocketAddress;
import edu.uw.zookeeper.safari.peer.protocol.PeerConnection.ServerPeerConnection;

public class ServerPeerConnections extends PeerConnections<ServerPeerConnection<Connection<? super MessagePacket>>> implements ServerConnectionFactory<ServerPeerConnection<Connection<? super MessagePacket>>> {

    public static ServerPeerConnections newInstance(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket>> connections) {
        return new ServerPeerConnections(identifier, timeOut, executor, connections);
    }
    
    public ServerPeerConnections(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket>> connections) {
        super(identifier, timeOut, executor, connections);
    }

    @Override
    public ServerConnectionFactory<? extends Connection<? super MessagePacket>> connections() {
        return (ServerConnectionFactory<? extends Connection<? super MessagePacket>>) connections;
    }

    @Subscribe
    public void handleServerConnection(Connection<? super MessagePacket> connection) {
        if (! (connection instanceof ServerPeerConnection)) {
            new ServerAcceptTask(connection);
        }
    }
    
    @Override
    public SocketAddress listenAddress() {
        return IdentifierSocketAddress.of(identifier(), connections().listenAddress());
    }

    @Override
    protected void startUp() throws Exception {
        connections().register(this);
        
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        try {
            connections().unregister(this);
        } catch (IllegalArgumentException e) {}
        
        super.shutDown();
    }

    protected class ServerAcceptTask {

        protected final Connection<? super MessagePacket> connection;
        
        protected ServerAcceptTask(Connection<? super MessagePacket> connection) {
            this.connection = connection;
            
            connection.register(this);
        }
        
        @Subscribe
        public void handleMessage(MessagePacket event) {
            if (MessageType.MESSAGE_TYPE_HANDSHAKE == event.first().type()) {
                MessageHandshake body = event.getBody(MessageHandshake.class);
                ServerPeerConnection<Connection<? super MessagePacket>> peer = ServerPeerConnection.<Connection<? super MessagePacket>>create(identifier(), body.getIdentifier(), connection, timeOut, executor);
                connection.unregister(this);
                put(peer);
            } else {
                throw new AssertionError(event);
            }
        }

        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                connection.unregister(this);
            }
        }
    }
}