package edu.uw.zookeeper.safari.peer.protocol;

import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.References;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.IdentifierSocketAddress;

public class ServerPeerConnections extends PeerConnections<ServerPeerConnection<Connection<? super MessagePacket<?>>>> implements ServerConnectionFactory<ServerPeerConnection<Connection<? super MessagePacket<?>>>> {

    public static ServerPeerConnections newInstance(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket<?>>> connections) {
        return new ServerPeerConnections(identifier, timeOut, executor, connections);
    }
    
    public ServerPeerConnections(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket<?>>> connections) {
        super(identifier, timeOut, executor, connections);
    }

    @Override
    public ServerConnectionFactory<? extends Connection<? super MessagePacket<?>>> connections() {
        return (ServerConnectionFactory<? extends Connection<? super MessagePacket<?>>>) connections;
    }

    @Handler
    public void handleServerConnection(Connection<? super MessagePacket<?>> connection) {
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
        connections().subscribe(this);
        
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        try {
            connections().unsubscribe(this);
        } catch (IllegalArgumentException e) {}
        
        super.shutDown();
    }
    
    @net.engio.mbassy.listener.Listener(references = References.Strong)
    protected class ServerAcceptTask {

        protected final Connection<? super MessagePacket<?>> connection;
        
        protected ServerAcceptTask(Connection<? super MessagePacket<?>> connection) {
            this.connection = connection;
            
            connection.subscribe(this);
        }
        
        @Handler
        public void handleMessage(MessagePacket<?> event) {
            if (MessageType.MESSAGE_TYPE_HANDSHAKE == event.getHeader().type()) {
                MessageHandshake body = (MessageHandshake) event.getBody();
                ServerPeerConnection<Connection<? super MessagePacket<?>>> peer = ServerPeerConnection.<Connection<? super MessagePacket<?>>>create(identifier(), body.getIdentifier(), connection, timeOut, executor);
                connection.unsubscribe(this);
                put(peer);
            } else {
                throw new AssertionError(event);
            }
        }

        @Handler
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                connection.unsubscribe(this);
            }
        }
    }
}