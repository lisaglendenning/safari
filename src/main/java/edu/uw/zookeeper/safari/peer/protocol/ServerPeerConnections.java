package edu.uw.zookeeper.safari.peer.protocol;

import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.ConnectionFactory.ConnectionsListener;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.IdentifierSocketAddress;

@SuppressWarnings("rawtypes")
public class ServerPeerConnections extends PeerConnections<ServerPeerConnection<?>> implements ServerConnectionFactory<ServerPeerConnection<?>>, ConnectionsListener<Connection<? super MessagePacket, ? extends MessagePacket, ?>> {

    public static ServerPeerConnections newInstance(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections) {
        return new ServerPeerConnections(identifier, timeOut, executor, connections, new StrongConcurrentSet<ConnectionsListener<? super ServerPeerConnection<?>>>());
    }
    
    public ServerPeerConnections(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections,
            IConcurrentSet<ConnectionsListener<? super ServerPeerConnection<?>>> listeners) {
        super(identifier, timeOut, executor, connections, listeners);
    }

    @Override
    public ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections() {
        return (ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>>) connections;
    }

    @Override
    public void handleConnectionOpen(Connection<? super MessagePacket, ? extends MessagePacket, ?> connection) {
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
        connections().unsubscribe(this);
        
        super.shutDown();
    }
    
    protected class ServerAcceptTask implements Connection.Listener<MessagePacket> {

        protected final Connection<? super MessagePacket, ? extends MessagePacket, ?> connection;
        
        protected ServerAcceptTask(Connection<? super MessagePacket, ? extends MessagePacket, ?> connection) {
            this.connection = connection;
            
            connection.subscribe(this);
        }
        
        @Override
        public void handleConnectionRead(MessagePacket event) {
            if (MessageType.MESSAGE_TYPE_HANDSHAKE == event.getHeader().type()) {
                MessageHandshake body = (MessageHandshake) event.getBody();
                connection.unsubscribe(this);
                ServerPeerConnection<?> peer = ServerPeerConnection.create(identifier(), body.getIdentifier(), connection, timeOut, executor);
                put(peer);
            } else {
                throw new AssertionError(event);
            }
        }

        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> state) {
            if (Connection.State.CONNECTION_CLOSED == state.to()) {
                connection.unsubscribe(this);
            }
        }
    }
}