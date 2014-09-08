package edu.uw.zookeeper.safari.peer.protocol;

import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.safari.Identifier;

@SuppressWarnings("rawtypes")
public class ServerPeerConnections extends PeerConnections<ServerPeerConnection<?>> implements ServerConnectionFactory<ServerPeerConnection<?>> {

    public static ServerPeerConnections newInstance(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections) {
        return new ServerPeerConnections(identifier, timeOut, executor, connections);
    }
    
    public ServerPeerConnections(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections) {
        super(identifier, timeOut, executor, connections, ImmutableList.<Service.Listener>of());
        addListener(new Listener(), MoreExecutors.directExecutor());
    }

    @Override
    public ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections() {
        return (ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>>) connections;
    }
    
    @Override
    public SocketAddress listenAddress() {
        return IdentifierSocketAddress.of(identifier(), connections().listenAddress());
    }
    
    protected class Listener extends Service.Listener implements ConnectionsListener<Connection<? super MessagePacket, ? extends MessagePacket, ?>> {

        @Override
        public void handleConnectionOpen(Connection<? super MessagePacket, ? extends MessagePacket, ?> connection) {
            if (! (connection instanceof ServerPeerConnection)) {
                new ServerAcceptTask(connection);
            }
        }
        
        @Override
        public void starting() {
            connections().subscribe(this);
        }
        
        @Override
        public void stopping(State from) {
            connections().unsubscribe(this);
        }
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