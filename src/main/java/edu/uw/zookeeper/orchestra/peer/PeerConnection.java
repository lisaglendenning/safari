package edu.uw.zookeeper.orchestra.peer;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;

public class PeerConnection<C extends Connection<? super MessagePacket>> extends ForwardingConnection<MessagePacket> {

    protected final C delegate;
    protected final Identifier localIdentifier;
    protected final Identifier remoteIdentifier;
    
    public PeerConnection(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            C delegate) {
        this.delegate = delegate;
        this.localIdentifier = localIdentifier;
        this.remoteIdentifier = remoteIdentifier;
        
    }

    @Override
    public IdentifierSocketAddress localAddress() {
        return IdentifierSocketAddress.of(localIdentifier, super.localAddress());
    }

    @Override
    public IdentifierSocketAddress remoteAddress() {
        return IdentifierSocketAddress.of(remoteIdentifier, super.remoteAddress());
    }

    @Override
    protected C delegate() {
        return delegate;
    }
    
    public static class ClientPeerConnection extends PeerConnection<Connection<? super MessagePacket>> {

        public ClientPeerConnection(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                Connection<? super MessagePacket> delegate) {
            super(localIdentifier, remoteIdentifier, delegate);
        }
    }


    public static class ServerPeerConnection extends PeerConnection<Connection<? super MessagePacket>> {

        public ServerPeerConnection(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                Connection<? super MessagePacket> delegate) {
            super(localIdentifier, remoteIdentifier, delegate);
        }
    }
}