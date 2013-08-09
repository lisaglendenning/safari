package edu.uw.zookeeper.orchestra.peer;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.orchestra.common.Identifier;
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
    public C delegate() {
        return delegate;
    }
    
    public static class ClientPeerConnection<C extends Connection<? super MessagePacket>> extends PeerConnection<C> {

        public static <C extends Connection<? super MessagePacket>> ClientPeerConnection<C> create(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                C connection) {
            return new ClientPeerConnection<C>(localIdentifier, remoteIdentifier, connection);
        }
        
        public ClientPeerConnection(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                C connection) {
            super(localIdentifier, remoteIdentifier, connection);
        }
    }


    public static class ServerPeerConnection<C extends Connection<? super MessagePacket>> extends PeerConnection<C> {

        public static <C extends Connection<? super MessagePacket>> ServerPeerConnection<C> create(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                C connection) {
            return new ServerPeerConnection<C>(localIdentifier, remoteIdentifier, connection);
        }
        
        public ServerPeerConnection(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                C connection) {
            super(localIdentifier, remoteIdentifier, connection);
        }
    }
}