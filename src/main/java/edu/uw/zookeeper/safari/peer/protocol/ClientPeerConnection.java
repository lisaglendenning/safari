package edu.uw.zookeeper.safari.peer.protocol;

import java.util.concurrent.ScheduledExecutorService;

import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;

@SuppressWarnings("rawtypes")
public class ClientPeerConnection<T extends Connection<? super MessagePacket,? extends MessagePacket,?>> extends PeerConnection<T, ClientPeerConnection<T>> {

    public static <T extends Connection<? super MessagePacket,? extends MessagePacket,?>> ClientPeerConnection<T> create(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            T connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new ClientPeerConnection<T>(
                localIdentifier, remoteIdentifier, connection, timeOut, executor);
    }
    
    public ClientPeerConnection(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            T connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super(localIdentifier, remoteIdentifier, connection, timeOut, executor);
    }
}