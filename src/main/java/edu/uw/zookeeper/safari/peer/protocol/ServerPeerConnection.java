package edu.uw.zookeeper.safari.peer.protocol;

import java.util.concurrent.ScheduledExecutorService;

import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;

@SuppressWarnings("rawtypes")
public class ServerPeerConnection<T extends Connection<? super MessagePacket,? extends MessagePacket,?>> extends PeerConnection<T, ServerPeerConnection<T>> {

    public static <T extends Connection<? super MessagePacket,? extends MessagePacket,?>> ServerPeerConnection<T> create(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            T connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new ServerPeerConnection<T>(localIdentifier, remoteIdentifier, connection, timeOut, executor);
    }
    
    public ServerPeerConnection(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            T connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super(localIdentifier, remoteIdentifier, connection, timeOut, executor);
    }
}