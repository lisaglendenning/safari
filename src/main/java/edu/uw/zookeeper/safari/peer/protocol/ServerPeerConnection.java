package edu.uw.zookeeper.safari.peer.protocol;

import java.util.concurrent.ScheduledExecutorService;

import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;

public class ServerPeerConnection<C extends Connection<? super MessagePacket<?>>> extends PeerConnection<C> {

    public static <C extends Connection<? super MessagePacket<?>>> ServerPeerConnection<C> create(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new ServerPeerConnection<C>(localIdentifier, remoteIdentifier, connection, timeOut, executor);
    }
    
    public ServerPeerConnection(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super(localIdentifier, remoteIdentifier, connection, timeOut, executor);
    }
}