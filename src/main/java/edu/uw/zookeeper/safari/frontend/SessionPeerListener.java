package edu.uw.zookeeper.safari.frontend;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public interface SessionPeerListener extends Connection.Listener<ShardedResponseMessage<?>> {
    Session session();
}
