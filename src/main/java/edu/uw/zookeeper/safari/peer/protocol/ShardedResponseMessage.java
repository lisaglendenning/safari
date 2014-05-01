package edu.uw.zookeeper.safari.peer.protocol;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.data.VersionedId;

public abstract class ShardedResponseMessage<V> extends ShardedMessage<V> implements Operation.RequestId {

    protected ShardedResponseMessage(
            VersionedId shard,
            V value) {
        super(shard, value);
    }
}
