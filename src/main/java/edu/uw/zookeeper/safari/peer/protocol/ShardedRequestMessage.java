package edu.uw.zookeeper.safari.peer.protocol;

import edu.uw.zookeeper.safari.VersionedId;

public abstract class ShardedRequestMessage<V> extends ShardedMessage<V> {

    protected ShardedRequestMessage(
            VersionedId shard,
            V value) {
        super(shard, value);
    }
}
