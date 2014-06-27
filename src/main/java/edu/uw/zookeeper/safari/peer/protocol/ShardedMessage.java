package edu.uw.zookeeper.safari.peer.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.safari.VersionedId;

public abstract class ShardedMessage<V> extends ValueMessage<VersionedId,V> {

    protected ShardedMessage(VersionedId shard, V value) {
        super(shard, value);
    }
    
    public VersionedId getShard() {
        return identifier;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("shard", identifier)
                .add("value", value)
                .toString();
    }
}
