package edu.uw.zookeeper.safari.peer.protocol;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.VersionedId;

public class ShardedRequest<V extends Operation.Request> extends AbstractPair<VersionedId, V> implements ShardedOperation.Request<V> {

    public static <V extends Operation.Request> ShardedRequest<V> of(VersionedId shard, V request) {
        return new ShardedRequest<V>(shard, request);
    }
    
    public ShardedRequest(VersionedId shard, V request) {
        super(shard, request);
    }

    @Override
    public VersionedId getShard() {
        return first;
    }

    @Override
    public V getRequest() {
        return second;
    }
}
