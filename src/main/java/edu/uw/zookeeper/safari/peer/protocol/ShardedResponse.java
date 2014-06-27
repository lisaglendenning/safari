package edu.uw.zookeeper.safari.peer.protocol;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.VersionedId;

public class ShardedResponse<V extends Operation.Response> extends AbstractPair<VersionedId, V> implements ShardedOperation.Response<V> {

    public static <V extends Operation.Response> ShardedResponse<V> of(VersionedId shard, V response) {
        return new ShardedResponse<V>(shard, response);
    }
    
    public ShardedResponse(VersionedId shard, V response) {
        super(shard, response);
    }

    @Override
    public VersionedId getShard() {
        return first;
    }

    @Override
    public V getResponse() {
        return second;
    }
}
