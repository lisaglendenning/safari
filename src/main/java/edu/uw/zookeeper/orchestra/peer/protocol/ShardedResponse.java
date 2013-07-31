package edu.uw.zookeeper.orchestra.peer.protocol;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.AbstractPair;

public class ShardedResponse<V extends Operation.Response> extends AbstractPair<Identifier, V> implements ShardedOperation.Response<V> {

    public static <V extends Operation.Response> ShardedResponse<V> of(Identifier id, V response) {
        return new ShardedResponse<V>(id, response);
    }
    
    public ShardedResponse(Identifier id, V response) {
        super(id, response);
    }

    @Override
    public Identifier getIdentifier() {
        return first;
    }

    @Override
    public V getResponse() {
        return second;
    }
}
