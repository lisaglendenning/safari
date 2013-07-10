package edu.uw.zookeeper.orchestra.backend;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.AbstractPair;

public class ShardedResponse<V extends Operation.Response> extends AbstractPair<Identifier, V> implements Operation.Response {

    public static <V extends Operation.Response> ShardedResponse<V> of(Identifier id, V response) {
        return new ShardedResponse<V>(id, response);
    }
    
    public ShardedResponse(Identifier id, V response) {
        super(id, response);
    }

    public Identifier getId() {
        return first;
    }

    public V getResponse() {
        return second;
    }
}
