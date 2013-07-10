package edu.uw.zookeeper.orchestra.backend;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.AbstractPair;

public class ShardedRequest<V extends Operation.Request> extends AbstractPair<Identifier, V> implements Operation.Request {

    public static <V extends Operation.Request> ShardedRequest<V> of(Identifier id, V request) {
        return new ShardedRequest<V>(id, request);
    }
    
    public ShardedRequest(Identifier id, V request) {
        super(id, request);
    }

    public Identifier getId() {
        return first;
    }

    public V getRequest() {
        return second;
    }
}
