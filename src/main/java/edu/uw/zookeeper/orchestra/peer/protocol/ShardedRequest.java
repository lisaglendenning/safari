package edu.uw.zookeeper.orchestra.peer.protocol;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.AbstractPair;

public class ShardedRequest<V extends Operation.Request> extends AbstractPair<Identifier, V> implements ShardedOperation.Request<V> {

    public static <V extends Operation.Request> ShardedRequest<V> of(Identifier id, V request) {
        return new ShardedRequest<V>(id, request);
    }
    
    public ShardedRequest(Identifier id, V request) {
        super(id, request);
    }

    @Override
    public Identifier getIdentifier() {
        return first;
    }

    @Override
    public V getRequest() {
        return second;
    }
}
