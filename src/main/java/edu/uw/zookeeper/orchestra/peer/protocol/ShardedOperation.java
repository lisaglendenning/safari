package edu.uw.zookeeper.orchestra.peer.protocol;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Operation;

public interface ShardedOperation extends Operation {
    Identifier getIdentifier();
    
    public interface Request<V extends Operation.Request> extends ShardedOperation, Operation.Request {
        V getRequest();
    }
    
    public interface Response<V extends Operation.Response> extends ShardedOperation, Operation.Response {
        V getResponse();
    }
}
