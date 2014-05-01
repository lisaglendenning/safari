package edu.uw.zookeeper.safari.peer.protocol;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.data.VersionedId;

public interface ShardedOperation extends Operation {
    VersionedId getShard();
    
    public interface Request<V extends Operation.Request> extends ShardedOperation, Operation.Request {
        V getRequest();
    }
    
    public interface Response<V extends Operation.Response> extends ShardedOperation, Operation.Response {
        V getResponse();
    }
}
