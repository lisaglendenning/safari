package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;

// TODO: For now, this doesn't implement Message.ClientRequest
// because then the wrong serializer/deserializer is chosen by Jackson
// (this is probably fixable...)
// same goes for ShardedServerResponseMessage
public class ShardedClientRequestMessage<V extends Records.Request> extends ShardedRequestMessage<Message.ClientRequest<V>> implements Operation.ProtocolRequest<V>, ShardedOperation.Request<Message.ClientRequest<V>> {

    public static <V extends Records.Request> ShardedClientRequestMessage<V> valueOf(
            VersionedId shard,
            Message.ClientRequest<V> request) {
        return new ShardedClientRequestMessage<V>(shard, request);
    }

    @JsonCreator
    public ShardedClientRequestMessage(
            @JsonProperty("shard") VersionedId shard,
            @JsonProperty("request") Message.ClientRequest<V> request) {
        super(shard, request);
    }
    
    @Override
    public Message.ClientRequest<V> getRequest() {
        return value;
    }

    @Override
    public int xid() {
        return value.xid();
    }

    @Override
    public V record() {
        return value.record();
    }
}
