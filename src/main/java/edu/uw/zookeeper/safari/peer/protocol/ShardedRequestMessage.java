package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;

public class ShardedRequestMessage<V extends Records.Request> extends ShardedMessage<Message.ClientRequest<V>> implements Operation.ProtocolRequest<V>, ShardedOperation.Request<Message.ClientRequest<V>> {

    public static <V extends Records.Request> ShardedRequestMessage<V> valueOf(
            VersionedId shard,
            Message.ClientRequest<V> request) {
        return new ShardedRequestMessage<V>(shard, request);
    }

    @JsonCreator
    public ShardedRequestMessage(
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
