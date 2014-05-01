package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.data.VersionedId;

public class ShardedServerResponseMessage<V extends Records.Response> extends ShardedResponseMessage<Message.ServerResponse<V>> implements Operation.ProtocolResponse<V>, ShardedOperation.Response<Message.ServerResponse<V>> {

    public static <V extends Records.Response> ShardedServerResponseMessage<V> valueOf(
            VersionedId shard, Message.ServerResponse<V> message) {
        return new ShardedServerResponseMessage<V>(shard, message);
    }

    @JsonCreator
    public ShardedServerResponseMessage(
            @JsonProperty("shard") VersionedId shard,
            @JsonProperty("response") Message.ServerResponse<V> response) {
        super(shard, response);
    }

    @Override
    public Message.ServerResponse<V> getResponse() {
        return value;
    }
    
    @Override
    public int xid() {
        return value.xid();
    }

    @Override
    public long zxid() {
        return value.zxid();
    }

    @Override
    public V record() {
        return value.record();
    }
}
