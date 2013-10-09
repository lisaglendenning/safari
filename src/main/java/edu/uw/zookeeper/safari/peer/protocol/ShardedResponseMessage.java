package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;

@JsonIgnoreProperties({"response", "xid", "zxid", "record"})
public class ShardedResponseMessage<V extends Records.Response> extends ShardedMessage<Message.ServerResponse<V>> implements Operation.ProtocolResponse<V>, ShardedOperation.Response<Message.ServerResponse<V>> {

    public static <V extends Records.Response> ShardedResponseMessage<V> of(
            Identifier identifier, Message.ServerResponse<V> message) {
        return new ShardedResponseMessage<V>(identifier, message);
    }

    @JsonCreator
    public ShardedResponseMessage(
            @JsonProperty("identifier") Identifier identifier,
            @JsonProperty("value") Message.ServerResponse<V> message) {
        super(identifier, message);
    }

    @Override
    public Message.ServerResponse<V> getResponse() {
        return getValue();
    }
    
    @Override
    public int xid() {
        return getValue().xid();
    }

    @Override
    public long zxid() {
        return getValue().zxid();
    }

    @Override
    public V record() {
        return getValue().record();
    }
}
