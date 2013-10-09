package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;

// TODO: For now, this doesn't implement Message.ClientRequest
// because then the wrong serializer/deserializer is chosen by Jackson
// (this is probably fixable...)
// same goes for ShardedResponseMessage
@JsonIgnoreProperties({"request", "xid", "record"})
public class ShardedRequestMessage<V extends Records.Request> extends ShardedMessage<Message.ClientRequest<V>> implements Operation.ProtocolRequest<V>, ShardedOperation.Request<Message.ClientRequest<V>> {

    public static <V extends Records.Request> ShardedRequestMessage<V> of(
            Identifier identifier,
            Message.ClientRequest<V> message) {
        return new ShardedRequestMessage<V>(identifier, message);
    }

    @JsonCreator
    public ShardedRequestMessage(
            @JsonProperty("identifier") Identifier identifier,
            @JsonProperty("value") Message.ClientRequest<V> message) {
        super(identifier, message);
    }
    
    @Override
    public Message.ClientRequest<V> getRequest() {
        return getValue();
    }

    @Override
    public int xid() {
        return getValue().xid();
    }

    @Override
    public V record() {
        return getValue().record();
    }
}
