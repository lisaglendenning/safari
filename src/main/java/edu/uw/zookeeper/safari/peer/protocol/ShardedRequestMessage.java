package edu.uw.zookeeper.safari.peer.protocol;

import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;

@JsonIgnoreProperties({"value", "request", "xid", "record"})
public class ShardedRequestMessage<V extends Records.Request> extends ShardedMessage<Message.ClientRequest<V>> implements Message.ClientRequest<V>, ShardedOperation.Request<Message.ClientRequest<V>> {

    public static <V extends Records.Request> ShardedRequestMessage<V> of(
            Identifier identifier,
            Message.ClientRequest<V> message) {
        return new ShardedRequestMessage<V>(identifier, message);
    }

    @SuppressWarnings("unchecked")
    @JsonCreator
    public ShardedRequestMessage(
            @JsonProperty("identifier") Identifier identifier,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(identifier, (Message.ClientRequest<V>) ProtocolRequestMessage.decode(Unpooled.wrappedBuffer(payload)));
    }

    public ShardedRequestMessage(Identifier identifier, Message.ClientRequest<V> message) {
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