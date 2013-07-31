package edu.uw.zookeeper.orchestra.peer.protocol;

import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.Records;

@JsonIgnoreProperties({"request", "xid", "record"})
public class ShardedRequestMessage<V extends Records.Request> extends EncodableMessage<Message.ClientRequest<V>> implements Message.ClientRequest<V>, ShardedOperation.Request<Message.ClientRequest<V>> {

    public static <V extends Records.Request> ShardedRequestMessage<V> of(
            Identifier identifier,
            Message.ClientRequest<V> request) {
        return new ShardedRequestMessage<V>(identifier, request);
    }

    @SuppressWarnings("unchecked")
    @JsonCreator
    public ShardedRequestMessage(
            @JsonProperty("identifier") Identifier identifier,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(identifier, (Message.ClientRequest<V>) ProtocolRequestMessage.decode(Unpooled.wrappedBuffer(payload)));
    }

    private final Identifier identifier;
    
    public ShardedRequestMessage(Identifier identifier, Message.ClientRequest<V> request) {
        super(request);
        this.identifier = identifier;
    }
    
    @Override
    public Identifier getIdentifier() {
        return identifier;
    }
    
    @Override
    public Message.ClientRequest<V> getRequest() {
        return delegate();
    }

    @Override
    public int getXid() {
        return getRequest().getXid();
    }

    @Override
    public V getRecord() {
        return getRequest().getRecord();
    }
}
