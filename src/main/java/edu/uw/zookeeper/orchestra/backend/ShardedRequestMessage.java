package edu.uw.zookeeper.orchestra.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.Records;

@JsonIgnoreProperties({"request"})
public class ShardedRequestMessage<V extends Records.Request> extends ShardedRequest<Message.ClientRequest<V>> implements Message.ClientRequest<V> {

    public static <V extends Records.Request> ShardedRequestMessage<V> of(
            Identifier id,
            Message.ClientRequest<V> request) {
        return new ShardedRequestMessage<V>(id, request);
    }

    @SuppressWarnings("unchecked")
    @JsonCreator
    public ShardedRequestMessage(
            @JsonProperty("id") Identifier id,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(id, (Message.ClientRequest<V>) ProtocolRequestMessage.decode(Unpooled.wrappedBuffer(payload)));
    }

    public ShardedRequestMessage(Identifier id, Message.ClientRequest<V> request) {
        super(id, request);
    }

    public byte[] getPayload() throws IOException {
        ByteBuf output = Unpooled.buffer();
        encode(output);
        byte[] bytes = new byte[output.readableBytes()];
        output.readBytes(bytes);
        return bytes;
    }

    @Override
    public int getXid() {
        return getRequest().getXid();
    }

    @Override
    public V getRecord() {
        return getRequest().getRecord();
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        getRequest().encode(output);
    }
}
