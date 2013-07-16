package edu.uw.zookeeper.orchestra.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

@JsonIgnoreProperties({"response"})
public class ShardedResponseMessage<V extends Records.Response> extends ShardedResponse<Message.ServerResponse<V>> implements Message.ServerResponse<V> {

    public static <V extends Records.Response> ShardedResponseMessage<V> of(
            Identifier id, Message.ServerResponse<V> response) {
        return new ShardedResponseMessage<V>(id, response);
    }

    @SuppressWarnings("unchecked")
    @JsonCreator
    public ShardedResponseMessage(
            @JsonProperty("id") Identifier id,
            @JsonProperty("opCode") int opCode,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(id, (Message.ServerResponse<V>) ProtocolResponseMessage.decode(OpCode.of(opCode), Unpooled.wrappedBuffer(payload)));
    }

    public ShardedResponseMessage(Identifier id, Message.ServerResponse<V> response) {
        super(id, response);
    }

    public byte[] getPayload() throws IOException {
        ByteBuf output = Unpooled.buffer();
        encode(output);
        byte[] bytes = new byte[output.readableBytes()];
        output.readBytes(bytes);
        return bytes;
    }

    public int getOpCode() {
        return getRecord().getOpcode().intValue();
    }

    @Override
    public int getXid() {
        return getResponse().getXid();
    }

    @Override
    public long getZxid() {
        return getResponse().getZxid();
    }

    @Override
    public V getRecord() {
        return getResponse().getRecord();
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        getResponse().encode(output);
    }
}
