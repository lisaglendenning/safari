package edu.uw.zookeeper.orchestra.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionResponseMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.Response;

@JsonIgnoreProperties({"response"})
public class ShardedResponseMessage extends ShardedResponse<Message.ServerResponse> implements Operation.SessionResponse {

    public static ShardedResponseMessage of(
            Identifier id, Message.ServerResponse response) {
        return new ShardedResponseMessage(id, response);
    }

    @JsonCreator
    public ShardedResponseMessage(
            @JsonProperty("id") Identifier id,
            @JsonProperty("opCode") int opCode,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(id, SessionResponseMessage.decode(OpCode.of(opCode), Unpooled.wrappedBuffer(payload)));
    }

    public ShardedResponseMessage(Identifier id, Message.ServerResponse response) {
        super(id, response);
    }

    public byte[] getPayload() throws IOException {
        ByteBuf output = Unpooled.buffer();
        getResponse().encode(output);
        byte[] bytes = new byte[output.readableBytes()];
        output.readBytes(bytes);
        return bytes;
    }

    public int getOpCode() {
        return getResponse().response().opcode().intValue();
    }

    @Override
    public int xid() {
        return getResponse().xid();
    }

    @Override
    public long zxid() {
        return getResponse().zxid();
    }

    @Override
    public Response response() {
        return getResponse().response();
    }
}
