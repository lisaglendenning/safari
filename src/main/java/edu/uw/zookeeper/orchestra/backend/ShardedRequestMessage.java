package edu.uw.zookeeper.orchestra.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionRequestMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records.Request;

@JsonIgnoreProperties({"request"})
public class ShardedRequestMessage extends ShardedRequest<Message.ClientRequest> implements Operation.SessionRequest {

    public static ShardedRequestMessage of(
            Identifier id,
            Message.ClientRequest request) {
        return new ShardedRequestMessage(id, request);
    }

    @JsonCreator
    public ShardedRequestMessage(
            @JsonProperty("id") Identifier id,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(id, SessionRequestMessage.decode(Unpooled.wrappedBuffer(payload)));
    }

    public ShardedRequestMessage(Identifier id, Message.ClientRequest request) {
        super(id, request);
    }

    public byte[] getPayload() throws IOException {
        ByteBuf output = Unpooled.buffer();
        getRequest().encode(output);
        byte[] bytes = new byte[output.readableBytes()];
        output.readBytes(bytes);
        return bytes;
    }

    @Override
    public int xid() {
        return getRequest().xid();
    }

    @Override
    public Request request() {
        return getRequest().request();
    }
}
