package edu.uw.zookeeper.orchestra.protocol;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionReplyMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;

@JsonIgnoreProperties({"reply"})
@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_REPLY)
public class MessageSessionReply extends MessageSessionBody {

    public static MessageSessionReply of(
            long sessionId, 
            Operation.SessionReply reply) {
        return new MessageSessionReply(sessionId, reply);
    }
    
    private final Operation.SessionReply reply;

    @JsonCreator
    public MessageSessionReply(
            @JsonProperty("sessionId") long sessionId, 
            @JsonProperty("opCode") int opCode,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(sessionId, SessionReplyMessage.decode(OpCode.of(opCode), Unpooled.wrappedBuffer(payload)));
    }

    public MessageSessionReply(
            long sessionId, 
            Operation.SessionReply reply) {
        super(sessionId);
        this.reply = reply;
    }
    
    public Operation.SessionReply getReply() {
        return reply;
    }

    public byte[] getPayload() throws IOException {
        ByteBuf output = Unpooled.buffer();
        getReply().encode(output);
        byte[] bytes = new byte[output.readableBytes()];
        output.readBytes(bytes);
        return bytes;
    }
    
    public int getOpCode() {
        return getReply().reply().opcode().intValue();
    }
}
