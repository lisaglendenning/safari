package edu.uw.zookeeper.orchestra.protocol;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionRequestMessage;

@JsonIgnoreProperties({"request"})
@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_REQUEST)
public class MessageSessionRequest extends MessageSessionBody {

    public static MessageSessionRequest of(
            long sessionId, 
            Operation.SessionRequest request) {
        return new MessageSessionRequest(sessionId, request);
    }
    
    private final Operation.SessionRequest request;

    @JsonCreator
    public MessageSessionRequest(
            @JsonProperty("sessionId") long sessionId, 
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(sessionId, SessionRequestMessage.decode(Unpooled.wrappedBuffer(payload)));
    }

    public MessageSessionRequest(
            long sessionId, 
            Operation.SessionRequest request) {
        super(sessionId);
        this.request = request;
    }
    
    public byte[] getPayload() throws IOException {
        ByteBuf output = Unpooled.buffer();
        getRequest().encode(output);
        byte[] bytes = new byte[output.readableBytes()];
        output.readBytes(bytes);
        return bytes;
    }
    
    public Operation.SessionRequest getRequest() {
        return request;
    }
}
