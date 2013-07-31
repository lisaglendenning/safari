package edu.uw.zookeeper.orchestra.peer.protocol;

import java.io.IOException;

import io.netty.buffer.Unpooled;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uw.zookeeper.protocol.ConnectMessage;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_OPEN_RESPONSE)
public class MessageSessionOpenResponse extends MessageSessionOpen<ConnectMessage.Response> {

    public static MessageSessionOpenResponse of(
            long sessionId,
            ConnectMessage.Response record) {
        return new MessageSessionOpenResponse(sessionId, record);
    }
    
    @JsonCreator
    public MessageSessionOpenResponse(
            @JsonProperty("sessionId") long sessionId,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(sessionId, ConnectMessage.Response.decode(Unpooled.wrappedBuffer(payload)));
    }

    public MessageSessionOpenResponse(
            long sessionId,
            ConnectMessage.Response record) {
        super(sessionId, record);
    }
}
