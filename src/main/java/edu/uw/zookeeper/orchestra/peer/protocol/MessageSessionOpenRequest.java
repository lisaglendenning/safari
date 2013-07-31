package edu.uw.zookeeper.orchestra.peer.protocol;

import java.io.IOException;

import io.netty.buffer.Unpooled;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.ConnectMessage;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_OPEN_REQUEST)
public class MessageSessionOpenRequest extends MessageSessionOpen<ConnectMessage.Request> {

    public static MessageSessionOpenRequest of(
            long sessionId,
            ConnectMessage.Request record) {
        return new MessageSessionOpenRequest(sessionId, record);
    }
    
    @JsonCreator
    public MessageSessionOpenRequest(
            @JsonProperty("sessionId") long sessionId,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(sessionId, ConnectMessage.Request.decode(Unpooled.wrappedBuffer(payload)));
    }

    public MessageSessionOpenRequest(
            long sessionId,
            ConnectMessage.Request record) {
        super(sessionId, record);
    }
}
