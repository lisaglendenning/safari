package edu.uw.zookeeper.orchestra.peer.protocol;

import java.io.IOException;

import io.netty.buffer.Unpooled;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.ConnectMessage;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_OPEN_REQUEST)
public class MessageSessionOpenRequest extends OpenSessionMessage<ConnectMessage.Request> {

    public static MessageSessionOpenRequest of(
            Long identifier,
            ConnectMessage.Request value) {
        return new MessageSessionOpenRequest(identifier, value);
    }
    
    @JsonCreator
    public MessageSessionOpenRequest(
            @JsonProperty("identifier") Long identifier,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(identifier, ConnectMessage.Request.decode(Unpooled.wrappedBuffer(payload)));
    }

    public MessageSessionOpenRequest(
            Long identifier,
            ConnectMessage.Request value) {
        super(identifier, value);
    }
}
