package edu.uw.zookeeper.orchestra.peer.protocol;

import java.io.IOException;

import io.netty.buffer.Unpooled;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uw.zookeeper.protocol.ConnectMessage;

@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_OPEN_RESPONSE)
public class MessageSessionOpenResponse extends OpenSessionMessage<ConnectMessage.Response> {

    public static MessageSessionOpenResponse of(
            Long identifier,
            ConnectMessage.Response value) {
        return new MessageSessionOpenResponse(identifier, value);
    }
    
    @JsonCreator
    public MessageSessionOpenResponse(
            @JsonProperty("identifier") Long identifier,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(identifier, ConnectMessage.Response.decode(Unpooled.wrappedBuffer(payload)));
    }

    public MessageSessionOpenResponse(
            Long identifier,
            ConnectMessage.Response value) {
        super(identifier, value);
    }
}
