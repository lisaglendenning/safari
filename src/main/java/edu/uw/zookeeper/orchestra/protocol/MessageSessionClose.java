package edu.uw.zookeeper.orchestra.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_CLOSE)
public class MessageSessionClose extends MessageSessionBody {

    @JsonCreator
    public MessageSessionClose(
            @JsonProperty("sessionId") long sessionId) {
        super(sessionId);
    }
}
