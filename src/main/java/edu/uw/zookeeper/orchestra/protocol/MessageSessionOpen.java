package edu.uw.zookeeper.orchestra.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_OPEN)
public class MessageSessionOpen extends MessageSessionBody {

    @JsonCreator
    public MessageSessionOpen(
            @JsonProperty("sessionId") long sessionId) {
        super(sessionId);
    }
}
