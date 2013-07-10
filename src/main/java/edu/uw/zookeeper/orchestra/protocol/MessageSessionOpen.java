package edu.uw.zookeeper.orchestra.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_OPEN)
public class MessageSessionOpen extends MessageSessionBody {

    private final long timeOutMillis;
    
    @JsonCreator
    public MessageSessionOpen(
            @JsonProperty("sessionId") long sessionId,
            @JsonProperty("timeOutMillis") long timeOutMillis) {
        super(sessionId);
        this.timeOutMillis = timeOutMillis;
    }
    
    public long getTimeOutMillis() {
        return timeOutMillis;
    }
}
