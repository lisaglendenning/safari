package edu.uw.zookeeper.orchestra.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_OPEN)
public class MessageSessionOpen extends MessageSessionBody {

    public static MessageSessionOpen of(long sessionId, int timeOutMillis) {
        return new MessageSessionOpen(sessionId, timeOutMillis);
    }
    
    private final int timeOutMillis;
    
    @JsonCreator
    public MessageSessionOpen(
            @JsonProperty("sessionId") long sessionId,
            @JsonProperty("timeOutMillis") int timeOutMillis) {
        super(sessionId);
        this.timeOutMillis = timeOutMillis;
    }
    
    public int getTimeOutMillis() {
        return timeOutMillis;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", getSessionId())
                .add("timeOutMillis", getTimeOutMillis())
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (obj.getClass() != getClass())) {
            return false;
        }
        MessageSessionOpen other = (MessageSessionOpen) obj;
        return Objects.equal(getSessionId(), other.getSessionId())
                && Objects.equal(getTimeOutMillis(), other.getTimeOutMillis());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId(), getTimeOutMillis());
    }
}
