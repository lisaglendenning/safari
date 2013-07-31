package edu.uw.zookeeper.orchestra.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;


@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_REQUEST)
public class MessageSessionRequest extends MessageBody {

    public static MessageSessionRequest of(
            long sessionId, 
            ShardedRequestMessage<?> request) {
        return new MessageSessionRequest(sessionId, request);
    }
    
    private final long sessionId;
    private final ShardedRequestMessage<?> request;

    @JsonCreator
    public MessageSessionRequest(
            @JsonProperty("sessionId") long sessionId, 
            @JsonProperty("request") ShardedRequestMessage<?> request) {
        this.sessionId = sessionId;
        this.request = request;
    }
    
    public long getSessionId() {
        return sessionId;
    }
    
    public ShardedRequestMessage<?> getRequest() {
        return request;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", getSessionId())
                .add("request", getRequest())
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
        MessageSessionRequest other = (MessageSessionRequest) obj;
        return Objects.equal(getSessionId(), other.getSessionId())
                && Objects.equal(getRequest(), other.getRequest());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId(), getRequest());
    }
}
