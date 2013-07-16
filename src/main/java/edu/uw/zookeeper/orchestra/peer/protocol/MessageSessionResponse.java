package edu.uw.zookeeper.orchestra.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import edu.uw.zookeeper.orchestra.backend.ShardedResponseMessage;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_RESPONSE)
public class MessageSessionResponse extends MessageSessionBody {

    public static MessageSessionResponse of(
            long sessionId, 
            ShardedResponseMessage<?> response) {
        return new MessageSessionResponse(sessionId, response);
    }
    
    private final ShardedResponseMessage<?> response;

    @JsonCreator
    public MessageSessionResponse(
            @JsonProperty("sessionId") long sessionId, 
            @JsonProperty("response") ShardedResponseMessage<?> response) {
        super(sessionId);
        this.response = response;
    }
    
    public ShardedResponseMessage<?> getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", getSessionId())
                .add("response", getResponse())
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
        MessageSessionResponse other = (MessageSessionResponse) obj;
        return Objects.equal(getSessionId(), other.getSessionId())
                && Objects.equal(getResponse(), other.getResponse());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId(), getResponse());
    }
}
