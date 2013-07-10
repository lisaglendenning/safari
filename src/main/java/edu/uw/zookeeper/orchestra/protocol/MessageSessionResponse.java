package edu.uw.zookeeper.orchestra.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.backend.ShardedResponseMessage;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_RESPONSE)
public class MessageSessionResponse extends MessageSessionBody {

    public static MessageSessionResponse of(
            long sessionId, 
            ShardedResponseMessage response) {
        return new MessageSessionResponse(sessionId, response);
    }
    
    private final ShardedResponseMessage response;

    @JsonCreator
    public MessageSessionResponse(
            @JsonProperty("sessionId") long sessionId, 
            @JsonProperty("response") ShardedResponseMessage response) {
        super(sessionId);
        this.response = response;
    }
    
    public ShardedResponseMessage getResponse() {
        return response;
    }
}
