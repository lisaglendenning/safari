package edu.uw.zookeeper.orchestra.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.backend.ShardedRequestMessage;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION_REQUEST)
public class MessageSessionRequest extends MessageSessionBody {

    public static MessageSessionRequest of(
            long sessionId, 
            ShardedRequestMessage request) {
        return new MessageSessionRequest(sessionId, request);
    }
    
    private final ShardedRequestMessage request;

    @JsonCreator
    public MessageSessionRequest(
            @JsonProperty("sessionId") long sessionId, 
            @JsonProperty("request") ShardedRequestMessage request) {
        super(sessionId);
        this.request = request;
    }
    
    public ShardedRequestMessage getRequest() {
        return request;
    }
}
