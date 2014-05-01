package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_REQUEST)
public class MessageSessionRequest extends SessionMessage<ShardedRequestMessage<?>> {

    public static MessageSessionRequest of(
            Long identifier, 
            ShardedRequestMessage<?> message) {
        return new MessageSessionRequest(identifier, message);
    }
    
    @JsonCreator
    public MessageSessionRequest(
            @JsonProperty("identifier") Long identifier, 
            @JsonProperty("message") ShardedRequestMessage<?> message) {
        super(identifier, message);
    }
}
