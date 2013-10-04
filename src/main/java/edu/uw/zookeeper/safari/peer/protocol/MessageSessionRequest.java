package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_REQUEST)
public class MessageSessionRequest extends SessionMessage<ShardedRequestMessage<?>> {

    public static MessageSessionRequest of(
            Long identifier, 
            ShardedRequestMessage<?> value) {
        return new MessageSessionRequest(identifier, value);
    }
    
    @JsonCreator
    public MessageSessionRequest(
            @JsonProperty("identifier") Long identifier, 
            @JsonProperty("value") ShardedRequestMessage<?> value) {
        super(identifier, value);
    }
}
