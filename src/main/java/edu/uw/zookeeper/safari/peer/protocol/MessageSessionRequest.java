package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_REQUEST)
public class MessageSessionRequest extends SessionMessage<ShardedRequestMessage<?>> {

    public static MessageSessionRequest of(
            Long id, 
            ShardedRequestMessage<?> value) {
        return new MessageSessionRequest(id, value);
    }
    
    @JsonCreator
    public MessageSessionRequest(
            @JsonProperty("id") Long id, 
            @JsonProperty("value") ShardedRequestMessage<?> value) {
        super(id, value);
    }
}
