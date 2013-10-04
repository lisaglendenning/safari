package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_RESPONSE)
public class MessageSessionResponse extends SessionMessage<ShardedResponseMessage<?>> {

    public static MessageSessionResponse of(
            Long identifier, 
            ShardedResponseMessage<?> value) {
        return new MessageSessionResponse(identifier, value);
    }

    @JsonCreator
    public MessageSessionResponse(
            @JsonProperty("identifier") Long identifier, 
            @JsonProperty("value") ShardedResponseMessage<?> value) {
        super(identifier, value);
    }
}
