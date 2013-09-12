package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_RESPONSE)
public class MessageSessionResponse extends SessionMessage<ShardedResponseMessage<?>> {

    public static MessageSessionResponse of(
            Long id, 
            ShardedResponseMessage<?> value) {
        return new MessageSessionResponse(id, value);
    }

    @JsonCreator
    public MessageSessionResponse(
            @JsonProperty("id") Long id, 
            @JsonProperty("value") ShardedResponseMessage<?> value) {
        super(id, value);
    }
}
