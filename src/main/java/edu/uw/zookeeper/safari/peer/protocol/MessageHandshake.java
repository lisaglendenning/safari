package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.safari.Identifier;

@MessageBodyType(MessageType.MESSAGE_TYPE_HANDSHAKE)
public class MessageHandshake extends IdentifierMessage<Identifier> {

    public static MessageHandshake of(Identifier id) {
        return new MessageHandshake(id);
    }

    @JsonCreator
    public MessageHandshake(@JsonProperty("identifier") Identifier id) {
        super(id);
    }
    
    public Identifier getIdentifier() {
        return identifier;
    }
}
