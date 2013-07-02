package edu.uw.zookeeper.orchestra.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import edu.uw.zookeeper.orchestra.Identifier;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_HANDSHAKE)
public class MessageHandshake extends MessageBody {

    public static MessageHandshake of(Identifier id) {
        return new MessageHandshake(id);
    }
    
    private final Identifier id;
    
    @JsonCreator
    public MessageHandshake(@JsonProperty("id") Identifier id) {
        this.id = checkNotNull(id);
    }

    public Identifier getId() {
        return id;
    }
    
    @Override
    public String toString() {
        return getId().toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof MessageHandshake)) {
            return false;
        }
        MessageHandshake other = (MessageHandshake) obj;
        return Objects.equal(getId(), other.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }
}
