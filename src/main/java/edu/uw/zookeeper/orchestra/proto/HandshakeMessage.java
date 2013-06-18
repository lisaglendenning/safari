package edu.uw.zookeeper.orchestra.proto;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import edu.uw.zookeeper.orchestra.Identifier;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_HANDSHAKE)
public class HandshakeMessage extends MessageBody {

    public static HandshakeMessage of(Identifier id) {
        return new HandshakeMessage(id);
    }
    
    private final Identifier id;
    
    @JsonCreator
    public HandshakeMessage(@JsonProperty("id") Identifier id) {
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
        if (! (obj instanceof HandshakeMessage)) {
            return false;
        }
        HandshakeMessage other = (HandshakeMessage) obj;
        return Objects.equal(getId(), other.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }
}
