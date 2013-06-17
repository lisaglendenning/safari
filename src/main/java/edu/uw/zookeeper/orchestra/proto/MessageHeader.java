package edu.uw.zookeeper.orchestra.proto;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class MessageHeader {
    
    public static MessageHeader of(MessageType type) {
        return new MessageHeader(type);
    }
    
    private final MessageType type;
    
    @JsonCreator
    public MessageHeader(@JsonProperty("type") MessageType type) {
        this.type = checkNotNull(type);
    }
    
    public MessageType getType() {
        return type;
    }

    @Override
    public String toString() {
        return getType().toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof MessageHeader)) {
            return false;
        }
        MessageHeader other = (MessageHeader) obj;
        return Objects.equal(getType(), other.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getType());
    }
}
