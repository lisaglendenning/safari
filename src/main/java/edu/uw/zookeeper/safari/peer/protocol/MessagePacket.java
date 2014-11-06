package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public final class MessagePacket<T extends MessageBody> {

    public static <T extends MessageBody> MessagePacket<T> valueOf(T body) {
        return valueOf(MessageTypes.typeOf(body.getClass()), body);
    }
    
    public static <T extends MessageBody> MessagePacket<T> valueOf(MessageType type, T body) {
        return new MessagePacket<T>(type, body);
    }
    
    private final MessageType type;
    private final T body;
    
    @JsonCreator
    public MessagePacket(@JsonProperty("type") MessageType type, @JsonProperty("body") T body) {
        this.type = checkNotNull(type);
        this.body = checkNotNull(body);
    }
    
    public MessageType getType() {
        return type;
    }
    
    public T getBody() {
        return body;
    }
    
    @Override
    public String toString() {
        return ImmutableList.of(getType(), getBody()).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MessagePacket)) {
            return false;
        }
        MessagePacket<?> other = (MessagePacket<?>) obj;
        return (getType() == other.getType()) 
                && Objects.equal(getBody(), other.getBody());
    }
    
    @Override
    public int hashCode() {
        return getType().hashCode();
    }
}
