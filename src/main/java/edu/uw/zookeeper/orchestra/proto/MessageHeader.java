package edu.uw.zookeeper.orchestra.proto;

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Encodable;

public class MessageHeader implements Encodable {

    public static MessageHeader decode(ByteBuf input) {
        return MessageHeader.of(MessageType.valueOf(input.readInt()));
    }
    
    public static MessageHeader of(MessageType type) {
        return new MessageHeader(type);
    }
    
    private final MessageType type;
    
    public MessageHeader(MessageType type) {
        this.type = checkNotNull(type);
    }
    
    public MessageType type() {
        return type;
    }

    @Override
    public String toString() {
        return type().toString();
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
        return Objects.equal(type(), other.type());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type());
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        output.writeInt(type().intValue());
    }
}
