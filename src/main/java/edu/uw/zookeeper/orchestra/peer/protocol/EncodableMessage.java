package edu.uw.zookeeper.orchestra.peer.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Encodable;

public abstract class EncodableMessage<T extends Encodable> extends MessageBody implements Encodable {

    private final T delegate;
    
    protected EncodableMessage(T delegate) {
        this.delegate = delegate;
    }
    
    public T delegate() {
        return delegate;
    }

    public byte[] getPayload() throws IOException {
        ByteBuf output = Unpooled.buffer();
        encode(output);
        byte[] bytes = new byte[output.readableBytes()];
        output.readBytes(bytes);
        return bytes;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        delegate().encode(output);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(delegate())
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (obj.getClass() != getClass())) {
            return false;
        }
        EncodableMessage<?> other = (EncodableMessage<?>) obj;
        return Objects.equal(delegate(), other.delegate());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(delegate());
    }
}
