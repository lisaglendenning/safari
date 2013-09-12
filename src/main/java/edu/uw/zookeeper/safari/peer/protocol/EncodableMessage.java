package edu.uw.zookeeper.safari.peer.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import edu.uw.zookeeper.protocol.Encodable;

@JsonIgnoreProperties({"value"})
public abstract class EncodableMessage<T, V extends Encodable> extends ValueMessage<T,V> implements Encodable {

    protected EncodableMessage(T identifier, V value) {
        super(identifier, value);
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
        getValue().encode(output);
    }
}
