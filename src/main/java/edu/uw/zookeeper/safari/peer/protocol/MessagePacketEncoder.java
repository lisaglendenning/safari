package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import edu.uw.zookeeper.net.Encoder;

@SuppressWarnings("rawtypes")
public final class MessagePacketEncoder implements Encoder<MessagePacket, MessagePacket> {

    public static MessagePacketEncoder defaults(ObjectMapper mapper) {
        return new MessagePacketEncoder(
                mapper.writer());
    }
    
    protected final ObjectWriter writer;
    
    public MessagePacketEncoder(ObjectWriter writer) {
        this.writer = checkNotNull(writer);
    }

    @Override
    public Class<? extends MessagePacket> encodeType() {
        return MessagePacket.class;
    }
    
    @Override
    public void encode(MessagePacket input, ByteBuf output)
            throws IOException {
        ByteBufOutputStream stream = new ByteBufOutputStream(output);
        try {
            writer.writeValue(stream, input);
        } finally {
            stream.close();
        }
    }
}
