package edu.uw.zookeeper.safari.peer.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import edu.uw.zookeeper.net.Decoder;

@SuppressWarnings("rawtypes")
public final class MessagePacketDecoder implements Decoder<MessagePacket, MessagePacket> {

    public static MessagePacketDecoder defaults(ObjectMapper mapper) {
        return new MessagePacketDecoder(
                mapper.reader());
    }
    
    protected final ObjectReader reader;
    
    public MessagePacketDecoder(ObjectReader reader) {
        this.reader = reader.withType(MessagePacket.class);
    }

    @Override
    public Class<? extends MessagePacket> decodeType() {
        return MessagePacket.class;
    }

    @Override
    public MessagePacket<?> decode(ByteBuf input) throws IOException {
        ByteBufInputStream stream = new ByteBufInputStream(input);
        try {
            return reader.readValue(stream);
        } finally {
            stream.close();
        }
    }
}
