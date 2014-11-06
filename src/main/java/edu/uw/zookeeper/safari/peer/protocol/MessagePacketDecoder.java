package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import edu.uw.zookeeper.net.Decoder;
import edu.uw.zookeeper.protocol.LoggingMarker;

@SuppressWarnings("rawtypes")
public class MessagePacketDecoder implements Decoder<MessagePacket, MessagePacket> {

    public static MessagePacketDecoder defaults(ObjectMapper mapper) {
        return new MessagePacketDecoder(
                mapper.reader(), 
                LogManager.getLogger(MessagePacketDecoder.class));
    }
    
    protected final Logger logger;
    protected final ObjectReader reader;
    
    public MessagePacketDecoder(ObjectReader reader, Logger logger) {
        this.logger = checkNotNull(logger);
        this.reader = reader.withType(MessagePacket.class);
    }

    @Override
    public Class<? extends MessagePacket> decodeType() {
        return MessagePacket.class;
    }

    @Override
    public MessagePacket<?> decode(ByteBuf input) throws IOException {
        if (logger.isTraceEnabled()) {
            byte[] bytes = new byte[input.readableBytes()];
            input.getBytes(input.readerIndex(), bytes);
            logger.trace(LoggingMarker.PROTOCOL_MARKER.get(), "Decoding: {}", new String(bytes));
        }
        ByteBufInputStream stream = new ByteBufInputStream(input);
        try {
            return reader.readValue(stream);
        } finally {
            stream.close();
        }
    }
}
