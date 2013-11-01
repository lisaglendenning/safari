package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableMap;

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
    protected final Map<MessageType, ObjectReader> readers;
    
    public MessagePacketDecoder(ObjectReader reader, Logger logger) {
        this.logger = checkNotNull(logger);
        ImmutableMap.Builder<MessageType, ObjectReader> readers = ImmutableMap.builder();
        for (Map.Entry<MessageType, Class<? extends MessageBody>> e: MessageTypes.registeredTypes()) {
            readers.put(e.getKey(), reader.withType(e.getValue()));
        }
        this.readers = readers.build();
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
            MessageHeader header = MessageHeader.decode(input);
            MessageBody body = readers.get(header.type()).readValue(stream);
            MessagePacket<?> message = MessagePacket.of(header, body);
            return message;
        } finally {
            stream.close();
        }
    }
}
