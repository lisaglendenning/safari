package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import edu.uw.zookeeper.net.Encoder;
import edu.uw.zookeeper.protocol.LoggingMarker;

@SuppressWarnings("rawtypes")
public class MessagePacketEncoder implements Encoder<MessagePacket, MessagePacket> {

    public static MessagePacketEncoder defaults(ObjectMapper mapper) {
        return new MessagePacketEncoder(
                mapper.writer(), 
                LogManager.getLogger(MessagePacketEncoder.class));
    }
    
    protected final Logger logger;
    protected final ObjectWriter writer;
    
    public MessagePacketEncoder(ObjectWriter writer, Logger logger) {
        this.logger = checkNotNull(logger);
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
        if (logger.isTraceEnabled()) {
            byte[] bytes = new byte[output.readableBytes()];
            output.getBytes(output.readerIndex(), bytes);
            logger.trace(LoggingMarker.PROTOCOL_MARKER.get(), "Encoded: {}", new String(bytes));
        }
    }
}
