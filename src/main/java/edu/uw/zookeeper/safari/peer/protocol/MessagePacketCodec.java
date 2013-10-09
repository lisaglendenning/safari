package edu.uw.zookeeper.safari.peer.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.protocol.Codec;
import edu.uw.zookeeper.protocol.LoggingMarker;

public class MessagePacketCodec implements Codec<MessagePacket, MessagePacket> {

    public static MessagePacketCodec newInstance(ObjectMapper mapper) {
        return new MessagePacketCodec(mapper);
    }
    
    static {
        for (Class<? extends MessageBody> cls: ImmutableList.of(
                MessageHandshake.class,
                MessageHeartbeat.class,
                MessageSessionOpenRequest.class,
                MessageSessionOpenResponse.class,
                MessageSessionRequest.class,
                MessageSessionResponse.class)) {
            MessageTypes.register(cls);
        }
    }
    
    protected final Logger logger;
    protected final ObjectMapper mapper;
    
    public MessagePacketCodec(ObjectMapper mapper) {
        this.logger = LogManager.getLogger(getClass());
        this.mapper = mapper;
    }
    
    @Override
    public void encode(MessagePacket input, ByteBuf output)
            throws IOException {
        input.first().encode(output);
        ByteBufOutputStream stream = new ByteBufOutputStream(output);
        try {
            mapper.writeValue(stream, input.second());
        } finally {
            stream.close();
        }
        if (logger.isTraceEnabled()) {
            byte[] bytes = new byte[output.readableBytes()];
            output.getBytes(output.readerIndex(), bytes);
            logger.trace(LoggingMarker.PROTOCOL_MARKER.get(), "Encoded: {}", new String(bytes));
        }
    }

    @Override
    public MessagePacket decode(ByteBuf input) throws IOException {
        if (logger.isTraceEnabled()) {
            byte[] bytes = new byte[input.readableBytes()];
            input.getBytes(input.readerIndex(), bytes);
            logger.trace(LoggingMarker.PROTOCOL_MARKER.get(), "Decoding: {}", new String(bytes));
        }
        ByteBufInputStream stream = new ByteBufInputStream(input);
        try {
            MessageHeader header = MessageHeader.decode(input);
            Class<? extends MessageBody> bodyType = MessageTypes.registeredType(header.type());
            MessageBody body = mapper.readValue(stream, bodyType);
            MessagePacket message = MessagePacket.of(header, body);
            return message;
        } finally {
            stream.close();
        }
    }
}
