package edu.uw.zookeeper.orchestra.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.protocol.Codec;

public class MessagePacketCodec implements Codec<MessagePacket, MessagePacket> {

    public static MessagePacketCodec newInstance(ObjectMapper mapper) {
        return new MessagePacketCodec(mapper);
    }
    
    static {
        for (Class<? extends MessageBody> cls: ImmutableList.of(
                HandshakeMessage.class, SessionMessage.class)) {
            MessageBody.register(cls);
        }
    }
    
    protected final ObjectMapper mapper;
    
    public MessagePacketCodec(ObjectMapper mapper) {
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
    }

    @Override
    public MessagePacket decode(ByteBuf input) throws IOException {
        ByteBufInputStream stream = new ByteBufInputStream(input);
        try {
            MessageHeader first = MessageHeader.decode(input);
            Class<? extends MessageBody> bodyType = MessageBody.registeredType(first.type());
            MessageBody second = mapper.readValue(stream, bodyType);
            return MessagePacket.of(first, second);
        } finally {
            stream.close();
        }
    }
}
