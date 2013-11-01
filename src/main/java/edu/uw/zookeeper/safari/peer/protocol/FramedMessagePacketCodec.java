package edu.uw.zookeeper.safari.peer.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Optional;

import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.Decoder;
import edu.uw.zookeeper.net.Encoder;
import edu.uw.zookeeper.protocol.Frame;

@SuppressWarnings("rawtypes")
public class FramedMessagePacketCodec implements Codec<MessagePacket, MessagePacket, MessagePacket, MessagePacket> {

    public static FramedMessagePacketCodec defaults(ObjectMapper mapper) {
        ObjectWriter writer = mapper.writer();
        ObjectReader reader = mapper.reader();
        Logger logger = LogManager.getLogger(FramedMessagePacketCodec.class);
        Encoder<? super MessagePacket, MessagePacket> encoder = new MessagePacketEncoder(writer, logger);
        Decoder<? extends MessagePacket, MessagePacket> decoder = new MessagePacketDecoder(reader, logger);
        return new FramedMessagePacketCodec(encoder, decoder);
    }
    
    protected final Encoder<? super MessagePacket, MessagePacket> encoder;
    protected final Decoder<? extends Optional<? extends MessagePacket>, MessagePacket> decoder;
    
    public FramedMessagePacketCodec(
            Encoder<? super MessagePacket, MessagePacket> encoder,
            Decoder<? extends MessagePacket, MessagePacket> decoder) {
        this.encoder = Frame.FramedEncoder.create(encoder);
        this.decoder = Frame.FramedDecoder.create(
                Frame.FrameDecoder.getDefault(), 
                decoder);
    }

    @Override
    public Class<? extends MessagePacket> encodeType() {
        return encoder.encodeType();
    }

    @Override
    public void encode(MessagePacket input, ByteBuf output)
            throws IOException {
        encoder.encode(input, output);
    }

    @Override
    public Class<? extends MessagePacket> decodeType() {
        return decoder.decodeType();
    }

    @Override
    public Optional<? extends MessagePacket> decode(ByteBuf input) throws IOException {
        return decoder.decode(input);
    }
}
