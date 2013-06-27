package edu.uw.zookeeper.orchestra.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Codec;
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.Frame;

public class FramedMessagePacketCodec implements Codec<MessagePacket, Optional<MessagePacket>> {

    public static FramedMessagePacketCodec newInstance(MessagePacketCodec codec) {
        return new FramedMessagePacketCodec(codec);
    }
    
    protected final MessagePacketCodec codec;
    protected final Encoder<MessagePacket> encoder;
    protected final Decoder<Optional<MessagePacket>> decoder;
    
    public FramedMessagePacketCodec(MessagePacketCodec codec) {
        this.codec = codec;
        this.encoder = Frame.FramedEncoder.create(codec);
        this.decoder = Frame.FramedDecoder.create(
                Frame.FrameDecoder.getDefault(), 
                codec);
    }
    
    @Override
    public void encode(MessagePacket input, ByteBuf output)
            throws IOException {
        encoder.encode(input, output);
    }

    @Override
    public Optional<MessagePacket> decode(ByteBuf input) throws IOException {
        return decoder.decode(input);
    }
}
