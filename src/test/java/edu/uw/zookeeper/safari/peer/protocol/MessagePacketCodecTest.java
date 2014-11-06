package edu.uw.zookeeper.safari.peer.protocol;

import static org.junit.Assert.*;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.protocol.MessageHandshake;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;

@RunWith(JUnit4.class)
public class MessagePacketCodecTest {
    @Test
    public void test() throws IOException {
        ObjectMapper mapper = ObjectMapperBuilder.defaults().build();
        MessagePacketEncoder encoder = MessagePacketEncoder.defaults(mapper);
        MessagePacketDecoder decoder = MessagePacketDecoder.defaults(mapper);
        for (MessagePacket<?> packet: ImmutableList.of(
                MessagePacket.valueOf(MessageHeartbeat.getInstance()),
                MessagePacket.valueOf(MessageHandshake.of(Identifier.valueOf(1))))) {
            encodeAndDecode(encoder, decoder, packet);
        }
    }
    
    protected void encodeAndDecode(
            final MessagePacketEncoder encoder, 
            final MessagePacketDecoder decoder,
            final MessagePacket<?> packet) throws IOException {
        ByteBuf buf = Unpooled.buffer();
        encoder.encode(packet, buf);
        assertEquals(packet, decoder.decode(buf));
    }
}
