package edu.uw.zookeeper.safari.peer.protocol;

import static org.junit.Assert.*;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.fasterxml.jackson.databind.ObjectMapper;

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
        Identifier id = Identifier.valueOf(1);
        MessagePacket<?> packet = MessagePacket.of(MessageHandshake.of(id));
        ByteBuf buf = Unpooled.buffer();
        encoder.encode(packet, buf);
        assertEquals(packet, decoder.decode(buf));
    }
}
