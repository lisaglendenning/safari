package edu.uw.zookeeper.orchestra.proto;

import static org.junit.Assert.*;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.protocol.HandshakeMessage;
import edu.uw.zookeeper.orchestra.protocol.JacksonModule;
import edu.uw.zookeeper.orchestra.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.protocol.MessagePacketCodec;

@RunWith(JUnit4.class)
public class MessagePacketCodecTest {
    @Test
    public void test() throws IOException {
        ObjectMapper mapper = JacksonModule.getMapper();
        MessagePacketCodec codec = MessagePacketCodec.newInstance(mapper);
        Identifier id = Identifier.valueOf(1);
        MessagePacket packet = MessagePacket.of(HandshakeMessage.of(id));
        ByteBuf buf = Unpooled.buffer();
        codec.encode(packet, buf);
        assertEquals(packet, codec.decode(buf));
    }
}
