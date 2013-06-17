package edu.uw.zookeeper.orchestra.proto;

import static org.junit.Assert.*;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.control.JacksonModule;

@RunWith(JUnit4.class)
public class FramedMessagePacketCodecTest {
    @Test
    public void test() throws IOException {
        ObjectMapper mapper = JacksonModule.getMapper();
        FramedMessagePacketCodec codec = FramedMessagePacketCodec.newInstance(MessagePacketCodec.newInstance(mapper));
        Identifier id = Identifier.valueOf(1);
        MessagePacket packet = MessagePacket.of(HandshakeMessage.of(id));
        ByteBuf buf = Unpooled.buffer();
        codec.encode(packet, buf);
        assertEquals(packet, codec.decode(buf));
    }
}
