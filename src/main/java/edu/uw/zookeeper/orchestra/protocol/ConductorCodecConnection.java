package edu.uw.zookeeper.orchestra.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ConductorCodecConnection extends CodecConnection<MessagePacket, MessagePacket, FramedMessagePacketCodec> {

    public static ParameterizedFactory<Publisher, Pair<Class<MessagePacket>, FramedMessagePacketCodec>> codecFactory(
            final ObjectMapper mapper) {
        return new ParameterizedFactory<Publisher, Pair<Class<MessagePacket>, FramedMessagePacketCodec>>() {
            @Override
            public Pair<Class<MessagePacket>, FramedMessagePacketCodec> get(
                    Publisher value) {
                return Pair.create(MessagePacket.class, FramedMessagePacketCodec.newInstance(MessagePacketCodec.newInstance(mapper)));
            }
        };
    }

    public static ParameterizedFactory<Pair<Pair<Class<MessagePacket>, FramedMessagePacketCodec>, Connection<MessagePacket>>, ConductorCodecConnection> factory() {
        return new ParameterizedFactory<Pair<Pair<Class<MessagePacket>, FramedMessagePacketCodec>, Connection<MessagePacket>>, ConductorCodecConnection>() {
            @Override
            public ConductorCodecConnection get(
                    Pair<Pair<Class<MessagePacket>, FramedMessagePacketCodec>, Connection<MessagePacket>> value) {
                return new ConductorCodecConnection(value.first().second(), value.second());
            }
        };
    }
    
    protected ConductorCodecConnection(FramedMessagePacketCodec codec, Connection<MessagePacket> connection) {
        super(codec, connection);
    }
}
