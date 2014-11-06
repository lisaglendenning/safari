package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;

import edu.uw.zookeeper.jackson.ListCoreSerializer;

public class MessagePacketCoreSerializer extends ListCoreSerializer<MessagePacket<?>> {

    public static MessagePacketCoreSerializer create() {
        return new MessagePacketCoreSerializer();
    }

    public MessagePacketCoreSerializer() {
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<MessagePacket> handledType() {
        return MessagePacket.class;
    }

    @Override
    protected void serializeValue(MessagePacket<?> value, JsonGenerator json) throws JsonGenerationException, IOException {
        json.writeNumber(value.getType().intValue());
        json.writeObject(value.getBody());
    }
}
