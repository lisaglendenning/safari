package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.jackson.ListCoreDeserializer;

public final class MessagePacketCoreDeserializer extends ListCoreDeserializer<MessagePacket<?>> {

    public static MessagePacketCoreDeserializer create() {
        return new MessagePacketCoreDeserializer();
    }

    private final Map<MessageType, Class<? extends MessageBody>> types;
    
    public MessagePacketCoreDeserializer() {
        super();
        ImmutableMap.Builder<MessageType, Class<? extends MessageBody>> types = ImmutableMap.builder();
        for (Map.Entry<MessageType, Class<? extends MessageBody>> e: MessageTypes.registeredTypes()) {
            types.put(e);
        }
        this.types = types.build();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<MessagePacket> handledType() {
        return MessagePacket.class;
    }

    @Override
    protected MessagePacket<?> deserializeValue(JsonParser json)
            throws IOException, JsonProcessingException {
        if (!json.hasCurrentToken()) {
            json.nextToken();
        }
        MessageType type = MessageType.valueOf(json.getIntValue());
        json.nextToken();
        MessageBody body = json.readValueAs(types.get(type));
        json.clearCurrentToken();
        return MessagePacket.valueOf(type, body);
    }
}
