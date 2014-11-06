package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;
import java.lang.reflect.Type;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class MessagePacketSerializer extends StdSerializer<MessagePacket<?>> {

    public static MessagePacketSerializer create() {
        return new MessagePacketSerializer();
    }

    protected final MessagePacketCoreSerializer delegate;

    public MessagePacketSerializer() {
        this(MessagePacketCoreSerializer.create());
    }
    
    protected MessagePacketSerializer(MessagePacketCoreSerializer delegate) {
        super(delegate.handledType(), true);
        this.delegate = delegate;
    }

    @Override
    public void serialize(MessagePacket<?> value, JsonGenerator json, SerializerProvider provider) throws JsonGenerationException, IOException {
        delegate.serialize(value, json);
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
        return createSchemaNode("array");
    }
}
