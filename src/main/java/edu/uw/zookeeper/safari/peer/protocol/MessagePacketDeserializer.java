package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class MessagePacketDeserializer extends StdDeserializer<MessagePacket<?>> {

    private static final long serialVersionUID = 8870635328598391085L;

    public static MessagePacketDeserializer create() {
        return new MessagePacketDeserializer();
    }

    protected final MessagePacketCoreDeserializer delegate;

    public MessagePacketDeserializer() {
        this(MessagePacketCoreDeserializer.create());
    }
    
    protected MessagePacketDeserializer(MessagePacketCoreDeserializer delegate) {
        super(delegate.handledType());
        this.delegate = delegate;
    }

    @Override
    public MessagePacket<?> deserialize(JsonParser json, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        return delegate.deserialize(json);
    }
}
