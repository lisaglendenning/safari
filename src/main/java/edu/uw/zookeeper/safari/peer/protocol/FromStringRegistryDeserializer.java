package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;

import edu.uw.zookeeper.data.Serializers;

public class FromStringRegistryDeserializer extends FromStringDeserializer<Object> {

    private static final long serialVersionUID = -8157879499774544500L;
    
    public FromStringRegistryDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    protected Object _deserialize(String value, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        return Serializers.getInstance().toClass(value, handledType());
    }

    @Override
    protected Object _deserializeFromEmptyString() throws IOException {
        return _deserialize("", null);
    }
}
