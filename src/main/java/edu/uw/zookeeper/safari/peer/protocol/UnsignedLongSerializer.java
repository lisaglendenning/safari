package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;
import java.lang.reflect.Type;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.primitives.UnsignedLong;

public class UnsignedLongSerializer extends StdSerializer<UnsignedLong> {

    public static UnsignedLongSerializer create() {
        return new UnsignedLongSerializer();
    }

    protected UnsignedLongSerializer() {
        super(UnsignedLong.class);
    }

    @Override
    public void serialize(UnsignedLong value, JsonGenerator json, SerializerProvider provider) throws JsonGenerationException, IOException {
        json.writeNumber(value.longValue());
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
        return createSchemaNode("number");
    }
}
