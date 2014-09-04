package edu.uw.zookeeper.safari.control.schema;

import java.io.IOException;
import java.lang.reflect.Type;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import edu.uw.zookeeper.safari.schema.volumes.BoundVolumeOperator;

@SuppressWarnings("rawtypes")
public class BoundVolumeOperatorSerializer extends StdSerializer<BoundVolumeOperator> {

    public static BoundVolumeOperatorSerializer create() {
        return new BoundVolumeOperatorSerializer(BoundVolumeOperatorCoreSerializer.create());
    }

    protected final BoundVolumeOperatorCoreSerializer delegate;
    
    protected BoundVolumeOperatorSerializer(BoundVolumeOperatorCoreSerializer delegate) {
        super(delegate.handledType());
        this.delegate = delegate;
    }

    @Override
    public void serialize(BoundVolumeOperator value, JsonGenerator json, SerializerProvider provider) throws JsonGenerationException, IOException {
        delegate.serialize(value, json);
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
        return createSchemaNode("array");
    }
}
