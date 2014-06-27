package edu.uw.zookeeper.safari.control.schema;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;

import edu.uw.zookeeper.jackson.ListCoreSerializer;
import edu.uw.zookeeper.safari.volume.BoundVolumeOperator;

public class BoundVolumeOperatorCoreSerializer extends ListCoreSerializer<BoundVolumeOperator<?>> {

    public static BoundVolumeOperatorCoreSerializer create() {
        return new BoundVolumeOperatorCoreSerializer();
    }

    protected BoundVolumeOperatorCoreSerializer() {}
    
    @SuppressWarnings("rawtypes")
    @Override
    public Class<BoundVolumeOperator> handledType() {
        return BoundVolumeOperator.class;
    }

    @Override
    protected void serializeValue(BoundVolumeOperator<?> value, JsonGenerator jgen) throws JsonGenerationException, IOException {
        jgen.writeString(value.getOperator().name());
        jgen.writeObject(value.getParameters());
    }
}
