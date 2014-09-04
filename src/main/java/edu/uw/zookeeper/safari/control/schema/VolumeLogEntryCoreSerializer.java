package edu.uw.zookeeper.safari.control.schema;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.jackson.JacksonCoreSerializer;
import edu.uw.zookeeper.safari.schema.volumes.BoundVolumeOperator;

@SuppressWarnings("rawtypes")
public class VolumeLogEntryCoreSerializer implements JacksonCoreSerializer<VolumeLogEntry> {

    public static VolumeLogEntryCoreSerializer create() {
        return new VolumeLogEntryCoreSerializer();
    }

    protected VolumeLogEntryCoreSerializer() {}
    
    @Override
    public Class<VolumeLogEntry> handledType() {
        return VolumeLogEntry.class;
    }

    @Override
    public void serialize(VolumeLogEntry value, JsonGenerator jgen) throws JsonGenerationException, IOException {
        Object obj = value.get();
        if (obj instanceof BoundVolumeOperator<?>) {
            jgen.writeObject(obj);
        } else {
            jgen.writeString(((ZNodeName) obj).toString());
        }
    }
}
