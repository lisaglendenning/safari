package edu.uw.zookeeper.safari.control.schema;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@SuppressWarnings("rawtypes")
public class VolumeLogEntrySerializer extends StdSerializer<VolumeLogEntry> {

    public static VolumeLogEntrySerializer create() {
        return new VolumeLogEntrySerializer(VolumeLogEntryCoreSerializer.create());
    }

    protected final VolumeLogEntryCoreSerializer delegate;
    
    protected VolumeLogEntrySerializer(VolumeLogEntryCoreSerializer delegate) {
        super(delegate.handledType());
        this.delegate = delegate;
    }

    @Override
    public void serialize(VolumeLogEntry value, JsonGenerator json, SerializerProvider provider) throws JsonGenerationException, IOException {
        delegate.serialize(value, json);
    }
}
