package edu.uw.zookeeper.safari.control.schema;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

@SuppressWarnings("rawtypes")
public class VolumeLogEntryDeserializer extends StdDeserializer<VolumeLogEntry> {

    public static VolumeLogEntryDeserializer create() {
        return new VolumeLogEntryDeserializer(VolumeLogEntryCoreDeserializer.create());
    }

    private static final long serialVersionUID = 191925488788083157L;
    
    protected final VolumeLogEntryCoreDeserializer delegate;

    protected VolumeLogEntryDeserializer(VolumeLogEntryCoreDeserializer delegate) {
        super(delegate.handledType());
        this.delegate = delegate;
    }

    @Override
    public VolumeLogEntry deserialize(JsonParser json, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        return delegate.deserialize(json);
    }
    
    @Override
    public boolean isCachable() { 
        return true; 
    }
}
