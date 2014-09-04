package edu.uw.zookeeper.safari.control.schema;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.uw.zookeeper.safari.schema.volumes.BoundVolumeOperator;

@SuppressWarnings("rawtypes")
public class BoundVolumeOperatorDeserializer extends StdDeserializer<BoundVolumeOperator> {

    public static BoundVolumeOperatorDeserializer create() {
        return new BoundVolumeOperatorDeserializer(BoundVolumeOperatorCoreDeserializer.create());
    }

    private static final long serialVersionUID = 191925488788083157L;
    
    protected final BoundVolumeOperatorCoreDeserializer delegate;

    protected BoundVolumeOperatorDeserializer(BoundVolumeOperatorCoreDeserializer delegate) {
        super(delegate.handledType());
        this.delegate = delegate;
    }

    @Override
    public BoundVolumeOperator deserialize(JsonParser json, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        return delegate.deserialize(json);
    }
    
    @Override
    public boolean isCachable() { 
        return true; 
    }
}
