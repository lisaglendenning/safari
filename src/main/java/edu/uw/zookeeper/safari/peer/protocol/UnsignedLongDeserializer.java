package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.primitives.UnsignedLong;

public class UnsignedLongDeserializer extends StdDeserializer<UnsignedLong> {

    public static UnsignedLongDeserializer create() {
        return new UnsignedLongDeserializer();
    }
    
    private static final long serialVersionUID = 1L;

    protected UnsignedLongDeserializer() {
        super(UnsignedLong.class);
    }

    @Override
    public UnsignedLong deserialize(JsonParser json, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        return UnsignedLong.valueOf(json.getLongValue());
    }
    
    @Override
    public boolean isCachable() { 
        return true; 
    }
}
