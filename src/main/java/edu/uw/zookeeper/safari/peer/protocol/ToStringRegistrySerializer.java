package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;

import edu.uw.zookeeper.data.Serializers;

public class ToStringRegistrySerializer<V> extends StdScalarSerializer<V> {

    public static <V> ToStringRegistrySerializer<V> create(Class<V> vc) {
        return new ToStringRegistrySerializer<V>(vc);
    }
    
    public ToStringRegistrySerializer(Class<V> vc) { 
        super(vc); 
    }

    @Override
    public void serialize(
            Object value, 
            JsonGenerator jgen, 
            SerializerProvider provider) throws IOException,
            JsonGenerationException {
        String stringOf = Serializers.ToString.TO_STRING.apply(value);
        jgen.writeString(stringOf);
    }
    
}