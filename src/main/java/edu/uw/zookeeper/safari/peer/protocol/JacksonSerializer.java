package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.Serializes;

public class JacksonSerializer implements Serializers.ByteCodec<Object> {

    public static JacksonSerializer create(ObjectMapper mapper) {
        return new JacksonSerializer(mapper);
    }
    
    protected final ObjectMapper mapper;

    protected JacksonSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    @Serializes(from=byte[].class, to=Object.class)
    public <T> T fromBytes(byte[] input, Class<T> type) throws IOException {
        return mapper.readValue(input, type);
    }
    
    @Override
    @Serializes(from=Object.class, to=byte[].class)
    public byte[] toBytes(Object input) throws JsonProcessingException {
        return mapper.writeValueAsBytes(input);
    }

    @Serializes(from=String.class, to=Object.class)
    public <T> T fromString(String input, Class<T> type) throws IOException {
        return mapper.readValue(input, type);
    }
    
    @Serializes(from=Object.class, to=String.class)
    public String toString(Object input) throws JsonProcessingException {
        return mapper.writeValueAsString(input);
    }
}