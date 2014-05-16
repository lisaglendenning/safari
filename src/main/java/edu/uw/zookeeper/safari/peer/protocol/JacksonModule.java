package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.data.Serializers;

public class JacksonModule extends AbstractModule {
    
    public static JacksonModule create() {
        return new JacksonModule();
    }
    
    public JacksonModule() {}
    
    @Override
    protected void configure() {
        bind(new TypeLiteral<Serializers.ByteCodec<Object>>(){}).to(JacksonSerializer.class);
    }

    @Provides @Singleton
    public ObjectMapper getObjectMapper() {
        return ObjectMapperBuilder.defaults().build();
    }
    
    @Provides
    public JacksonSerializer getJacksonSerializer(
            ObjectMapper mapper) {
        return JacksonSerializer.create(mapper);
    }
}
