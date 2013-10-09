package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class JacksonModule extends AbstractModule {
    
    public static JacksonModule create() {
        return new JacksonModule();
    }
    
    public JacksonModule() {}
    
    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public ObjectMapper getObjectMapper() {
        return ObjectMapperBuilder.defaults().build();
    }

    @Provides @Singleton
    public JacksonSerializer getJacksonSerializer(
            ObjectMapper mapper) {
        return JacksonSerializer.create(mapper);
    }
}
