package edu.uw.zookeeper.safari.peer.protocol;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.EnsembleRoleView;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerAddressView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Version;

public class ObjectMapperBuilder extends edu.uw.zookeeper.jackson.databind.ObjectMapperBuilder {

    public static ObjectMapperBuilder defaults() {
        return new ObjectMapperBuilder();
    }
    
    public ObjectMapperBuilder() {}

    @Override
    protected ObjectMapper getDefaultObjectMapper() {
        ObjectMapper mapper = super.getDefaultObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }
    
    @Override
    protected List<Module> getDefaultModules() {
        return ImmutableList.<Module>of(new JacksonModuleBuilder().build());
    }
    
    public static class JacksonModuleBuilder extends edu.uw.zookeeper.jackson.databind.JacksonModuleBuilder {

        public JacksonModuleBuilder() {}
        
        @Override
        protected String getDefaultProjectName() {
            return Version.getProjectName();
        }

        @Override
        protected com.fasterxml.jackson.core.Version getDefaultVersion() {
            edu.uw.zookeeper.Version version = Version.getDefault();
            return new com.fasterxml.jackson.core.Version(
                    version.getMajor(),
                    version.getMinor(),
                    version.getPatch(),
                    version.getLabel(),
                    Version.getGroup(),
                    Version.getArtifact());
        }
        
        @Override
        protected List<JsonSerializer<?>> getDefaultSerializers() {
            ImmutableList.Builder<JsonSerializer<?>> serializers = ImmutableList.builder();
            for (Class<?> cls: getRegistryClasses()) {
                serializers.add(ToStringRegistrySerializer.create(cls));
            }
            Serializers.getInstance().add(ServerAddressView.class);
            return serializers.build();
        }

        @Override
        protected Map<Class<?>, JsonDeserializer<?>> getDefaultDeserializers() {
            ImmutableMap.Builder<Class<?>, JsonDeserializer<?>> deserializers = ImmutableMap.builder();
            for (Class<?> cls: getRegistryClasses()) {
                deserializers.put(cls,
                    new FromStringRegistryDeserializer(cls));
            }
            return deserializers.build();
        }
        
        protected Class<?>[] getRegistryClasses() {
            Class<?>[] registryClasses = {
                    ZNodeLabel.class, 
                    ZNodeLabel.Component.class,
                    ZNodeLabel.Path.class,
                    ServerView.Address.class,
                    ServerInetAddressView.class,
                    EnsembleView.class,
                    EnsembleRoleView.class,
                    Identifier.class };
            return registryClasses;
        }
    }
}
