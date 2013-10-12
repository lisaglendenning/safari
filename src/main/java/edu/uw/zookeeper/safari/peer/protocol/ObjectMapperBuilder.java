package edu.uw.zookeeper.safari.peer.protocol;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.EnsembleRoleView;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerAddressView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.clients.trace.ProtocolResponseHeaderDeserializer;
import edu.uw.zookeeper.clients.trace.ProtocolResponseHeaderSerializer;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.jackson.databind.ProtocolRequestDeserializer;
import edu.uw.zookeeper.jackson.databind.ProtocolRequestSerializer;
import edu.uw.zookeeper.jackson.databind.RequestRecordDeserializer;
import edu.uw.zookeeper.jackson.databind.RequestRecordSerializer;
import edu.uw.zookeeper.jackson.databind.ResponseRecordDeserializer;
import edu.uw.zookeeper.jackson.databind.ResponseRecordSerializer;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Version;

public class ObjectMapperBuilder extends edu.uw.zookeeper.jackson.databind.ObjectMapperBuilder {

    public static ObjectMapperBuilder defaults() {
        return new ObjectMapperBuilder();
    }
    
    public ObjectMapperBuilder() {}

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
            Serializers.getInstance().add(ServerAddressView.class);
            ImmutableList.Builder<JsonSerializer<?>> serializers = ImmutableList.builder();
            for (Class<?> cls: getRegistryClasses()) {
                serializers.add(ToStringRegistrySerializer.create(cls));
            }
            serializers
                .add(RequestRecordSerializer.create())
                .add(ResponseRecordSerializer.create())
                .add(ProtocolRequestSerializer.create())
                .add(ProtocolResponseHeaderSerializer.create());
            return serializers.build();
        }

        @Override
        protected Map<Class<?>, JsonDeserializer<?>> getDefaultDeserializers() {
            ImmutableMap.Builder<Class<?>, JsonDeserializer<?>> deserializers = ImmutableMap.builder();
            for (Class<?> cls: getRegistryClasses()) {
                deserializers.put(cls,
                    new FromStringRegistryDeserializer(cls));
            }
            deserializers
                .put(Records.Request.class, RequestRecordDeserializer.create())
                .put(Records.Response.class, ResponseRecordDeserializer.create())
                .put(Message.ClientRequest.class, ProtocolRequestDeserializer.create())
                .put(Message.ServerResponse.class, ProtocolResponseHeaderDeserializer.create());
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