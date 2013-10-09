package edu.uw.zookeeper.safari.control;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;
import edu.uw.zookeeper.safari.peer.protocol.JacksonSerializer;

@DependsOn(ControlConnectionsService.class)
public class ControlMaterializerService extends ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}
        
        @Provides @Singleton
        public ControlMaterializerService getControlMaterializerService(
                Injector injector,
                ObjectMapper mapper,
                ControlConnectionsService<?> connections) {
            return ControlMaterializerService.newInstance(
                    injector, JacksonSerializer.create(mapper), connections);
        }
        
        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(
                    JacksonModule.create(),
                    ControlConnectionsService.module());
        }
    }
    
    public static ControlMaterializerService newInstance(
            Injector injector,
            Serializers.ByteCodec<Object> serializer, 
            ControlConnectionsService<?> connections) {
        return new ControlMaterializerService(injector, serializer, connections);
    }

    protected final Injector injector;
    protected final Materializer<Message.ServerResponse<?>> materializer;
    protected final WatchPromiseTrie watches;

    protected ControlMaterializerService(
            Injector injector,
            Serializers.ByteCodec<Object> serializer, 
            ControlConnectionsService<?> connections) {
        super(connections.factory());
        this.injector = injector;
        this.materializer = Materializer.newInstance(
                        ControlSchema.getInstance().get(),
                        serializer,
                        this);
        this.watches = WatchPromiseTrie.newInstance();
    }
    
    public Materializer<Message.ServerResponse<?>> materializer() {
        return materializer;
    }

    public WatchPromiseTrie watches() {
        return watches;
    }
    
    @Override
    protected void startUp() throws Exception {
        DependentService.addOnStart(injector, this);
        
        super.startUp();

        this.register(watches);
        WatchEventPublisher.create(this, this);
        
        Control.createPrefix(materializer());
    }
}
