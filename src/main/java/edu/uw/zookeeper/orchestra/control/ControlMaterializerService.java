package edu.uw.zookeeper.orchestra.control;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.peer.protocol.JacksonModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;

@DependsOn(ControlConnectionsService.class)
public class ControlMaterializerService extends ClientConnectionExecutorService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}
        
        @Override
        protected void configure() {
            super.configure();
        }

        @Provides @Singleton
        public ControlMaterializerService getControlClientService(
                ControlConnectionsService<?> connections) {
            return ControlMaterializerService.newInstance(connections);
        }
        
        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(ControlConnectionsService.module());
        }
    }
    
    public static ControlMaterializerService newInstance(
            ControlConnectionsService<?> connections) {
        return new ControlMaterializerService(connections);
    }

    protected final Materializer<Message.ServerResponse<?>> materializer;
    protected final WatchPromiseTrie watches;

    protected ControlMaterializerService(
            ControlConnectionsService<?> connections) {
        super(connections);
        this.materializer = Materializer.newInstance(
                        ControlSchema.getInstance().get(),
                        JacksonModule.getSerializer(),
                        this, 
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
        Service factory = (Service) this.factory;
        switch (factory.state()) {
        case NEW:
            factory.startAsync();
        case STARTING:
            factory.awaitRunning();
        case RUNNING:
            break;
        case TERMINATED:
        case STOPPING:
        case FAILED:
            throw new IllegalStateException();
        }
        
        super.startUp();

        this.register(watches);
        WatchEventPublisher.create(this, this);
        
        Control.createPrefix(materializer());
    }
}
