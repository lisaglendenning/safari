package edu.uw.zookeeper.orchestra.control;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.peer.protocol.JacksonModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;

@DependsOn(ControlConnectionsService.class)
public class ControlMaterializerService<C extends Connection<? super Operation.Request>> extends ClientConnectionExecutorService<C> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}
        
        @Override
        protected void configure() {
            super.configure();
            TypeLiteral<ControlMaterializerService<?>> generic = new TypeLiteral<ControlMaterializerService<?>>(){};
            bind(ControlMaterializerService.class).to(generic);
        }

        @Provides @Singleton
        public ControlMaterializerService<?> getControlClientService(
                ControlConnectionsService<?> connections) {
            return ControlMaterializerService.newInstance(connections);
        }
        
        @Override
        protected com.google.inject.Module[] getModules() {
            com.google.inject.Module[] modules = { ControlConnectionsService.module() };
            return modules;
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> ControlMaterializerService<C> newInstance(
            ControlConnectionsService<C> connections) {
        return new ControlMaterializerService<C>(connections);
    }

    protected final Materializer<Message.ServerResponse<?>> materializer;
    protected final WatchPromiseTrie watches;

    protected ControlMaterializerService(
            ControlConnectionsService<C> connections) {
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
    protected ControlConnectionsService<C> factory() {
        return (ControlConnectionsService<C>) factory;
    }

    @Override
    protected void startUp() throws Exception {
        factory().start().get();
        
        super.startUp();

        this.register(watches);
        WatchEventPublisher.create(this, this);
        
        Control.createPrefix(materializer());
    }
}
