package edu.uw.zookeeper.orchestra.control;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.proto.Records;

@DependsOn(ControlConnectionsService.class)
public class ControlMaterializerService<C extends Connection<? super Operation.Request>> extends ClientConnectionExecutorService<C> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(getControlConnectionsModule());
            TypeLiteral<ControlMaterializerService<?>> generic = new TypeLiteral<ControlMaterializerService<?>>(){};
            bind(ControlMaterializerService.class).to(generic);
        }

        @Provides @Singleton
        public ControlMaterializerService<?> getControlClientService(
                ControlConnectionsService<?> connections,
                DependentServiceMonitor monitor) {
            return monitor.listen(ControlMaterializerService.newInstance(connections));
        }
        
        protected com.google.inject.Module getControlConnectionsModule() {
            return ControlConnectionsService.module();
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> ControlMaterializerService<C> newInstance(
            ControlConnectionsService<C> connections) {
        return new ControlMaterializerService<C>(connections);
    }

    protected final Materializer<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> materializer;
    protected final WatchPromiseTrie watches;

    protected ControlMaterializerService(
            ControlConnectionsService<C> connections) {
        super(connections);
        this.materializer = Materializer.newInstance(
                        Control.getSchema(),
                        Control.getByteCodec(),
                        this, 
                        this);
        this.watches = WatchPromiseTrie.newInstance();
    }
    
    public Materializer<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> materializer() {
        return materializer;
    }
    
    public EnsembleView<ServerInetAddressView> view() {
        return factory().view();
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
