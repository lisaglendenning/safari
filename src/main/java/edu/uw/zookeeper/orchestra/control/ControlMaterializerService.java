package edu.uw.zookeeper.orchestra.control;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.client.PingingClient;

@DependsOn(ControlConnectionsService.class)
public class ControlMaterializerService<C extends Connection<? super Operation.Request>> extends ClientConnectionExecutorService<C> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(ControlConnectionsService.module());
            TypeLiteral<ControlMaterializerService<?>> generic = new TypeLiteral<ControlMaterializerService<?>>(){};
            bind(ControlMaterializerService.class).to(generic);
            bind(generic).to(new TypeLiteral<ControlMaterializerService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>(){});
        }

        @Provides @Singleton
        public ControlMaterializerService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getControlClientService(
                ControlConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections,
                RuntimeModule runtime) {
            ControlMaterializerService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    ControlMaterializerService.newInstance(connections);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> ControlMaterializerService<C> newInstance(
            ControlConnectionsService<C> connections) {
        return new ControlMaterializerService<C>(connections);
    }

    protected final Materializer<Message.ClientRequest<?>, Message.ServerResponse<?>> materializer;
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
    
    public Materializer<Message.ClientRequest<?>, Message.ServerResponse<?>> materializer() {
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
