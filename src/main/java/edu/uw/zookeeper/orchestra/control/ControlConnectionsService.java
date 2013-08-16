package edu.uw.zookeeper.orchestra.control;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.AbstractMain.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.orchestra.ClientConnectionsModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;

class ControlConnectionsService<C extends Connection<? super Operation.Request>> extends ForwardingService implements Factory<ListenableFuture<ClientConnectionExecutor<C>>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends ClientConnectionsModule {

        protected Module() {}
        
        @Override
        protected void configure() {
            super.configure();
            TypeLiteral<ControlConnectionsService<?>> generic = new TypeLiteral<ControlConnectionsService<?>>(){};
            bind(ControlConnectionsService.class).to(generic);
        }

        @Provides @Singleton
        public ControlConnectionsService<?> getControlConnectionsService(
                ControlConfiguration configuration,
                ListeningExecutorServiceFactory executors,
                NetClientModule clients) {
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                    getClientConnectionFactory(configuration.getTimeOut(), executors, clients);
            ControlConnectionsService<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    ControlConnectionsService.newInstance(configuration, clientConnections);
            return instance;
        }

        @Override
        protected com.google.inject.Module[] getModules() {
            com.google.inject.Module[] modules = { ControlConfiguration.module()};
            return modules;
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> ControlConnectionsService<C> newInstance(
            ControlConfiguration configuration,
            ClientConnectionFactory<C> connections) {
        EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, C>> factory = 
                EnsembleViewFactory.newInstance(
                        connections,
                        ServerInetAddressView.class, 
                        configuration.getEnsemble(), 
                        configuration.getTimeOut());
        return new ControlConnectionsService<C>(connections, factory);
    }

    protected final ClientConnectionFactory<C> connections;
    protected final EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, C>> factory;
    
    protected ControlConnectionsService(
            ClientConnectionFactory<C> connections,
            EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, C>> factory) {
        this.connections = connections;
        this.factory = factory;
    }
    
    public ClientConnectionFactory<C> connections() {
        return connections;
    }

    @Override
    public ListenableFuture<ClientConnectionExecutor<C>> get() {
        return factory.get().get();
    }

    @Override
    protected Service delegate() {
        return connections;
    }
}