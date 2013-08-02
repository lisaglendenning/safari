package edu.uw.zookeeper.orchestra.control;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.PingingClient;

class ControlConnectionsService<C extends Connection<? super Operation.Request>> extends AbstractIdleService implements Factory<ClientConnectionExecutor<C>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(getControlConfigurationModule());
            TypeLiteral<ControlConnectionsService<?>> generic = new TypeLiteral<ControlConnectionsService<?>>(){};
            bind(ControlConnectionsService.class).to(generic);
        }

        @Provides @Singleton
        public ControlConnectionsService<?> getControlClientConnectionFactory(
                ControlConfiguration configuration,
                ServiceMonitor serviceMonitor,
                ScheduledExecutorService scheduled,
                NettyClientModule clientModule) {
            ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = ClientApplicationModule.codecFactory();
            ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> pingingFactory = 
                    PingingClient.factory(configuration.getTimeOut(), scheduled);
            ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                    clientModule.get(codecFactory, pingingFactory).get();
            serviceMonitor.addOnStart(clientConnections);
            ControlConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    ControlConnectionsService.newInstance(clientConnections, configuration);
            serviceMonitor.addOnStart(instance);
            return instance;
        }
        
        protected com.google.inject.Module getControlConfigurationModule() {
            return ControlConfiguration.module();
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> ControlConnectionsService<C> newInstance(
            ClientConnectionFactory<C> clientConnections,
            ControlConfiguration configuration) {
        EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, C>> factory = 
                EnsembleViewFactory.newInstance(
                    clientConnections,
                    ServerInetAddressView.class, 
                    configuration.getEnsemble(), 
                    configuration.getTimeOut());
        return new ControlConnectionsService<C>(clientConnections, factory);
    }

    protected final ClientConnectionFactory<C> clientConnections;
    protected final EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, C>> factory;
    
    protected ControlConnectionsService(
            ClientConnectionFactory<C> clientConnections,
            EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, C>> factory) {
        this.clientConnections = clientConnections;
        this.factory = factory;
    }
    
    public ClientConnectionFactory<C> clientConnections() {
        return clientConnections;
    }
    
    public EnsembleView<ServerInetAddressView> view() {
        return factory.view();
    }
    
    @Override
    public ClientConnectionExecutor<C> get() {
        return factory.get().get();
    }

    @Override
    protected void startUp() throws Exception {
        clientConnections.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        clientConnections.stop().get();
    }
}