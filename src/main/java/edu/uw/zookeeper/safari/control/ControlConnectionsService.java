package edu.uw.zookeeper.safari.control;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.common.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.safari.common.DependentModule;

public class ControlConnectionsService<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends ForwardingService implements Factory<ListenableFuture<ClientConnectionExecutor<C>>> {

    public static com.google.inject.Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        protected Module() {}
        
        @Override
        protected void configure() {
            super.configure();
            bind(ControlConnectionsService.class).to(new TypeLiteral<ControlConnectionsService<?>>(){}).in(Singleton.class);
        }

        @Provides @Singleton
        public ControlConnectionsService<?> getControlConnectionsService(
                RuntimeModule runtime,
                ControlConfiguration configuration,
                ListeningExecutorServiceFactory executors,
                NetClientModule clientModule,
                ScheduledExecutorService executor) {
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections = 
                    getClientConnectionFactory(runtime, configuration, clientModule);
            ControlConnectionsService<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    ControlConnectionsService.newInstance(configuration, connections, executor);
            return instance;
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(ControlConfiguration.module());
        }
        
        protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionFactory(                
                RuntimeModule runtime,
                ControlConfiguration configuration,
                NetClientModule clientModule) {
            return ClientConnectionFactoryBuilder.defaults()
                    .setClientModule(clientModule)
                    .setTimeOut(configuration.getTimeOut())
                    .setRuntimeModule(runtime)
                    .build();
        }
    }
    
    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ControlConnectionsService<C> newInstance(
            ControlConfiguration configuration,
            ClientConnectionFactory<C> connections,
            ScheduledExecutorService executor) {
        EnsembleViewFactory<ServerViewFactory<Session, ClientConnectionExecutor<C>>> factory = 
                EnsembleViewFactory.fromSession(
                        connections,
                        configuration.getEnsemble(), 
                        configuration.getTimeOut(),
                        executor);
        return new ControlConnectionsService<C>(connections, factory);
    }

    protected final ClientConnectionFactory<C> connections;
    protected final EnsembleViewFactory<ServerViewFactory<Session, ClientConnectionExecutor<C>>> factory;
    
    protected ControlConnectionsService(
            ClientConnectionFactory<C> connections,
            EnsembleViewFactory<ServerViewFactory<Session, ClientConnectionExecutor<C>>> factory) {
        this.connections = connections;
        this.factory = factory;
    }
    
    public ClientConnectionFactory<C> connections() {
        return connections;
    }
    
    public EnsembleViewFactory<ServerViewFactory<Session, ClientConnectionExecutor<C>>> factory() {
        return factory;
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