package edu.uw.zookeeper.orchestra.backend;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.common.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ZxidTracker;

public class BackendConnectionsService<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends ForwardingService implements Factory<ListenableFuture<C>>, Function<C,C> {

    public static com.google.inject.Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}
        
        @Override
        protected void configure() {
            super.configure();
            bind(BackendConnectionsService.class).to(new TypeLiteral<BackendConnectionsService<?>>() {}).in(Singleton.class);
        }

        @Provides @Singleton
        public BackendConnectionsService<?> getBackendConnectionService(
                RuntimeModule runtime,
                BackendConfiguration configuration,
                NetClientModule clientModule,
                ListeningExecutorServiceFactory executors) throws Exception {
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections = getClientConnectionFactory(runtime, configuration, clientModule);
            BackendConnectionsService<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    BackendConnectionsService.newInstance(configuration, connections);
            return instance;
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(BackendConfiguration.module());
        }

        protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionFactory(                
                RuntimeModule runtime,
                BackendConfiguration configuration,
                NetClientModule clientModule) {
            return ClientConnectionFactoryBuilder.defaults()
                    .setClientModule(clientModule)
                    .setTimeOut(configuration.getTimeOut())
                    .setRuntimeModule(runtime)
                    .build();
        }
    }
    
    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> BackendConnectionsService<C> newInstance(
            BackendConfiguration configuration,
            ClientConnectionFactory<C> connections) {
        FixedClientConnectionFactory<C> factory = 
                FixedClientConnectionFactory.create(
                        configuration.getView().getClientAddress().get(), connections);
        return new BackendConnectionsService<C>(factory);
    }

    protected final FixedClientConnectionFactory<C> connections;
    protected final ZxidTracker zxids;
    
    protected BackendConnectionsService(
            FixedClientConnectionFactory<C> connections) {
        this.connections = connections;
        this.zxids = ZxidTracker.create();
    }

    public FixedClientConnectionFactory<C> connections() {
        return connections;
    }
    
    public ZxidTracker zxids() {
        return zxids;
    }
    
    @Override
    public ListenableFuture<C> get() {
        return Futures.transform(connections.get(), this);
    }
    
    @Override
    public C apply(C input) {
        ZxidTracker.ZxidListener.create(zxids, input);
        return input;
    }
    
    @Override
    protected Service delegate() {
        return connections.second();
    }
}
