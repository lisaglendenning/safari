package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.ServiceMonitor;

public class BackendConnectionsService<C extends Connection<? super Operation.Request>> extends AbstractIdleService implements Factory<C> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(BackendConfiguration.module());
            TypeLiteral<BackendConnectionsService<?>> generic = new TypeLiteral<BackendConnectionsService<?>>() {};
            bind(BackendConnectionsService.class).to(generic);
            bind(generic).to(new TypeLiteral<BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>() {});
        }

        @Provides @Singleton
        public BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getBackendConnectionService(
                BackendConfiguration configuration,
                NettyClientModule clientModule,
                ScheduledExecutorService executor,
                ServiceMonitor monitor) throws Exception {
            ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = ClientApplicationModule.codecFactory();
            ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> pingingFactory = 
                    PingingClient.factory(configuration.getTimeOut(), executor);
            ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections = 
                   clientModule.get(codecFactory, pingingFactory).get();
            monitor.addOnStart(connections);
            ServerViewFactory.FixedClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                    ServerViewFactory.FixedClientConnectionFactory.create(configuration.getView().getClientAddress().get(), connections);
            BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    BackendConnectionsService.newInstance(clientConnections);
            monitor.addOnStart(instance);
            return instance;
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> BackendConnectionsService<C> newInstance(
            ServerViewFactory.FixedClientConnectionFactory<C> connections) {
        return new BackendConnectionsService<C>(connections);
    }

    protected final ServerViewFactory.FixedClientConnectionFactory<C> connections;
    protected final ZxidTracker zxids;
    
    protected BackendConnectionsService(
            ServerViewFactory.FixedClientConnectionFactory<C> connections) {
        this.connections = connections;
        this.zxids = ZxidTracker.create();
    }

    public ZxidTracker zxids() {
        return zxids;
    }
    
    @Override
    public C get() {
        C client = connections.get();
        ZxidTracker.ZxidListener.create(zxids, client);
        return client;
    }
    
    @Override
    protected void startUp() throws Exception {
        connections.second().start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        connections.second().stop().get();
    }
}
