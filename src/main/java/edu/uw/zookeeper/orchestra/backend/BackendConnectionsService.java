package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.protocol.client.ZxidTracker;

public class BackendConnectionsService<C extends Connection<? super Operation.Request>> extends ForwardingService implements Factory<C> {

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
        }

        @Provides @Singleton
        public BackendConnectionsService<?> getBackendConnectionService(
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
            BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    BackendConnectionsService.newInstance(configuration, connections);
            monitor.addOnStart(instance);
            return instance;
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> BackendConnectionsService<C> newInstance(
            BackendConfiguration configuration,
            ClientConnectionFactory<C> connections) {
        ServerViewFactory.FixedClientConnectionFactory<C> factory = 
                ServerViewFactory.FixedClientConnectionFactory.create(
                        configuration.getView().getClientAddress().get(), connections);
        return new BackendConnectionsService<C>(factory);
    }

    protected final ServerViewFactory.FixedClientConnectionFactory<C> connections;
    protected final ZxidTracker zxids;
    
    protected BackendConnectionsService(
            ServerViewFactory.FixedClientConnectionFactory<C> connections) {
        this.connections = connections;
        this.zxids = ZxidTracker.create();
    }

    public ServerViewFactory.FixedClientConnectionFactory<C> connections() {
        return connections;
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
    protected Service delegate() {
        return connections.second();
    }
}
