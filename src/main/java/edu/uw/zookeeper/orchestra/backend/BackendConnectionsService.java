package edu.uw.zookeeper.orchestra.backend;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.RuntimeModule;
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
                RuntimeModule runtime) throws Exception {
            ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = ClientApplicationModule.codecFactory();
            ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> pingingFactory = 
                    PingingClient.factory(configuration.getTimeOut(), runtime.executors().asScheduledExecutorServiceFactory().get());
            ClientConnectionFactory<Operation.Request, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                   clientModule.get(codecFactory, pingingFactory).get();
            runtime.serviceMonitor().addOnStart(clientConnections);
            BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    BackendConnectionsService.newInstance(configuration.getView(), clientConnections);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> BackendConnectionsService<C> newInstance(
            BackendView view,
            ClientConnectionFactory<?, C> clientConnections) {
        return new BackendConnectionsService<C>(view, clientConnections);
    }

    protected final BackendView view;
    protected final ServerViewFactory.FixedClientConnectionFactory<C> clientConnections;
    protected final ZxidTracker zxids;
    
    protected BackendConnectionsService(
            BackendView view,
            ClientConnectionFactory<?, C> clientConnections) {
        this.view = view;
        this.clientConnections = ServerViewFactory.FixedClientConnectionFactory.create(view.getClientAddress().get(), clientConnections);
        this.zxids = ZxidTracker.create();
    }

    public BackendView view() {
        return view;
    }
    
    public ZxidTracker zxids() {
        return zxids;
    }
    
    @Override
    public C get() {
        C client = clientConnections.get();
        ZxidTracker.ZxidListener.create(zxids, client);
        return client;
    }
    
    @Override
    protected void startUp() throws Exception {
        clientConnections.second().start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        clientConnections.second().stop().get();
    }
}
