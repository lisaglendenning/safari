package edu.uw.zookeeper.orchestra.backend;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.ClientApplicationModule;
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
            BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = new BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>(configuration.getView(), clientConnections);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }

    protected final BackendView view;
    protected final ClientConnectionFactory<?,C> clientConnections;
    protected final ZxidTracker zxids;
    
    protected BackendConnectionsService(
            BackendView view,
            ClientConnectionFactory<?, C> clientConnections) {
        this.clientConnections = clientConnections;
        this.view = view;
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
        C client;
        try {
            client = clientConnections.connect(view.getClientAddress().get()).get();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        ZxidTracker.ZxidListener.create(zxids, client);
        return client;
    }
    
    @Override
    protected void startUp() throws Exception {
        clientConnections.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
