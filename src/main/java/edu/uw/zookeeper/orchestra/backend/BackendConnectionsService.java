package edu.uw.zookeeper.orchestra.backend;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.orchestra.ClientConnectionsModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ZxidTracker;

public class BackendConnectionsService<C extends Connection<? super Operation.Request>> extends ForwardingService implements Factory<ListenableFuture<C>>, Function<C,C> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends ClientConnectionsModule {

        public Module() {}
        
        @Override
        protected void configure() {
            super.configure();
            TypeLiteral<BackendConnectionsService<?>> generic = new TypeLiteral<BackendConnectionsService<?>>() {};
            bind(BackendConnectionsService.class).to(generic);
        }

        @Provides @Singleton
        public BackendConnectionsService<?> getBackendConnectionService(
                BackendConfiguration configuration,
                NetClientModule clients,
                ListeningExecutorServiceFactory executors,
                ServiceMonitor monitor) throws Exception {
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                    getClientConnectionFactory(configuration.getTimeOut(), executors, clients);
            BackendConnectionsService<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    BackendConnectionsService.newInstance(configuration, clientConnections);
            monitor.addOnStart(instance);
            return instance;
        }

        @Override
        protected com.google.inject.Module[] getModules() {
            com.google.inject.Module[] modules = { BackendConfiguration.module() };
            return modules;
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> BackendConnectionsService<C> newInstance(
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
