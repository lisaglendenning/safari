package edu.uw.zookeeper.safari.backend;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.client.SimpleClientBuilder;
import edu.uw.zookeeper.common.CachingBuilder;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.safari.backend.BackendConfiguration;
import edu.uw.zookeeper.safari.backend.BackendConnectionsService;
import edu.uw.zookeeper.safari.backend.BackendView;
import edu.uw.zookeeper.server.SimpleServerBuilder;
import edu.uw.zookeeper.server.SimpleServerConnectionsBuilder;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleBackendConnections extends BackendConnectionsService<ClientProtocolConnection<Message.ClientSession, Message.ServerSession, ?, ?>> {

    public static com.google.inject.Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        @Override
        protected void configure() {
            bind(BackendConnectionsService.class).to(new TypeLiteral<BackendConnectionsService<?>>(){}).in(Singleton.class);
            bind(new TypeLiteral<BackendConnectionsService<?>>(){}).to(SimpleBackendConnections.class).in(Singleton.class);
        }
        
        @Provides @Singleton
        public SimpleBackendConnections getSimpleControlConnections(
                IntraVmNetModule netModule, RuntimeModule runtime) {
            ServerInetAddressView address = ServerInetAddressView.of((InetSocketAddress) netModule.factory().addresses().get());
            return SimpleBackendConnections.newInstance(address, netModule, netModule, runtime);
        }
        
        @Provides @Singleton
        public BackendConfiguration getControlConfiguration(
                SimpleBackendConnections instance) {
            return instance.getConfiguration();
        }
    }
    
    public static SimpleBackendConnections newInstance(
            ServerInetAddressView address,
            NetServerModule serverModule,
            NetClientModule clientModule,
            RuntimeModule runtime) {
        SimpleServerConnectionsBuilder connectionsBuilder = SimpleServerConnectionsBuilder.defaults(address, serverModule);
        SimpleServerBuilder<SimpleServerExecutor.Builder> server = SimpleServerBuilder.fromBuilders(
                SimpleServerExecutor.builder(connectionsBuilder.getConnectionBuilder()), 
                connectionsBuilder)
                    .setRuntimeModule(runtime)
                    .setDefaults();
        @SuppressWarnings("unchecked")
        ClientConnectionFactory<ClientProtocolConnection<Message.ClientSession, Message.ServerSession, ?, ?>> connections = 
            (ClientConnectionFactory<ClientProtocolConnection<Message.ClientSession, Message.ServerSession, ?, ?>>) SimpleClientBuilder.connectionBuilder(clientModule).setRuntimeModule(runtime).setDefaults().build();
        EnsembleView<ServerInetAddressView> ensemble = EnsembleView.of(address);
        BackendConfiguration configuration = new BackendConfiguration(BackendView.of(address, ensemble), server.getConnectionsBuilder().getTimeOut());
        FixedClientConnectionFactory<ClientProtocolConnection<Message.ClientSession, Message.ServerSession, ?, ?>> factory = 
                FixedClientConnectionFactory.create(
                        configuration.getView().getClientAddress().get(), connections);
        return new SimpleBackendConnections(configuration, server, factory);
    }

    protected final BackendConfiguration configuration;
    protected final CachingBuilder<List<Service>, ? extends SimpleServerBuilder<?>> server;
    
    protected SimpleBackendConnections(
            BackendConfiguration configuration,
            SimpleServerBuilder<SimpleServerExecutor.Builder> server,
            FixedClientConnectionFactory<ClientProtocolConnection<Message.ClientSession, Message.ServerSession, ?, ?>> connections) {
        super(connections);
        this.configuration = configuration;
        this.server = CachingBuilder.fromBuilder(server);
    }

    public BackendConfiguration getConfiguration() {
        return configuration;
    }
    
    public SimpleServerBuilder<?> getServer() {
        return server.get();
    }

    @Override
    protected void startUp() throws Exception {
        for (Service e: server.build()) {
            startService(e);
        }
        
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        
        for (Service e: server.build()) {
            e.stopAsync().awaitTerminated();
        }
    }
}
