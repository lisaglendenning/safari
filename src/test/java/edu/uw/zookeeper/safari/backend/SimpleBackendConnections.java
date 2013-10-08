package edu.uw.zookeeper.safari.backend;

import java.net.InetSocketAddress;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.client.SimpleClientBuilder;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.safari.backend.BackendConfiguration;
import edu.uw.zookeeper.safari.backend.BackendConnectionsService;
import edu.uw.zookeeper.safari.backend.BackendView;
import edu.uw.zookeeper.server.SimpleServerBuilder;

public class SimpleBackendConnections extends BackendConnectionsService<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> {

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
        SimpleServerBuilder server = SimpleServerBuilder.defaults(address, serverModule)
                .setRuntimeModule(runtime)
                .setDefaults();
        @SuppressWarnings("unchecked")
        ClientConnectionFactory<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> connections = 
            (ClientConnectionFactory<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>>) SimpleClientBuilder.connectionBuilder(clientModule).setRuntimeModule(runtime).setDefaults().build();
        EnsembleView<ServerInetAddressView> ensemble = EnsembleView.of(address);
        BackendConfiguration configuration = new BackendConfiguration(BackendView.of(address, ensemble), server.getTimeOut());
        FixedClientConnectionFactory<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> factory = 
                FixedClientConnectionFactory.create(
                        configuration.getView().getClientAddress().get(), connections);
        return new SimpleBackendConnections(configuration, server, factory);
    }

    protected final BackendConfiguration configuration;
    protected final SimpleServerBuilder server;
    
    protected SimpleBackendConnections(
            BackendConfiguration configuration,
            SimpleServerBuilder server,
            FixedClientConnectionFactory<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> connections) {
        super(connections);
        this.configuration = configuration;
        this.server = server;
    }

    public BackendConfiguration getConfiguration() {
        return configuration;
    }
    
    public SimpleServerBuilder getServer() {
        return server;
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
