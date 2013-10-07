package edu.uw.zookeeper.safari.control;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.safari.control.ControlConfiguration;
import edu.uw.zookeeper.safari.control.ControlConnectionsService;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
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
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.server.SimpleServerBuilder;

public class SimpleControlConnectionsService extends ControlConnectionsService<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> {

    public static com.google.inject.Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        @Override
        protected void configure() {
            bind(ControlConnectionsService.class).to(new TypeLiteral<ControlConnectionsService<?>>(){}).in(Singleton.class);
            bind(new TypeLiteral<ControlConnectionsService<?>>(){}).to(SimpleControlConnectionsService.class).in(Singleton.class);
        }
        
        @Provides @Singleton
        public SimpleControlConnectionsService getSimpleControlConnections(
                IntraVmNetModule netModule, 
                RuntimeModule runtime) {
            ServerInetAddressView address = ServerInetAddressView.of((InetSocketAddress) netModule.factory().addresses().get());
            return SimpleControlConnectionsService.newInstance(address, netModule, netModule, runtime);
        }
        
        @Provides @Singleton
        public ControlConfiguration getControlConfiguration(
                SimpleControlConnectionsService instance) {
            return instance.getConfiguration();
        }
    }
    
    public static SimpleControlConnectionsService newInstance(
            ServerInetAddressView address,
            NetServerModule serverModule,
            NetClientModule clientModule,
            RuntimeModule runtime) {
        SimpleServerBuilder server = SimpleServerBuilder.defaults(address, serverModule)
                .setRuntimeModule(runtime)
                .setDefaults();
        EnsembleView<ServerInetAddressView> ensemble = EnsembleView.of(address);
        ControlConfiguration configuration = new ControlConfiguration(ensemble, server.getConnectionBuilder().getTimeOut());
        @SuppressWarnings("unchecked")
        ClientConnectionFactory<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> connections = 
            (ClientConnectionFactory<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>>) SimpleClientBuilder.connectionBuilder(clientModule).setRuntimeModule(runtime).setDefaults().build();
        EnsembleViewFactory<ServerViewFactory<Session, OperationClientExecutor<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>>>> factory = 
                EnsembleViewFactory.fromSession(
                        connections,
                        configuration.getEnsemble(), 
                        configuration.getTimeOut(),
                        runtime.getExecutors().get(ScheduledExecutorService.class));
        return new SimpleControlConnectionsService(configuration, server, connections, factory);
    }
    
    protected final ControlConfiguration configuration;
    protected final SimpleServerBuilder server;
    
    protected SimpleControlConnectionsService(
            ControlConfiguration configuration,
            SimpleServerBuilder server,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> connections,
            EnsembleViewFactory<ServerViewFactory<Session, OperationClientExecutor<ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>>>> factory) {
        super(connections, factory);
        this.configuration = configuration;
        this.server = server;
    }
    
    public ControlConfiguration getConfiguration() {
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
