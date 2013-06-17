package edu.uw.zookeeper.orchestra;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.server.ServerApplicationModule;

public class EnsembleConnections extends AbstractIdleService {

    public static EnsembleConnections newInstance(
            RuntimeModule runtime,
            ClientConnectionsModule clientConnectionsModule,
            ServerConnectionsModule serverConnectionsModule) {
        ServerInetAddressView clientAddress = 
                ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
        ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections = 
                serverConnectionsModule.serverConnections().get(clientAddress.get());
        return new EnsembleConnections(runtime, clientAddress, serverConnections);
    }
    
    protected final RuntimeModule runtime;
    protected final ServerInetAddressView address;
    protected final ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections;
    
    protected EnsembleConnections(
            RuntimeModule runtime,
            ServerInetAddressView address,
            ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections) {
        this.runtime = runtime;
        this.address = address;
        this.serverConnections = serverConnections;
    }
    
    public ServerInetAddressView address() {
        return address;
    }
    
    public ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections() {
        return serverConnections;
    }

    @Override
    protected void startUp() throws Exception {
        runtime.serviceMonitor().add(serverConnections);
        serverConnections().start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        serverConnections().stop().get();
    }
}
