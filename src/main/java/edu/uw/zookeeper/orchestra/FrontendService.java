package edu.uw.zookeeper.orchestra;

import java.net.SocketAddress;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class FrontendService extends AbstractIdleService {

    public static FrontendService newInstance(
            final RuntimeModule runtime,
            final ParameterizedFactory<Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>>> serverConnectionFactory) {
        final ServerInetAddressView clientAddress = 
                ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
        final ParameterizedFactory<Connection<Message.ServerMessage>, ServerCodecConnection> serverCodecConnectionFactory = 
                ServerCodecConnection.factory(runtime.publisherFactory());
        final Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection> serverCodecFactory = 
                CodecConnection.factory(serverCodecConnectionFactory);

        Factories.LazyHolder<ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnections = Factories.synchronizedLazyFrom(
                new Factory<ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>>() {
                    public ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> get() {
                        ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections = 
                                AbstractMain.monitors(runtime.serviceMonitor()).apply(
                                        serverConnectionFactory.get(serverCodecFactory).get(clientAddress.get()));
                        serverConnections.startAndWait();
                        return serverConnections;
                    }
                });
        return new FrontendService(clientAddress, serverConnections);
    }
    
    protected ServerInetAddressView address;
    protected Factories.LazyHolder<ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnections;
    
    protected FrontendService(ServerInetAddressView address,
            Factories.LazyHolder<ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnections) {
        this.address = address;
        this.serverConnections = serverConnections;
    }
    
    public ServerInetAddressView address() {
        return address;
    }
    
    public ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections() {
        return serverConnections.get();
    }

    @Override
    protected void startUp() throws Exception {
        // TODO Auto-generated method stub
        serverConnections();
    }

    @Override
    protected void shutDown() throws Exception {
        // TODO Auto-generated method stub
        
    }
    

}
