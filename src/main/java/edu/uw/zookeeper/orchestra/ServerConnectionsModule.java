package edu.uw.zookeeper.orchestra;

import java.net.SocketAddress;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.netty.server.NettyServerModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ServerConnectionsModule {
    
    public static ServerConnectionsModule newInstance(
            RuntimeModule runtime, 
            NettyServerModule serverFactory) {
        return new ServerConnectionsModule(runtime, serverFactory);
    }
    
    protected final ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnections;
    
    public ServerConnectionsModule(
            RuntimeModule runtime, 
            NettyServerModule serverFactory) {
        // Common framework for ZooKeeper server connections
        this.serverConnections = serverFactory.get(
                ServerCodecConnection.codecFactory(),
                ServerCodecConnection.factory());
    }
    
    public ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnections() {
        return serverConnections;
    }
}
