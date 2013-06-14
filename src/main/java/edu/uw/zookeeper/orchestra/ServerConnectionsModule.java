package edu.uw.zookeeper.orchestra;

import java.net.SocketAddress;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ServerConnectionsModule {
    
    public static ServerConnectionsModule newInstance(
            RuntimeModule runtime, 
            ParameterizedFactory<Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>>> serverConnectionFactory) {
        return new ServerConnectionsModule(runtime, serverConnectionFactory);
    }
    
    protected final ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnections;
    protected final Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection> codecFactory;
    
    public ServerConnectionsModule(
            RuntimeModule runtime, 
            ParameterizedFactory<Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>>> serverConnectionFactory) {
        // Common framework for ZooKeeper server connections
         this.codecFactory = 
                CodecConnection.factory(ServerCodecConnection.factory(runtime.publisherFactory()));
        this.serverConnections = serverConnectionFactory.get(codecFactory);
    }
    
    public ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnections() {
        return serverConnections;
    }
    
    public Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection> codecFactory() {
        return codecFactory;
    }
}
