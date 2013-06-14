package edu.uw.zookeeper.orchestra;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.Connection.CodecFactory;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.TimeValue;

public class ClientConnectionsModule {
    
    public static ClientConnectionsModule newInstance(
            RuntimeModule runtime, 
            ParameterizedFactory<CodecFactory<Message.ClientSessionMessage, Message.ServerSessionMessage, PingingClientCodecConnection>, Factory<ChannelClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection>>> clientConnectionFactory) {
        return new ClientConnectionsModule(runtime, clientConnectionFactory);
    }
    
    protected final ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections;
    protected final TimeValue timeOut;
    protected final Connection.CodecFactory<Message.ClientSessionMessage, Message.ServerSessionMessage, PingingClientCodecConnection> codecFactory;
    protected final AssignXidProcessor xids;
    
    public ClientConnectionsModule(
            RuntimeModule runtime, 
            ParameterizedFactory<CodecFactory<Message.ClientSessionMessage, Message.ServerSessionMessage, PingingClientCodecConnection>, Factory<ChannelClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection>>> clientConnectionFactory) {
        // Common framework for ZooKeeper client connections
        this.timeOut = ClientApplicationModule.TimeoutFactory.newInstance().get(runtime.configuration());
        this.codecFactory = CodecConnection.factory(
                PingingClientCodecConnection.factory(
                        timeOut, runtime.executors().asScheduledExecutorServiceFactory().get()));
        this.xids = AssignXidProcessor.newInstance();
        this.clientConnections = clientConnectionFactory.get(codecFactory).get();
    }
    
    public ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections() {
        return clientConnections;
    }
    
    public TimeValue timeOut() {
        return timeOut;
    }
    
    public Connection.CodecFactory<Message.ClientSessionMessage, Message.ServerSessionMessage, PingingClientCodecConnection> codecFactory() {
        return codecFactory;
    }
    
    public AssignXidProcessor xids() {
        return xids;
    }
}
