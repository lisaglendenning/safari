package edu.uw.zookeeper.orchestra;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.util.TimeValue;

public class ClientConnectionsModule {
    
    public static ClientConnectionsModule newInstance(
            RuntimeModule runtime, 
            NettyClientModule clientFactory) {
        return new ClientConnectionsModule(runtime, clientFactory);
    }
    
    protected final ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections;
    protected final TimeValue timeOut;
    protected final AssignXidProcessor xids;
    
    public ClientConnectionsModule(
            RuntimeModule runtime, 
            NettyClientModule clientFactory) {
        // Common framework for ZooKeeper client connections
        this.timeOut = ClientApplicationModule.TimeoutFactory.newInstance().get(runtime.configuration());
        this.xids = AssignXidProcessor.newInstance();
        this.clientConnections = clientFactory.get(
                PingingClientCodecConnection.codecFactory(), 
                PingingClientCodecConnection.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get())).get();
    }
    
    public ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections() {
        return clientConnections;
    }
    
    public TimeValue timeOut() {
        return timeOut;
    }
    public AssignXidProcessor xids() {
        return xids;
    }
}
