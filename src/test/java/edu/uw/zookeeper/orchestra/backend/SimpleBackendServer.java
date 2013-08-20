package edu.uw.zookeeper.orchestra.backend;

import java.net.SocketAddress;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.server.SimpleServer;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleBackendServer extends SimpleServer {

    public static SimpleBackendServer newInstance(
            IntraVmNetModule module) {
        return newInstance(module.factory().addresses().get(), module);
    }
    
    public static SimpleBackendServer newInstance(
            SocketAddress address,
            NetServerModule module) {
        SimpleServerExecutor executor = SimpleServerExecutor.newInstance();
        ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory = 
                module.getServerConnectionFactory(
                        ServerApplicationModule.codecFactory(),
                        ServerApplicationModule.connectionFactory()).get(address);
        ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections = 
                ServerConnectionExecutorsService.newInstance(connectionFactory, executor.getTasks());
        return new SimpleBackendServer(connections, executor);
    }

    public SimpleBackendServer(
            ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections,
            SimpleServerExecutor executor) {
        super(connections, executor);
    }
}
