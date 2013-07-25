package edu.uw.zookeeper.orchestra.control;

import java.net.InetSocketAddress;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.SimpleClientConnections;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.server.SimpleServerConnections;

public class SimpleControlConnections extends AbstractModule {

    public static SimpleControlConnections create(SimpleServerConnections server) {
        return new SimpleControlConnections(server);
    }
    
    protected final SimpleServerConnections server;
    
    public SimpleControlConnections(SimpleServerConnections server) {
        this.server = server;
    }
    
    @Override
    protected void configure() {
        install(getControlConfigurationModule());
        TypeLiteral<ControlConnectionsService<?>> generic = new TypeLiteral<ControlConnectionsService<?>>(){};
        bind(ControlConnectionsService.class).to(generic);
    }

    @Provides @Singleton
    public ControlConnectionsService<?> getControlClientConnectionFactory(
            ControlConfiguration configuration) {
        ClientConnectionFactory<Operation.Request, ProtocolCodecConnection<Operation.Request,AssignXidCodec,IntraVmConnection<InetSocketAddress>>> clientConnections = 
                server.clients(SimpleClientConnections.<IntraVmConnection<InetSocketAddress>>codecFactory());
        return ControlConnectionsService.newInstance(clientConnections, configuration);
    }
    
    protected com.google.inject.Module getControlConfigurationModule() {
        return SimpleControlConfiguration.create(server.connections());
    }
}
