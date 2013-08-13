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

public class SimpleControlConnections extends AbstractModule {

    public static SimpleControlConnections create() {
        return new SimpleControlConnections();
    }
    
    public SimpleControlConnections() {
    }
    
    @Override
    protected void configure() {
        TypeLiteral<ControlConnectionsService<?>> generic = new TypeLiteral<ControlConnectionsService<?>>(){};
        bind(ControlConnectionsService.class).to(generic);
    }

    @Provides @Singleton
    public ControlConnectionsService<?> getControlClientConnectionFactory(
            SimpleControlConfiguration configuration) {
        ClientConnectionFactory<ProtocolCodecConnection<Operation.Request,AssignXidCodec,IntraVmConnection<InetSocketAddress>>> clientConnections = 
                configuration.getServer().getConnections().clients(SimpleClientConnections.<IntraVmConnection<InetSocketAddress>>codecFactory());
        return ControlConnectionsService.newInstance(clientConnections, configuration);
    }
}
