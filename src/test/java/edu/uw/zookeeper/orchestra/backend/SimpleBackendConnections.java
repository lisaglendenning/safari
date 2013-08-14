package edu.uw.zookeeper.orchestra.backend;

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

public class SimpleBackendConnections extends AbstractModule {

    public static SimpleBackendConnections create() {
        return new SimpleBackendConnections();
    }
    
    public SimpleBackendConnections() {
    }
    
    @Override
    protected void configure() {
        TypeLiteral<BackendConnectionsService<?>> generic = new TypeLiteral<BackendConnectionsService<?>>(){};
        bind(BackendConnectionsService.class).to(generic);
    }

    @Provides @Singleton
    public BackendConnectionsService<?> getBackendConnectionsService(
            SimpleBackendConfiguration configuration) {
        ClientConnectionFactory<ProtocolCodecConnection<Operation.Request,AssignXidCodec,IntraVmConnection<InetSocketAddress>>> clientConnections = 
                configuration.getServer().getConnections().clients(SimpleClientConnections.<IntraVmConnection<InetSocketAddress>>codecFactory());
        return BackendConnectionsService.newInstance(configuration, clientConnections);
    }
}
