package edu.uw.zookeeper.orchestra.peer;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.net.intravm.IntraVmEndpointFactory;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;

public class SimplePeerConnections extends AbstractModule {

    public static SimplePeerConnections create() {
        return new SimplePeerConnections();
    }
    
    public SimplePeerConnections() {
    }
    
    @Override
    protected void configure() {
    }


    @Provides @Singleton
    public IntraVmEndpointFactory<InetSocketAddress> getIntraVmEndpointFactory() {
        return IntraVmEndpointFactory.defaults();
    }
    
    @Provides @Singleton
    public PeerConnectionsService getPeerConnectionsService(
            PeerConfiguration configuration,
            ControlMaterializerService<?> control,
            ServiceLocator locator,
            IntraVmEndpointFactory<InetSocketAddress> endpoints,
            DependentServiceMonitor monitor) throws InterruptedException, ExecutionException, KeeperException {
        ServerConnectionFactory<Connection<MessagePacket>> serverConnections = null;
        ClientConnectionFactory<Connection<MessagePacket>> clientConnections =  null;
        PeerConnectionsService instance = monitor.listen(PeerConnectionsService.newInstance(
                configuration.getView().id(), 
                serverConnections, 
                clientConnections,
                null,
                null,
                control.materializer(),
                locator));
        return instance;
    }

    @Provides @Singleton
    public ClientPeerConnections getClientPeerConnections(
            PeerConnectionsService service) {
        return service.clients();
    }

    @Provides @Singleton
    public ServerPeerConnections getServerPeerConnections(
            PeerConnectionsService service) {
        return service.servers();
    }
}
