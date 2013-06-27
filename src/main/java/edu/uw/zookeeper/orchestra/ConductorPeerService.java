package edu.uw.zookeeper.orchestra;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.protocol.ConductorCodecConnection;
import edu.uw.zookeeper.orchestra.protocol.MessagePacket;

public class ConductorPeerService extends AbstractIdleService {

    protected final ServerConnectionFactory<MessagePacket, ConductorCodecConnection> serverConnections;
    protected final ClientConnectionFactory<MessagePacket, ConductorCodecConnection> clientConnections;

    public ConductorPeerService(
            ServerConnectionFactory<MessagePacket, ConductorCodecConnection> serverConnections,
            ClientConnectionFactory<MessagePacket, ConductorCodecConnection> clientConnections) {
        this.serverConnections = serverConnections;
        this.clientConnections = clientConnections;
    }
    
    @Override
    protected void startUp() throws Exception {
        serverConnections.start().get();
        clientConnections.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}

