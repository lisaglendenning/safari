package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.AbstractIdleService;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.protocol.ConductorCodecConnection;
import edu.uw.zookeeper.orchestra.protocol.JacksonModule;
import edu.uw.zookeeper.orchestra.protocol.MessagePacket;
import edu.uw.zookeeper.protocol.Operation;

public class ConductorService extends AbstractIdleService {

    public static ConductorService newInstance(
            ServiceManager manager) throws InterruptedException, ExecutionException, KeeperException {
        ConductorAddressView conductorView = ConductorConfiguration.get(manager);
        ServerConnectionFactory<MessagePacket, ConductorCodecConnection> serverConnections = 
                manager.netModule().servers().get(ConductorCodecConnection.codecFactory(JacksonModule.getMapper()), ConductorCodecConnection.factory()).get(conductorView.address().get());
        manager.runtime().serviceMonitor().addOnStart(serverConnections);
        return new ConductorService(conductorView, serverConnections, manager);
    }

    protected final ServiceManager manager;
    protected final ConductorAddressView view;
    protected final ServerConnectionFactory<MessagePacket, ConductorCodecConnection> serverConnections;

    public ConductorService(
            ConductorAddressView view,
            ServerConnectionFactory<MessagePacket, ConductorCodecConnection> serverConnections,
            ServiceManager manager) {
        this.view = view;
        this.serverConnections = serverConnections;
        this.manager = manager;
    }
    
    public ConductorAddressView view() {
        return view;
    }

    public ServerConnectionFactory<MessagePacket, ConductorCodecConnection> serverConnections() {
        return serverConnections;
    }
    
    protected void register() throws KeeperException, InterruptedException, ExecutionException {
        Materializer materializer = manager.controlClient().materializer();
        Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.of(view().id());
        Orchestra.Conductors.Entity.Presence presenceNode = Orchestra.Conductors.Entity.Presence.of(entityNode);
        Operation.SessionResult result = materializer.operator().create(presenceNode.path()).submit().get();
        Operations.unlessError(result.reply().reply(), result.toString());
    }
    
    @Override
    protected void startUp() throws Exception {
        serverConnections().start().get();
        register();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
