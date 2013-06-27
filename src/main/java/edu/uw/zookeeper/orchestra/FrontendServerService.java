package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpireSessionsTask;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.ServerConnectionListener;
import edu.uw.zookeeper.server.ServerExecutor;
import edu.uw.zookeeper.server.SessionParametersPolicy;

public class FrontendServerService extends AbstractIdleService {

    public static FrontendServerService newInstance(ServiceManager manager) {
        ServerInetAddressView clientAddress = FrontendConfiguration.get(manager.runtime());
        ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections = 
                manager.netModule().servers().get(
                        ServerCodecConnection.codecFactory(),
                        ServerCodecConnection.factory()).get(clientAddress.get());
        manager.runtime().serviceMonitor().addOnStart(serverConnections);

        SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(manager.runtime().configuration());
        ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(manager.runtime().publisherFactory().get(), policy);
        ExpireSessionsTask expires = ExpireSessionsTask.newInstance(sessions, manager.runtime().executors().asScheduledExecutorServiceFactory().get(), manager.runtime().configuration());
        AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        ServerExecutor serverExecutor = ServerExecutor.newInstance(manager.runtime().executors().asListeningExecutorServiceFactory().get(), manager.runtime().publisherFactory(), sessions);
        ServerConnectionListener server = ServerConnectionListener.newInstance(serverConnections, serverExecutor, serverExecutor, serverExecutor);

        return new FrontendServerService(clientAddress, serverConnections, manager);
    }

    protected final ServiceManager manager;
    protected final ServerInetAddressView address;
    protected final ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections;
    
    protected FrontendServerService(
            ServerInetAddressView address,
            ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections,
            ServiceManager manager) {
        this.address = address;
        this.serverConnections = serverConnections;
        this.manager = manager;
    }
    
    public ServerInetAddressView address() {
        return address;
    }
    
    public ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections() {
        return serverConnections;
    }
    
    public void register() throws InterruptedException, ExecutionException, KeeperException {
        Materializer materializer = manager.controlClient().materializer();
        Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.of(manager.conductor().view().id());
        Orchestra.Conductors.Entity.ClientAddress clientAddressNode = Orchestra.Conductors.Entity.ClientAddress.create(address(), entityNode, materializer);
        if (! address().equals(clientAddressNode.get())) {
            throw new IllegalStateException(clientAddressNode.get().toString());
        }        
    }

    @Override
    protected void startUp() throws Exception {
        serverConnections().start().get();
        
        //runtime.serviceMonitor().add(expires);
        
        register();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
