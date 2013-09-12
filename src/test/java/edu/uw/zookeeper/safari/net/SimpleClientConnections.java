package edu.uw.zookeeper.safari.net;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.server.SimpleServerBuilder;

public class SimpleClientConnections extends AbstractIdleService implements Factory<ListenableFuture<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>>> {

    protected final ServiceMonitor monitor;
    protected final SimpleServerBuilder server;
    protected final ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clients;
    
    @Inject
    protected SimpleClientConnections(
            ServiceMonitor monitor,
            SimpleServerBuilder server,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clients) {
        this.monitor = monitor;
        this.server = server;
        this.clients = clients;
    }
    
    public SimpleServerBuilder getServer() {
        return server;
    }
    
    @Override
    public ListenableFuture<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> get() {
        return clients.connect(server.getConnectionBuilder().getAddress().get());
    }

    @Override
    protected void startUp() throws Exception {
        for (Service e: server.build()) {
            monitor.add(e);
        }
        monitor.add(clients);
        monitor.addOnStart(this);
    }

    @Override
    protected void shutDown() throws Exception {
    }
}