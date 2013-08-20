package edu.uw.zookeeper.orchestra.backend;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;

public class SimpleBackendConfiguration extends AbstractModule {

    public static SimpleBackendConfiguration create() {
        return new SimpleBackendConfiguration();
    }
    
    public SimpleBackendConfiguration() {
    }

    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public SimpleBackendServer getSimpleBackendServer(
            IntraVmNetModule net,
            ScheduledExecutorService executor) {
        return SimpleBackendServer.newInstance(net, executor);
    }
    
    @Provides @Singleton
    public BackendConfiguration getSimpleBackendConfiguration(
            SimpleBackendServer server) {
        ServerInetAddressView address = ServerInetAddressView.of(
                (InetSocketAddress) server.getConnections().connections().listenAddress());
        EnsembleView<ServerInetAddressView> ensemble = EnsembleView.of(address);
        BackendView view = BackendView.of(address, ensemble);
        TimeValue timeOut = TimeValue.create(Session.Parameters.NEVER_TIMEOUT, TimeUnit.MILLISECONDS);
        return new BackendConfiguration(view, timeOut);
    }
}
