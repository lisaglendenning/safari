package edu.uw.zookeeper.orchestra.control;

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

public class SimpleControlConfiguration extends AbstractModule {

    public static SimpleControlConfiguration create() {
        return new SimpleControlConfiguration();
    }

    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public SimpleControlServer getSimpleControlServer(
            IntraVmNetModule net,
            ScheduledExecutorService executor) {
        return SimpleControlServer.newInstance(net, executor);
    }
    
    @Provides @Singleton
    public ControlConfiguration getControlConfiguration(
            SimpleControlServer server) {
        EnsembleView<ServerInetAddressView> ensemble = EnsembleView.of(
                ServerInetAddressView.of(
                        (InetSocketAddress) server.getConnections().connections().listenAddress()));
        TimeValue timeOut = TimeValue.create(Session.Parameters.NEVER_TIMEOUT, TimeUnit.MILLISECONDS);
        return new ControlConfiguration(ensemble, timeOut);
    }
}
