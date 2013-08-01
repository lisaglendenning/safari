package edu.uw.zookeeper.orchestra.control;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.util.TimeValue;

public class SimpleControlConfiguration extends AbstractModule {

    public static SimpleControlConfiguration create(
            ServerConnectionFactory<?> server) {
        return new SimpleControlConfiguration(server);
    }
    
    protected final ServerConnectionFactory<?> server;
    
    public SimpleControlConfiguration(ServerConnectionFactory<?> server) {
        this.server = server;
    }

    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public ControlConfiguration getControlConfiguration() {
        EnsembleView<ServerInetAddressView> ensemble = 
                EnsembleView.of(ServerInetAddressView.of((InetSocketAddress) server.listenAddress()));
        TimeValue timeOut = TimeValue.create(Session.Parameters.NEVER_TIMEOUT, TimeUnit.MILLISECONDS);
        return new ControlConfiguration(ensemble, timeOut);
    }
}
