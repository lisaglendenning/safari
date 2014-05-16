package edu.uw.zookeeper.safari.frontend;

import org.apache.logging.log4j.LogManager;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ConfigurableTimeout;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.server.ServerConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.server.ServerProtocolConnection;
import edu.uw.zookeeper.server.ClientAddressConfiguration;

public class FrontendConnections extends AbstractModule {

    public static FrontendConnections create() {
        return new FrontendConnections();
    }
    
    protected FrontendConnections() {}
    
    @Override
    protected void configure() {
    }

    @Provides @Singleton @Frontend
    public ServerInetAddressView getFrontendConfiguration(Configuration configuration) {
        return ClientAddressConfiguration.get(configuration);
    }

    @Provides @Singleton @Frontend
    public TimeValue getFrontendTimeOutConfiguration(Configuration configuration) {
        return ConfigurableTimeout.get(configuration);
    }

    @Provides @Frontend @Singleton
    public ServerConnectionFactory<? extends ServerProtocolConnection<?,?>> newServerConnectionFactory(
            RuntimeModule runtime,
            final @Frontend ServerInetAddressView address,
            NetServerModule serverModule) {
        ServerConnectionFactory<? extends ServerProtocolConnection<?,?>> connections = ServerConnectionFactoryBuilder.defaults()
                .setAddress(address)
                .setServerModule(serverModule)
                .setRuntimeModule(runtime)
                .build();
        runtime.getServiceMonitor().add(connections);
        connections.addListener(
                new Service.Listener() {
                    @Override
                    public void running() {
                        LogManager.getLogger(Frontend.class).info("Listening on {}", address);
                    }
                }, SameThreadExecutor.getInstance());
        return connections;
    }
}
