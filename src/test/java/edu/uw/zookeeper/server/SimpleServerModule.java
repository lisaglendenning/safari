package edu.uw.zookeeper.server;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.CachingBuilder;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.protocol.server.ServerConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleServerModule extends AbstractModule {

    public static SimpleServerModule create() {
        return new SimpleServerModule();
    }
    
    protected SimpleServerModule() {
    }
    
    @Provides @Singleton
    public SimpleServerBuilder<SimpleServerExecutor.Builder> newSimpleServer(
            InetSocketAddress address,
            NetServerModule serverModule,
            RuntimeModule runtime) {
        ClientAddressConfiguration.set(runtime.getConfiguration(), ServerInetAddressView.of(address));
        CachingBuilder<List<Service>, SimpleServerBuilder<SimpleServerExecutor.Builder>> instance = 
                CachingBuilder.fromBuilder(
                        SimpleServerBuilder.fromConnections(
                                ServerConnectionsHandler.builder()
                                    .setConnectionBuilder(ServerConnectionFactoryBuilder.defaults().setServerModule(serverModule)))
                                .setRuntimeModule(runtime)
                                .setDefaults());
        for (Service e: instance.build()) {
            runtime.getServiceMonitor().add(e);
        }
        return instance.get();
    }

    @Override
    protected void configure() {
        bind(SimpleServerBuilder.class).to(new TypeLiteral<SimpleServerBuilder<?>>(){}).in(Singleton.class);
        bind(new TypeLiteral<SimpleServerBuilder<?>>(){}).to(new TypeLiteral<SimpleServerBuilder<SimpleServerExecutor.Builder>>(){}).in(Singleton.class);
    }
}
