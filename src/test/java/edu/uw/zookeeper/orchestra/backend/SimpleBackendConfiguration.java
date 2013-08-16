package edu.uw.zookeeper.orchestra.backend;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.server.SimpleServer;

public class SimpleBackendConfiguration extends BackendConfiguration {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {
    
        public Module() {
        }
    
        @Override
        protected void configure() {
            bind(BackendConfiguration.class).to(SimpleBackendConfiguration.class).in(Singleton.class);
        }
        
        @Provides @Singleton
        public SimpleBackendConfiguration getSimpleBackendConfiguration(
                IntraVmNetModule net) {
            return SimpleBackendConfiguration.create(SimpleServer.newInstance(net));
        }
    }
    
    public static SimpleBackendConfiguration create(
            SimpleServer server) {
        ServerInetAddressView address = ServerInetAddressView.of(
                (InetSocketAddress) server.getConnections().connections().listenAddress());
        EnsembleView<ServerInetAddressView> ensemble = EnsembleView.of(address);
        BackendView view = BackendView.of(address, ensemble);
        TimeValue timeOut = TimeValue.create(Session.Parameters.NEVER_TIMEOUT, TimeUnit.MILLISECONDS);
        return new SimpleBackendConfiguration(server, view, timeOut);
    }
    
    protected final SimpleServer server;
    
    protected SimpleBackendConfiguration(
            SimpleServer server,
            BackendView view, 
            TimeValue timeOut) {
        super(view, timeOut);
        this.server = server;
    }

    public SimpleServer getServer() {
        return server;
    }
}
