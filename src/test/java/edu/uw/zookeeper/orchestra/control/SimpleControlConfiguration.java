package edu.uw.zookeeper.orchestra.control;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.server.SimpleServer;

public class SimpleControlConfiguration extends ControlConfiguration {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {
    
        public Module() {
        }
    
        @Override
        protected void configure() {
            bind(ControlConfiguration.class).to(SimpleControlConfiguration.class).in(Singleton.class);
        }
        
        @Provides @Singleton
        public SimpleControlConfiguration getSimpleControlConfiguration() {
            return SimpleControlConfiguration.create(SimpleServer.create());
        }
    }
    
    public static SimpleControlConfiguration create(
            SimpleServer server) {
        return new SimpleControlConfiguration(server);
    }
    
    protected final SimpleServer server;
    
    public SimpleControlConfiguration(
            SimpleServer server) {
        super(EnsembleView.of(
                ServerInetAddressView.of(
                        (InetSocketAddress) server.getConnections().connections().listenAddress())), 
            TimeValue.create(Session.Parameters.NEVER_TIMEOUT, TimeUnit.MILLISECONDS));
        this.server = server;
    }

    public SimpleServer getServer() {
        return server;
    }
}
