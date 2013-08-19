package edu.uw.zookeeper.orchestra.frontend;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Factory;

public class SimpleFrontendConfiguration extends AbstractModule {

    public static SimpleFrontendConfiguration create() {
        return new SimpleFrontendConfiguration();
    }
    
    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public FrontendConfiguration getFrontendConfiguration(
            Factory<? extends SocketAddress> addresses) {
        ServerInetAddressView address = 
                ServerInetAddressView.of((InetSocketAddress) addresses.get());
        return new FrontendConfiguration(address);
    }
}
