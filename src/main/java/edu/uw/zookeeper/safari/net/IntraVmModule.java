package edu.uw.zookeeper.safari.net;

import java.net.SocketAddress;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.net.intravm.IntraVmCodecEndpointFactory;
import edu.uw.zookeeper.net.intravm.IntraVmEndpointFactory;
import edu.uw.zookeeper.net.intravm.IntraVmFactory;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;

public class IntraVmModule extends AbstractModule {

    public static IntraVmModule create() {
        return new IntraVmModule();
    }
    
    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public Factory<? extends SocketAddress> getAddresses() {
        return IntraVmEndpointFactory.loopbackAddresses(1);
    }
    
    @Provides @Singleton
    public IntraVmNetModule getIntraVmNetModule(
            IntraVmFactory factory) {
        return IntraVmNetModule.create(
                IntraVmCodecEndpointFactory.unpooled(),
                IntraVmEndpointFactory.sameThreadExecutors(),
                factory);
    }

    @Provides @Singleton
    public IntraVmFactory getIntraVmFactory(
            Factory<? extends SocketAddress> addresses) {
        return IntraVmFactory.newInstance(addresses,
                IntraVmEndpointFactory.syncMessageBus());
    }
}
