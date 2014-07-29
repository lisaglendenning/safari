package edu.uw.zookeeper.net;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.net.intravm.AbstractIntraVmEndpointFactory;
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
    public Factory<InetSocketAddress> getAddresses() {
        return IntraVmEndpointFactory.loopbackAddresses(1);
    }

    @Provides
    public InetSocketAddress getAddress(Factory<InetSocketAddress> addresses) {
        return addresses.get();
    }
    
    @Provides @Singleton
    public IntraVmNetModule getIntraVmNetModule(
            IntraVmFactory<ByteBuf,ByteBuf> factory) {
        return newIntraVmNetModule(
                factory);
    }

    @Provides @Singleton
    public IntraVmFactory<ByteBuf,ByteBuf> getIntraVmFactory(
            Factory<InetSocketAddress> addresses) {
        return IntraVmFactory.newInstance(addresses);
    }
    
    protected IntraVmNetModule newIntraVmNetModule(
            IntraVmFactory<ByteBuf,ByteBuf> factory) {
        return IntraVmNetModule.create(
                IntraVmCodecEndpointFactory.unpooled(),
                newExecutor(),
                factory);
    }

    protected Factory<? extends Executor> newExecutor() {
        return AbstractIntraVmEndpointFactory.sameThreadExecutors();
    }
}
