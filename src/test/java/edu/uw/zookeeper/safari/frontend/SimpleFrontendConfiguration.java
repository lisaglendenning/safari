package edu.uw.zookeeper.safari.frontend;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.safari.frontend.FrontendConfiguration;
import edu.uw.zookeeper.safari.frontend.FrontendServerExecutor;
import edu.uw.zookeeper.safari.frontend.FrontendServerService;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.TimeValue;

public class SimpleFrontendConfiguration extends FrontendServerService.Module {

    public static SimpleFrontendConfiguration create() {
        return new SimpleFrontendConfiguration();
    }

    @Provides @Singleton
    public FrontendConfiguration getFrontendConfiguration(
            Factory<? extends SocketAddress> addresses) {
        ServerInetAddressView address = 
                ServerInetAddressView.of((InetSocketAddress) addresses.get());
        TimeValue timeOut = TimeValue.create(Session.Parameters.noTimeout(), Session.Parameters.timeoutUnit());
        return new FrontendConfiguration(address, timeOut);
    }

    @Override
    protected List<com.google.inject.Module> getDependentModules() {
        return Lists.<com.google.inject.Module>newArrayList(
                RegionsConnectionsService.module(),
                FrontendServerExecutor.module());
    }
}
