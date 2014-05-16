package edu.uw.zookeeper.net;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;

public class IntraVmAsNetModule extends AbstractModule {

    public static IntraVmAsNetModule create() {
        return new IntraVmAsNetModule();
    }
    
    @Override
    protected void configure() {
        install(IntraVmModule.create());
        bind(NetClientModule.class).to(IntraVmNetModule.class).in(Singleton.class);
        bind(NetServerModule.class).to(IntraVmNetModule.class).in(Singleton.class);
    }
}
