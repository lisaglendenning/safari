package edu.uw.zookeeper.orchestra.net;

import com.google.inject.Singleton;

import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;

public class IntraVmAsNetModule extends IntraVmModule {

    public static IntraVmAsNetModule create() {
        return new IntraVmAsNetModule();
    }
    
    @Override
    protected void configure() {
        super.configure();
        bind(NetClientModule.class).to(IntraVmNetModule.class).in(Singleton.class);
        bind(NetServerModule.class).to(IntraVmNetModule.class).in(Singleton.class);
    }
}
