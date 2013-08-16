package edu.uw.zookeeper.orchestra.net;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;

public class IntraVmDefaultsModule extends AbstractModule {

    public static IntraVmDefaultsModule create() {
        return new IntraVmDefaultsModule();
    }
    
    @Override
    protected void configure() {
        bind(NetClientModule.class).to(IntraVmNetModule.class).in(Singleton.class);
        bind(NetServerModule.class).to(IntraVmNetModule.class).in(Singleton.class);
    }

    @Provides @Singleton
    public IntraVmNetModule getIntraVmNetModule() {
        return IntraVmNetModule.defaults();
    }
}
