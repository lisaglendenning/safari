package edu.uw.zookeeper.orchestra;


import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceApplication;

public enum MainApplicationModule implements ParameterizedFactory<RuntimeModule, Application> {
    INSTANCE;
    
    public static MainApplicationModule getInstance() {
        return INSTANCE;
    }

    @Override
    public Application get(RuntimeModule runtime) {
        NettyModule netModule = NettyModule.newInstance(runtime);
        AbstractMain.monitors(runtime.serviceMonitor()).apply(
                Conductor.newInstance(runtime, 
                        netModule.clientConnectionFactory(),
                        netModule.serverConnectionFactory()));
        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }
}
