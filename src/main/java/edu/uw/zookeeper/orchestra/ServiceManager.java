package edu.uw.zookeeper.orchestra;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.netty.NettyModule;

public class ServiceManager extends AbstractIdleService {

    public static ServiceManager newInstance(RuntimeModule runtime, NettyModule netModule) {
        ServiceManager manager = new ServiceManager(runtime, netModule);
        runtime.serviceMonitor().add(manager);
        return manager;
    }
    
    protected final RuntimeModule runtime;
    protected final NettyModule netModule;
    protected volatile ControlClientService controlClient = null;
    protected volatile BackendClientService backendClient = null;
    protected volatile ConductorService conductor = null;
    protected volatile EnsembleMember ensembleMember = null;
    protected volatile FrontendServerService frontendServer = null;
    
    public ServiceManager(RuntimeModule runtime, NettyModule netModule) {
        this.runtime = runtime;
        this.netModule = netModule;
    }
    
    public RuntimeModule runtime() {
        return runtime;
    }
    
    public NettyModule netModule() {
        return netModule;
    }
    
    public ControlClientService controlClient() {
        return controlClient;
    }
    
    public BackendClientService backendClient() {
        return backendClient;
    }
    
    public ConductorService conductor() {
        return conductor;
    }
    
    @Override
    protected void startUp() throws Exception {
        this.controlClient = 
                ControlClientService.newInstance(runtime, netModule.clients());
        runtime.serviceMonitor().add(controlClient);
        controlClient.start().get();

        this.conductor = ConductorService.newInstance(this);
        runtime.serviceMonitor().add(conductor);
        conductor.start().get();
        
        this.backendClient = BackendClientService.newInstance(this);
        runtime.serviceMonitor().add(backendClient);
        backendClient.start().get();

        this.ensembleMember = EnsembleMember.newInstance(this);
        runtime.serviceMonitor().add(ensembleMember);
        ensembleMember.start().get();

        this.frontendServer = FrontendServerService.newInstance(this);
        runtime.serviceMonitor().add(frontendServer);
        frontendServer.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
