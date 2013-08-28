package edu.uw.zookeeper.orchestra;


import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.clients.common.RuntimeModuleProvider;
import edu.uw.zookeeper.clients.common.ServiceLocator;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.backend.BackendRequestService;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.data.VolumeCacheService;
import edu.uw.zookeeper.orchestra.frontend.AssignmentCacheService;
import edu.uw.zookeeper.orchestra.frontend.FrontendServerService;
import edu.uw.zookeeper.orchestra.net.IntraVmModule;
import edu.uw.zookeeper.orchestra.net.NettyModule;
import edu.uw.zookeeper.orchestra.peer.EnsembleMemberService;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;

public class MainApplicationModule extends DependentModule {

    public static ParameterizedFactory<RuntimeModule, Application> main() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                MainApplicationModule module = MainApplicationModule.newInstance(runtime);
                Injector injector = Guice.createInjector(module);
                return injector.getInstance(Application.class);
            }            
        };
    }
    
    public static MainApplicationModule newInstance(RuntimeModule runtime) {
        return new MainApplicationModule(RuntimeModuleProvider.create(runtime));
    }
    
    protected final RuntimeModuleProvider runtime;
    
    public MainApplicationModule(
            RuntimeModuleProvider runtime) {
        this.runtime = runtime;
    }
    
    @Provides @Singleton
    public Application getApplication(
            ServiceMonitor monitor,
            MainService main) {
        monitor.add(main);
        return ServiceApplication.newInstance(monitor);
    }

    @Override
    protected Module[] getModules() {
        Module[] modules = { 
                runtime, 
                IntraVmModule.create(), 
                NettyModule.create(), 
                ControlMaterializerService.module(),
                VolumeCacheService.module(),
                AssignmentCacheService.module(),
                BackendRequestService.module(),
                PeerConnectionsService.module(),
                EnsembleMemberService.module(),
                FrontendServerService.module() };
        return modules;
    }

    @Singleton
    @DependsOn({
        ControlMaterializerService.class, 
        VolumeCacheService.class, 
        AssignmentCacheService.class, 
        BackendRequestService.class, 
        PeerConnectionsService.class,
        EnsembleMemberService.class, 
        FrontendServerService.class })
    public static class MainService extends DependentService {
        @Inject
        public MainService(ServiceLocator locator) {
            super(locator);
        }
    }
}
