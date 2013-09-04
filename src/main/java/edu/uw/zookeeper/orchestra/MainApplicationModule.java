package edu.uw.zookeeper.orchestra;


import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.clients.common.ServiceLocator;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.backend.BackendRequestService;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.GuiceRuntimeModule;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.data.VolumeCacheService;
import edu.uw.zookeeper.orchestra.frontend.AssignmentCacheService;
import edu.uw.zookeeper.orchestra.frontend.FrontendServerService;
import edu.uw.zookeeper.orchestra.net.IntraVmModule;
import edu.uw.zookeeper.orchestra.net.NettyModule;
import edu.uw.zookeeper.orchestra.peer.EnsembleMemberService;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;

public class MainApplicationModule extends DependentModule {
    
    public static Application getApplication(RuntimeModule runtime) {
        Injector injector = Guice.createInjector(
                GuiceRuntimeModule.create(runtime), 
                MainApplicationModule.newInstance());
        return injector.getInstance(Application.class);
    }

    public static MainApplicationModule newInstance() {
        return new MainApplicationModule();
    }
    
    public MainApplicationModule() {
    }
    
    @Provides @Singleton
    public Application getApplication(
            ServiceMonitor monitor,
            MainService main) {
        monitor.add(main);
        return ServiceApplication.newInstance(monitor);
    }

    @Override
    protected List<com.google.inject.Module> getDependentModules() {
        return ImmutableList.<com.google.inject.Module>of(
                IntraVmModule.create(), 
                NettyModule.create(), 
                ControlMaterializerService.module(),
                VolumeCacheService.module(),
                AssignmentCacheService.module(),
                BackendRequestService.module(),
                PeerConnectionsService.module(),
                EnsembleMemberService.module(),
                FrontendServerService.module());
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
