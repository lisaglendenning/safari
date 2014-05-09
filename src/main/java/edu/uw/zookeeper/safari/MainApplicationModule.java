package edu.uw.zookeeper.safari;


import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.safari.backend.BackendRequestService;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.common.GuiceRuntimeModule;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
import edu.uw.zookeeper.safari.frontend.FrontendServerService;
import edu.uw.zookeeper.safari.net.IntraVmModule;
import edu.uw.zookeeper.safari.net.NettyModule;
import edu.uw.zookeeper.safari.peer.RegionConfiguration;
import edu.uw.zookeeper.safari.peer.RegionMemberService;
import edu.uw.zookeeper.safari.peer.PeerConnectionsService;

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
                RegionConfiguration.module(),
                BackendRequestService.module(),
                PeerConnectionsService.module(),
                RegionMemberService.module(),
                FrontendServerService.module());
    }

    @Singleton
    @DependsOn({
        ControlMaterializerService.class, 
        VolumeCacheService.class, 
        BackendRequestService.class, 
        PeerConnectionsService.class,
        RegionMemberService.class, 
        FrontendServerService.class })
    public static class MainService extends DependentService {
        @Inject
        public MainService(Injector injector) {
            super(injector);
        }
    }
}
