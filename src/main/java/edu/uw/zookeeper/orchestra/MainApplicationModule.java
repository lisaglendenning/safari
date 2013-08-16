package edu.uw.zookeeper.orchestra;


import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.orchestra.backend.BackendRequestService;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.data.VolumeCacheService;
import edu.uw.zookeeper.orchestra.frontend.AssignmentCacheService;
import edu.uw.zookeeper.orchestra.frontend.FrontendServerService;
import edu.uw.zookeeper.orchestra.net.NettyModule;
import edu.uw.zookeeper.orchestra.peer.PeerService;

public class MainApplicationModule extends AbstractModule {

    public static ParameterizedFactory<RuntimeModule, Application> main() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                MainApplicationModule module = new MainApplicationModule(RuntimeModuleProvider.create(runtime));
                Injector injector = Guice.createInjector(module);
                MainService service =
                    injector.getInstance(DependentServiceMonitor.class).listen(
                            injector.getInstance(MainService.class));
                runtime.serviceMonitor().add(service);
                return ServiceApplication.newInstance(runtime.serviceMonitor());
            }            
        };
    }
    
    protected final RuntimeModuleProvider runtime;
    
    public MainApplicationModule(
            RuntimeModuleProvider runtime) {
        this.runtime = runtime;
    }

    @Override
    protected void configure() {
        install(runtime);
        install(NettyModule.create());
        install(ControlMaterializerService.module());
        install(VolumeCacheService.module());
        install(AssignmentCacheService.module());
        install(BackendRequestService.module());
        install(PeerService.module());
        install(FrontendServerService.module());
    }

    @Singleton
    @DependsOn({ControlMaterializerService.class, VolumeCacheService.class, AssignmentCacheService.class, BackendRequestService.class, PeerService.class, FrontendServerService.class})
    public static class MainService extends DependentService {

        protected final ServiceLocator locator;
        
        @Inject
        public MainService(ServiceLocator locator) {
            this.locator = locator;
        }

        @Override
        protected ServiceLocator locator() {
            return locator;
        }
    }
}
