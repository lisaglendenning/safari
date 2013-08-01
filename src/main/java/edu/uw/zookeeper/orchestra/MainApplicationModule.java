package edu.uw.zookeeper.orchestra;


import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.netty.server.NettyServerModule;
import edu.uw.zookeeper.orchestra.backend.BackendRequestService;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.frontend.FrontendServerService;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.orchestra.peer.PeerService;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceApplication;

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
        install(ControlMaterializerService.module());
        install(VolumeCacheService.module());
        install(AssignmentCacheService.module());
        install(BackendRequestService.module());
        install(PeerService.module());
        install(FrontendServerService.module());
    }

    @Provides @Singleton
    public NettyModule getNetModule(RuntimeModule runtime) {
        return NettyModule.newInstance(runtime);
    }

    @Provides @Singleton
    public NettyClientModule getClientModule(NettyModule module) {
        return module.clients();
    }

    @Provides @Singleton
    public NettyServerModule getServerModule(NettyModule module) {
        return module.servers();
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
