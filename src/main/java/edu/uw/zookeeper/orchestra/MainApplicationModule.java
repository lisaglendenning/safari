package edu.uw.zookeeper.orchestra;


import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.netty.server.NettyServerModule;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.ServiceApplication;

public class MainApplicationModule extends AbstractModule {

    public static ParameterizedFactory<RuntimeModule, Application> main() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                MainApplicationModule module = new MainApplicationModule(runtime);
                runtime.serviceMonitor().add(module.getMainService());
                return ServiceApplication.newInstance(runtime.serviceMonitor());
            }            
        };
    }
    
    protected final RuntimeModule runtime;
    protected final MainService service;
    
    public MainApplicationModule(RuntimeModule runtime) {
        this.runtime = runtime;
        this.service = new MainService();
    }

    @Override
    protected void configure() {
        bind(ServiceLocator.class).to(MainService.class);
    }

    @Provides
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Provides
    public MainService getMainService() {
        return service;
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
    
    protected class MainService extends AbstractIdleService implements Reference<Injector>, ServiceLocator {

        protected final Injector injector;
        
        protected MainService() {
            this.injector = Guice.createInjector(
                    MainApplicationModule.this, 
                    ControlClientService.module(),
                    BackendClientService.module(),
                    FrontendServerService.module(),
                    ConductorService.module());
        }
        
        @Override
        public Injector get() {
            return injector;
        }
        
        @Override
        public <T> T getInstance(Class<T> type) {
            return get().getInstance(type);
        }

        @Override
        protected void startUp() throws Exception {
            getInstance(ControlClientService.class).start().get();
            getInstance(ConductorService.class).start().get();
            getInstance(BackendClientService.class).start().get();
            getInstance(FrontendServerService.class).start().get();
        }

        @Override
        protected void shutDown() throws Exception {
        }
    }
}
