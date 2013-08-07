package edu.uw.zookeeper.orchestra.control;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.InjectorServiceLocator;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.server.SimpleServerConnections;
import edu.uw.zookeeper.server.SimpleServerExecutor;

@RunWith(JUnit4.class)
public class ControlTest {
    
    public static class MainModule extends AbstractModule {
    
        protected final SimpleServerExecutor serverExecutor;
        protected final SimpleServerConnections serverConnections;

        public MainModule() { 
            this.serverExecutor = SimpleServerExecutor.newInstance();
            this.serverConnections = SimpleServerConnections.newInstance(serverExecutor.getTasks());
        }
    
        @Override
        protected void configure() {
            bind(ServiceLocator.class).to(InjectorServiceLocator.class).in(Singleton.class);
            
            install(SimpleControlMaterializer.create(getServerConnections()));
        }
        
        @Provides @Singleton
        public SimpleServerExecutor getServerExecutor() {
            return serverExecutor;
        }

        @Provides @Singleton
        public SimpleServerConnections getServerConnections() {
            return serverConnections;
        }

        @Provides @Singleton
        public ServiceMonitor getServiceMonitor() {
            return ServiceMonitor.newInstance();
        }
        
        @Provides @Singleton
        public MainService getMainService(
                DependentServiceMonitor monitor,
                ServiceLocator locator) {
            MainService main = monitor.listen(new MainService(locator));
            monitor.get().add(main);
            return main;
        }
    }

    @Singleton
    @DependsOn({SimpleServerConnections.class, ControlMaterializerService.class})
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
    
    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        MainModule module = new MainModule();
        Injector injector = Guice.createInjector(module);
        injector.getInstance(MainService.class);
        injector.getInstance(ServiceMonitor.class).start().get();

        injector.getInstance(ServiceMonitor.class).stop().get();
    }
}
