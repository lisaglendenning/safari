package edu.uw.zookeeper.orchestra.control;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.InjectorServiceLocator;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;

@RunWith(JUnit4.class)
public class ControlTest {
    
    public static class MainModule extends AbstractModule {
    
        public MainModule() {
        }
    
        @Override
        protected void configure() {
            bind(ServiceLocator.class).to(InjectorServiceLocator.class).in(Singleton.class);
            install(SimpleControlMaterializer.create());
        }
        
        @Provides @Singleton
        public ServiceMonitor getServiceMonitor() {
            return ServiceMonitor.newInstance();
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        MainModule module = new MainModule();
        Injector injector = Guice.createInjector(module);
        DependentServiceMonitor monitor = injector.getInstance(DependentServiceMonitor.class);
        monitor.get().add(injector.getInstance(SimpleControlConfiguration.class).getServer());
        monitor.get().add(monitor.listen(injector.getInstance(ControlMaterializerService.class)));
        monitor.get().start().get();

        monitor.get().stop().get();
    }
}
