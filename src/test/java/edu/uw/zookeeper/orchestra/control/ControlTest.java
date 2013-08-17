package edu.uw.zookeeper.orchestra.control;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.DependentModule;
import edu.uw.zookeeper.orchestra.RuntimeModuleProvider;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;

@RunWith(JUnit4.class)
public class ControlTest {
    
    public static ControlTestModule module() {
        return new ControlTestModule();
    }
    
    public static class ControlTestModule extends DependentModule {
    
        public ControlTestModule() {
        }
        
        public void start(ServiceLocator locator) throws InterruptedException, ExecutionException {
            locator.getInstance(SimpleControlConfiguration.class).getServer().start().get();
            locator.getInstance(ControlMaterializerService.class).start().get();
        }
    
        @Override
        protected void configure() {
            super.configure();
        }
        
        @Override
        protected Module[] getModules() {
            Module[] modules = {
                    RuntimeModuleProvider.create(),
                    IntraVmAsNetModule.create(), 
                    SimpleControlMaterializer.create()};
            return modules;
        }
        
        @Provides @Singleton
        public ControlTestModule getSelf() {
            return this;
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        ControlTestModule module = module();
        Injector injector = Guice.createInjector(module);
        module.start(injector.getInstance(ServiceLocator.class));
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.start().get();
        Thread.sleep(500);
        monitor.stop().get();
    }
}
