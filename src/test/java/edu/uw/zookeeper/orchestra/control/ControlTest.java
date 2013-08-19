package edu.uw.zookeeper.orchestra.control;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

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
    
        public static Injector injector() {
            return Guice.createInjector(
                    RuntimeModuleProvider.create(),
                    IntraVmAsNetModule.create(),
                    create());
        }
        
        public static void start(ServiceLocator locator) throws InterruptedException, ExecutionException {
            locator.getInstance(SimpleControlConfiguration.class).getServer().start().get();
            locator.getInstance(ControlMaterializerService.class).start().get();
        }
        
        public static ControlTestModule create() {
            return new ControlTestModule();
        }
        
        public ControlTestModule() {
        }

        @Override
        protected void configure() {
            super.configure();
        }
        
        @Override
        protected Module[] getModules() {
            Module[] modules = { SimpleControlMaterializer.create() };
            return modules;
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = ControlTestModule.injector();
        ControlTestModule.start(injector.getInstance(ServiceLocator.class));
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.start().get();
        Thread.sleep(500);
        monitor.stop().get();
    }
}
