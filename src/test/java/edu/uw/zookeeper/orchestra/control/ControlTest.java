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
import edu.uw.zookeeper.orchestra.net.IntraVmDefaultsModule;

@RunWith(JUnit4.class)
public class ControlTest {
    
    public static class MainModule extends DependentModule {
    
        public MainModule() {
        }
    
        @Override
        protected void configure() {
            super.configure();
        }
        
        @Override
        protected Module[] getModules() {
            Module[] modules = {
                    RuntimeModuleProvider.create(),
                    IntraVmDefaultsModule.create(), 
                    SimpleControlMaterializer.create()};
            return modules;
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        MainModule module = new MainModule();
        Injector injector = Guice.createInjector(module);
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.add(injector.getInstance(SimpleControlConfiguration.class).getServer());
        monitor.add(injector.getInstance(ControlMaterializerService.class));
        monitor.start().get();

        monitor.stop().get();
    }
}
