package edu.uw.zookeeper.orchestra.control;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;

import edu.uw.zookeeper.clients.common.RuntimeModuleProvider;
import edu.uw.zookeeper.clients.common.ServiceLocator;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;

@RunWith(JUnit4.class)
public class ControlTest {
    
    public static ControlTestModule module() {
        return new ControlTestModule();
    }
    
    @Singleton
    @DependsOn({ 
        SimpleControlServer.class, 
        ControlMaterializerService.class})
    public static class ControlTestService extends DependentService {

        @Inject
        public ControlTestService(ServiceLocator locator) {
            super(locator);
        }
    }
    
    public static class ControlTestModule extends DependentModule {
    
        public static Injector injector() {
            return Guice.createInjector(
                    RuntimeModuleProvider.create(),
                    IntraVmAsNetModule.create(),
                    create());
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
        injector.getInstance(ControlTestService.class).startAsync().awaitRunning();;
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
