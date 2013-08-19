package edu.uw.zookeeper.orchestra.peer;


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
import edu.uw.zookeeper.orchestra.control.ControlTest;
import edu.uw.zookeeper.orchestra.control.ControlTest.ControlTestModule;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;

@RunWith(JUnit4.class)
public class PeerTest {
    
    public static PeerTestModule module() {
        return PeerTestModule.create();
    }
    
    public static class PeerTestModule extends DependentModule {

        public static Injector injector() {
            return Guice.createInjector(
                    RuntimeModuleProvider.create(),
                    IntraVmAsNetModule.create(),
                    ControlTest.module(),
                    create());
        }
        
        public static void start(ServiceLocator locator) throws InterruptedException, ExecutionException {
            ControlTestModule.start(locator);
            locator.getInstance(PeerConnectionsService.class).start().get();
        }
        
        public static PeerTestModule create() {
            return new PeerTestModule();
        }
        
        public PeerTestModule() {
        }

        @Override
        protected Module[] getModules() {
            Module[] modules = { SimplePeerConnections.create()};
            return modules;
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = PeerTestModule.injector();
        PeerTestModule.start(injector.getInstance(ServiceLocator.class));
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.start().get();
        Thread.sleep(500);
        monitor.stop().get();
    }
}
