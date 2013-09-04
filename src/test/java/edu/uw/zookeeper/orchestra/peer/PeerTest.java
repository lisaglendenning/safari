package edu.uw.zookeeper.orchestra.peer;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;

import edu.uw.zookeeper.clients.common.ServiceLocator;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.GuiceRuntimeModule;
import edu.uw.zookeeper.orchestra.control.ControlTest;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;

@RunWith(JUnit4.class)
public class PeerTest {
    
    public static PeerTestModule module() {
        return PeerTestModule.create();
    }
    
    @Singleton
    @DependsOn({ 
        ControlTest.ControlTestService.class, 
        PeerConnectionsService.class})
    public static class PeerTestService extends DependentService {

        @Inject
        public PeerTestService(ServiceLocator locator) {
            super(locator);
        }
    }
    
    public static class PeerTestModule extends DependentModule {

        public static Injector injector() {
            return Guice.createInjector(
                    GuiceRuntimeModule.create(),
                    IntraVmAsNetModule.create(),
                    ControlTest.module(),
                    create());
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
        injector.getInstance(DependentServiceMonitor.class).start(PeerTestService.class);
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
