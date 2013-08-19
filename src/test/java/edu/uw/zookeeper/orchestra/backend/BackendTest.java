package edu.uw.zookeeper.orchestra.backend;

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
import edu.uw.zookeeper.orchestra.data.VolumeCacheService;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.orchestra.peer.PeerTest;

@RunWith(JUnit4.class)
public class BackendTest {

    public static BackendTestModule module() {
        return BackendTestModule.create();
    }
    
    public static class BackendTestModule extends DependentModule {

        public static Injector injector() {
            return Guice.createInjector(
                    RuntimeModuleProvider.create(),
                    IntraVmAsNetModule.create(),
                    ControlTest.module(),
                    PeerTest.module(),
                    VolumeCacheService.module(),
                    create());
        }
        
        public static void start(ServiceLocator locator) throws InterruptedException, ExecutionException {
            ControlTestModule.start(locator);
            locator.getInstance(VolumeCacheService.class).start().get();
            locator.getInstance(SimpleBackendConfiguration.class).getServer().start().get();
            locator.getInstance(BackendRequestService.class).start().get();
            locator.getInstance(PeerConnectionsService.class).start().get();
        }
        
        public static BackendTestModule create() {
            return new BackendTestModule();
        }
        
        public BackendTestModule() {
        }

        @Override
        protected Module[] getModules() {
            Module[] modules = { SimpleBackend.create() };
            return modules;
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = BackendTestModule.injector();
        BackendTestModule.start(injector.getInstance(ServiceLocator.class));
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.start().get();
        Thread.sleep(500);
        monitor.stop().get();
    }
}
