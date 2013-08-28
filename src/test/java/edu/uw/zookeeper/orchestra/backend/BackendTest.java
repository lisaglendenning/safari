package edu.uw.zookeeper.orchestra.backend;

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
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.control.ControlTest;
import edu.uw.zookeeper.orchestra.data.VolumeCacheService;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.orchestra.peer.PeerTest;

@RunWith(JUnit4.class)
public class BackendTest {

    public static BackendTestModule module() {
        return BackendTestModule.create();
    }

    @Singleton
    @DependsOn({ 
        ControlTest.ControlTestService.class, 
        VolumeCacheService.class,
        SimpleBackendServer.class,
        BackendRequestService.class,
        PeerConnectionsService.class })
    public static class BackendTestService extends DependentService {

        @Inject
        public BackendTestService(ServiceLocator locator) {
            super(locator);
        }
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
        injector.getInstance(DependentServiceMonitor.class).start(BackendTestService.class);
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
