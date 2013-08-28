package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.clients.common.RuntimeModuleProvider;
import edu.uw.zookeeper.clients.common.ServiceLocator;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.backend.SimpleBackendConfiguration;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.control.ControlTest;
import edu.uw.zookeeper.orchestra.data.VolumeCacheService;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;
import edu.uw.zookeeper.orchestra.peer.EnsembleConfiguration;
import edu.uw.zookeeper.orchestra.peer.PeerTest;

@RunWith(JUnit4.class)
public class FrontendTest {

    public static FrontendTestModule module() {
        return FrontendTestModule.create();
    }

    @Singleton
    @DependsOn({ 
        ControlTest.ControlTestService.class, 
        FrontendServerService.class })
    public static class FrontendTestService extends DependentService {

        @Inject
        public FrontendTestService(ServiceLocator locator) {
            super(locator);
        }
    }
    
    public static class FrontendTestModule extends DependentModule {

        public static Injector injector() {
            return Guice.createInjector(
                    RuntimeModuleProvider.create(),
                    IntraVmAsNetModule.create(),
                    ControlTest.module(),
                    PeerTest.module(),
                    SimpleBackendConfiguration.create(),
                    EnsembleConfiguration.module(),
                    create());
        }

        public static FrontendTestModule create() {
            return new FrontendTestModule();
        }
        
        public FrontendTestModule() {
        }

        @Override
        protected Module[] getModules() {
            Module[] modules = {
                    VolumeCacheService.module(),
                    AssignmentCacheService.module(),
                    SimpleFrontend.create() };
            return modules;
        }

        @Provides @Singleton
        public FrontendTestModule getSelf() {
            return this;
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = FrontendTestModule.injector();
        injector.getInstance(DependentServiceMonitor.class).start(FrontendTestService.class);
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
