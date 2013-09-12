package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.safari.backend.BackendTest;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.frontend.AssignmentCacheService;
import edu.uw.zookeeper.safari.frontend.FrontendServerService;
import edu.uw.zookeeper.safari.peer.EnsembleConfiguration;

@RunWith(JUnit4.class)
public class FrontendTest {

    public static Injector injector() {
        return BackendTest.injector().createChildInjector(
                AssignmentCacheService.module(),
                EnsembleConfiguration.module(),
                SimpleFrontendConfiguration.create());
    }

    @DependsOn({ 
        ControlMaterializerService.class, 
        FrontendServerService.class })
    public static class FrontendTestService extends DependentService {

        public static class Module extends AbstractModule {

            public static Injector injector() {
                return FrontendTest.injector().createChildInjector(
                        new Module());
            }
            
            @Override
            protected void configure() {
                bind(FrontendTestService.class).in(Singleton.class);
            }
        }
        
        @Inject
        protected FrontendTestService(Injector injector) {
            super(injector);
        }
    }
    
    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = FrontendTestService.Module.injector();
        injector.getInstance(FrontendTestService.class).startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
