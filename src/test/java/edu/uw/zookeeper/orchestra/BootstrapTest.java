package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.backend.BackendTest;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.frontend.AssignmentCacheService;
import edu.uw.zookeeper.orchestra.frontend.FrontendServerService;
import edu.uw.zookeeper.orchestra.frontend.SimpleFrontendConfiguration;
import edu.uw.zookeeper.orchestra.peer.EnsembleMemberService;

@RunWith(JUnit4.class)
public class BootstrapTest {

    public static Injector injector() {
        return BackendTest.injector().createChildInjector(
                AssignmentCacheService.module(),
                EnsembleMemberService.module(),
                SimpleFrontendConfiguration.create());
    }

    @DependsOn({ 
        ControlMaterializerService.class, 
        EnsembleMemberService.class,
        FrontendServerService.class })
    public static class SimpleMainService extends DependentService {

        public static class Module extends AbstractModule {

            public static Injector injector() {
                return BootstrapTest.injector().createChildInjector(
                        new Module());
            }
            
            @Override
            protected void configure() {
                bind(SimpleMainService.class).in(Singleton.class);
            }
        }
        
        @Inject
        protected SimpleMainService(Injector injector) {
            super(injector);
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = SimpleMainService.Module.injector();
        injector.getInstance(SimpleMainService.class).startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
