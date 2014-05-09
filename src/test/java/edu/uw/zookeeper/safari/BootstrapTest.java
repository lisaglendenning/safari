package edu.uw.zookeeper.safari;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.safari.backend.BackendRequestService;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlTest;
import edu.uw.zookeeper.safari.frontend.FrontendServerService;
import edu.uw.zookeeper.safari.frontend.FrontendTest;
import edu.uw.zookeeper.safari.peer.RegionMemberService;

@RunWith(JUnit4.class)
public class BootstrapTest {

    public static Injector injector() {
        return injector(ControlTest.injector());
    }

    public static Injector injector(Injector parent) {
        return FrontendTest.injector(parent).createChildInjector(
                RegionMemberService.module());
    }

    @DependsOn({ 
        ControlMaterializerService.class, 
        BackendRequestService.class,
        RegionMemberService.class,
        FrontendServerService.class })
    public static class SimpleMainService extends DependentService {

        public static class Module extends AbstractModule {

            public static Injector injector() {
                return injector(BootstrapTest.injector());
            }

            public static Injector injector(Injector parent) {
                return parent.createChildInjector(
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

    @Test(timeout=60000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = SimpleMainService.Module.injector();
        injector.getInstance(SimpleMainService.class).startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(15000);
        monitor.stopAsync().awaitTerminated();
    }
}
