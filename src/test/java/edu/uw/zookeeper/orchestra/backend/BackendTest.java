package edu.uw.zookeeper.orchestra.backend;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.data.VolumeCacheService;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.orchestra.peer.PeerTest;

@RunWith(JUnit4.class)
public class BackendTest {

    public static Injector injector() {
        return PeerTest.injector().createChildInjector(
                VolumeCacheService.module(),
                SimpleBackendRequestServiceModule.create());
    }
    
    public static class SimpleBackendRequestServiceModule extends BackendRequestService.Module {

        public static SimpleBackendRequestServiceModule create() {
            return new SimpleBackendRequestServiceModule();
        }
        
        public SimpleBackendRequestServiceModule() {
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(SimpleBackendConnections.module());
        }
    }
    
    @DependsOn({ 
        ControlMaterializerService.class, 
        VolumeCacheService.class,
        BackendRequestService.class,
        PeerConnectionsService.class })
    public static class BackendTestService extends DependentService {

        public static class Module extends AbstractModule {

            public static Injector injector() {
                return BackendTest.injector().createChildInjector(new Module());
            }
            
            @Override
            protected void configure() {
                bind(BackendTestService.class).in(Singleton.class);
            }
        }
        
        @Inject
        protected BackendTestService(Injector injector) {
            super(injector);
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = BackendTestService.Module.injector();
        injector.getInstance(BackendTestService.class).startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
