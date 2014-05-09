package edu.uw.zookeeper.safari.backend;

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
import edu.uw.zookeeper.safari.backend.BackendRequestService;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlTest;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
import edu.uw.zookeeper.safari.peer.PeerTest;
import edu.uw.zookeeper.safari.peer.RegionConfiguration;

@RunWith(JUnit4.class)
public class BackendTest {

    public static Injector injector() {
        return injector(ControlTest.injector());
    }

    public static Injector injector(Injector parent) {
        return PeerTest.injector(parent).createChildInjector(
                VolumeCacheService.module(),
                RegionConfiguration.module(),
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
            return ImmutableList.<com.google.inject.Module>of(
                    VersionedVolumeCacheService.module(),
                    SimpleBackendConnections.module());
        }
    }
    
    @DependsOn({ 
        ControlMaterializerService.class, 
        VolumeCacheService.class,
        BackendRequestService.class})
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

    @Test(timeout=10000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = BackendTestService.Module.injector();
        injector.getInstance(BackendTestService.class).startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
