package edu.uw.zookeeper.safari.peer;


import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlTest;
import edu.uw.zookeeper.safari.peer.PeerConnectionsService;

@RunWith(JUnit4.class)
public class PeerTest {
    
    public static Injector injector() {
        return injector(ControlTest.injector());
    }
    
    public static Injector injector(Injector parent) {
        return parent.createChildInjector(
                SimplePeerConnectionsModule.create());
    }
    
    public static class SimplePeerConnectionsModule extends PeerConnectionsService.Module {

        public static SimplePeerConnectionsModule create() {
            return new SimplePeerConnectionsModule();
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(SimplePeerConfiguration.create());
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = injector();
        injector.getInstance(ControlMaterializerService.class).startAsync().awaitRunning();
        injector.getInstance(PeerConnectionsService.class).startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
