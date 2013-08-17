package edu.uw.zookeeper.orchestra.peer;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.DependentModule;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlTest;

@RunWith(JUnit4.class)
public class PeerTest {
    
    public static class PeerTestModule extends DependentModule {
    
        public PeerTestModule() {
        }

        @Override
        protected Module[] getModules() {
            Module[] modules = {
                    ControlTest.module(),
                    SimplePeerConfiguration.create(),
                    PeerConnectionsService.module()};
            return modules;
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        PeerTestModule module = new PeerTestModule();
        Injector injector = Guice.createInjector(module);
        injector.getInstance(ControlTest.ControlTestModule.class).start(injector.getInstance(ServiceLocator.class));
        injector.getInstance(PeerConnectionsService.class).start().get();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.start().get();
        Thread.sleep(500);
        monitor.stop().get();
    }
}
