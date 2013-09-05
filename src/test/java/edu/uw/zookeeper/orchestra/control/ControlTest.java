package edu.uw.zookeeper.orchestra.control;


import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;

import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.common.GuiceRuntimeModule;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;

@RunWith(JUnit4.class)
public class ControlTest {
    
    public static Injector injector() {
        return Guice.createInjector(
                GuiceRuntimeModule.create(DefaultRuntimeModule.defaults()),
                IntraVmAsNetModule.create(),
                SimpleControlMaterializerModule.create());
    }
    
    public static class SimpleControlMaterializerModule extends ControlMaterializerService.Module {

        public static SimpleControlMaterializerModule create() {
            return new SimpleControlMaterializerModule();
        }
        
        public SimpleControlMaterializerModule() {
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(SimpleControlConnectionsService.module());
        }
    }

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = injector();
        injector.getInstance(ControlMaterializerService.class).startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
