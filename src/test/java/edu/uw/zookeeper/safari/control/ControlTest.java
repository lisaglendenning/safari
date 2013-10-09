package edu.uw.zookeeper.safari.control;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.safari.common.GuiceRuntimeModule;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.net.IntraVmAsNetModule;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;
import edu.uw.zookeeper.safari.peer.protocol.JacksonSerializer;

@RunWith(JUnit4.class)
public class ControlTest {
    
    public static Injector injector() {
        return injector(Guice.createInjector(
                GuiceRuntimeModule.create(DefaultRuntimeModule.defaults()),
                IntraVmAsNetModule.create(),
                JacksonModule.create()));
    }

    public static Injector injector(Injector parent) {
        return parent.createChildInjector(
                SimpleControlConnectionsService.module(),
                module());
    }
    
    public static ControlModule module() {
        return new ControlModule();
    }

    public static class ControlModule extends AbstractModule {

        public ControlModule() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public ControlMaterializerService getControlMaterializerService(
                Injector injector,
                JacksonSerializer serializer,
                ControlConnectionsService<?> connections) {
            return ControlMaterializerService.newInstance(injector, serializer, connections);
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
