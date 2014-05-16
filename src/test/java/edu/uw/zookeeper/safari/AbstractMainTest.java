package edu.uw.zookeeper.safari;

import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import edu.uw.zookeeper.common.ServiceMonitor;

public abstract class AbstractMainTest {

    protected final Logger logger;
    
    protected AbstractMainTest() {
        this.logger = LogManager.getLogger(this);
    }
    
    public Injector monitored(
            final Iterable<? extends Component<?>> components,
            final Class<? extends Provider<ServiceMonitor>> monitor) {
        return Guice.createInjector(
                Modules.AnnotatedComponentsModule.forComponents(components),
                Modules.ComponentMonitorProvider.create(monitor));
    }
    
    public void pauseWithComponents(
            final Iterable<? extends Component<?>> components, 
            final long pause) throws Exception {
        pauseWithService(
                monitored(components, Modules.StoppingServiceMonitorProvider.class), 
                pause);
    }
    
    public void pauseWithService(
            final Injector injector, 
            final long pause) throws Exception {
         callWithService(
                 injector,
                 new Callable<Void>() {
                     @Override
                     public Void call() throws InterruptedException {
                         logger.info("Sleeping for {} ms", pause);
                         Thread.sleep(pause);
                        return null;
                     }
                 });
    }
    
    public <V> V callWithService(
            final Injector injector, 
            final Callable<V> callable) throws Exception {
         return CallableService.create(
                injector.getInstance(Service.class),
                callable,
                logger).call();
    }
}
