package edu.uw.zookeeper.safari;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
    
    protected AbstractMainTest(Logger logger) {
        this.logger = logger;
    }

    public Injector nonstopping(final Iterable<? extends Component<?>> components, final com.google.inject.Module...modules) {
        return nonstopping(Iterables.concat(ImmutableList.of(Modules.AnnotatedComponentsModule.forComponents(components)), Arrays.asList(modules)));
    }

    public Injector stopping(final Iterable<? extends Component<?>> components, final com.google.inject.Module...modules) {
        return stopping(Iterables.concat(ImmutableList.of(Modules.AnnotatedComponentsModule.forComponents(components)), Arrays.asList(modules)));
    }

    public Injector nonstopping(final com.google.inject.Module...modules) {
        return nonstopping(Arrays.asList(modules));
    }

    public Injector stopping(final com.google.inject.Module...modules) {
        return stopping(Arrays.asList(modules));
    }
    
    public Injector nonstopping(
            final Iterable<? extends com.google.inject.Module> modules) {
        return monitored(modules, 
                Modules.NonStoppingServiceMonitorProvider.class);
    }
    
    public Injector stopping(
            final Iterable<? extends com.google.inject.Module> modules) {
        return monitored(modules, 
                Modules.StoppingServiceMonitorProvider.class);
    }
    
    public Injector monitored(
            final Iterable<? extends com.google.inject.Module> modules,
            final Class<? extends Provider<ServiceMonitor>> monitor) {
        return injector(
                Iterables.concat(modules, 
                        ImmutableList.of(
                                Modules.ComponentMonitorProvider.create(monitor))));
    }
    
    public Injector injector(com.google.inject.Module...modules) {
        return Guice.createInjector(modules);
    }
    
    public Injector injector(Iterable<? extends com.google.inject.Module> modules) {
        return Guice.createInjector(modules);
    }
    
    public void pauseWithComponents(
            final Iterable<? extends Component<?>> components, 
            final long pause) throws Exception {
        pauseWithService(
                stopping(components), pause);
    }
    
    public void pauseWithService(
            final Injector injector, 
            final long pause) throws Exception {
         callWithService(
                 injector,
                 new Callable<Void>() {
                     @Override
                     public Void call() throws InterruptedException {
                         pause(pause);
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
    
    protected void pause(long ms) throws InterruptedException {
        pause(ms, TimeUnit.MILLISECONDS);
    }
    
    protected void pause(long value, TimeUnit unit) throws InterruptedException {
        logger.info("Sleeping for {} {}", value, unit);
        Thread.sleep(TimeUnit.MILLISECONDS.convert(value, unit));
    }
}
