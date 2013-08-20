package edu.uw.zookeeper.orchestra.common;

import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;

@Singleton
public class DependentServiceMonitor {

    public static DependentServiceMonitor create(
            ServiceMonitor monitor,
            ServiceLocator locator) {
        return new DependentServiceMonitor(monitor, locator);
    }

    public static Iterator<Class<?>> dependentServiceTypes(DependsOn depends) {
        return Iterators.filter(
                Iterators.forArray(depends.value()), 
                new Predicate<Class<?>>() {
                    @Override
                    public boolean apply(Class<?> input) {
                        return Service.class.isAssignableFrom(input);
                    }
                });
    }
    
    private final Logger logger;
    private final ServiceMonitor monitor;
    private final ServiceLocator locator;

    @Inject
    public DependentServiceMonitor(
            ServiceMonitor monitor,
            ServiceLocator locator) {
        this.logger = LogManager.getLogger(getClass());
        this.monitor = monitor;
        this.locator = locator;
    }

    public ListenableFuture<List<Service.State>> start(DependsOn depends) {
        StartDependsOn task = new StartDependsOn(depends);
        task.run();
        return task;
    }
    
    public ListenableFuture<Service.State> start(Class<? extends Service> type) {
        DependsOn depends = type.getAnnotation(DependsOn.class);
        if (depends != null) {
            return Futures.transform(start(depends), new Start(type));
        } else {
            return getInstance(type).start();
        }
    }
    
    public <T extends Service> T getInstance(Class<T> type) {
        T service = locator.getInstance(type);
        if (! monitor.isMonitoring(service)) {
            monitor.add(service);
        }
        return service;
    }

    protected class StartDependsOn extends RunnablePromiseTask<Iterator<Class<?>>, List<Service.State>> {
    
        protected final List<ListenableFuture<Service.State>> futures;
        
        public StartDependsOn(
                DependsOn task) {
            this(task, LoggingPromise.create(logger, SettableFuturePromise.<List<Service.State>>create()));
        }
        
        public StartDependsOn(
                DependsOn task, Promise<List<Service.State>> delegate) {
            super(dependentServiceTypes(task), delegate);
            this.futures = Lists.newArrayListWithCapacity(task.value().length);
        }
        
        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancel = super.cancel(mayInterruptIfRunning);
            if (cancel) {
                for (ListenableFuture<Service.State> future: futures) {
                    if (! future.isDone()) {
                        future.cancel(mayInterruptIfRunning);
                    }
                }
            }
            return cancel;
        }
    
        @Override
        public synchronized Optional<List<Service.State>> call() throws Exception {
            // don't start the next if the previous future failed
            if (! futures.isEmpty()) {
                ListenableFuture<Service.State> future = futures.get(futures.size() - 1);
                if (future.isDone()) {
                    future.get();
                } else {
                    return Optional.absent();
                }
            }
            if (task.hasNext()) {
                @SuppressWarnings("unchecked")
                Class<? extends Service> next = (Class<? extends Service>) task.next();
                ListenableFuture<Service.State> future = start(next);
                futures.add(future);
                future.addListener(this, MoreExecutors.sameThreadExecutor());
                return Optional.absent();
            } else {
                List<Service.State> results = Lists.newArrayListWithCapacity(futures.size());
                for (ListenableFuture<Service.State> future: futures) {
                    if (future.isDone()) {
                        results.add(future.get());
                    } else {
                        return Optional.absent();
                    }
                }
                return Optional.of(results);
            }
        }
    }
    
    protected class Start implements AsyncFunction<Object, Service.State> {
    
        protected final Class<? extends Service> task;
        
        public Start(Class<? extends Service> task) {
            this.task = task;
        }

        @Override
        public ListenableFuture<Service.State> apply(Object input)
                throws Exception {
            return getInstance(task).start();
        }
    }
}
