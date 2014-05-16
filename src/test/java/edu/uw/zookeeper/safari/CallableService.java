package edu.uw.zookeeper.safari;

import java.util.concurrent.Callable;

import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Service;

public class CallableService<V> implements Callable<V> {

    public static <V> CallableService<V> create(
            Service service, Callable<? extends V> callable, Logger logger) {
        return new CallableService<V>(service, callable, logger);
    }
    
    protected final Service service;
    protected final Callable<? extends V> callable;
    protected final Logger logger;
    
    public CallableService(Service service, Callable<? extends V> callable, Logger logger) {
        this.service = service;
        this.callable = callable;
        this.logger = logger;
    }

    public Service service() {
        return service;
    }
    
    public Callable<? extends V> callable() {
        return callable;
    }
    
    @Override
    public V call() throws Exception {
        service().startAsync().awaitRunning();
        try {
            return callable().call();
        } catch (Exception e) {
            logger.warn("", e);
            throw e;
        } finally {
            service().stopAsync().awaitTerminated();
        }
    }
}