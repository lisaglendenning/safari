package edu.uw.zookeeper.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Service;

public class LoggingServiceListener<T> extends Service.Listener {

    public static <T> LoggingServiceListener<T> create(T delegate) {
        return new LoggingServiceListener<T>(delegate);
    }
    
    public static <T> LoggingServiceListener<T> create(T delegate, Logger logger) {
        return new LoggingServiceListener<T>(delegate, logger);
    }

    protected final T delegate;
    protected final Logger logger;

    @SuppressWarnings("unchecked")
    protected LoggingServiceListener() {
        this.logger = LogManager.getLogger(this);
        this.delegate = (T) this;
    }
    
    @SuppressWarnings("unchecked")
    protected LoggingServiceListener(
            Logger logger) {
        this.logger = logger;
        this.delegate = (T) this;
    }
    
    protected LoggingServiceListener(
            T delegate) {
        this.logger = LogManager.getLogger(this);
        this.delegate = delegate;
    }
    
    protected LoggingServiceListener(
            T delegate,
            Logger logger) {
        this.logger = logger;
        this.delegate = delegate;
    }
    
    public Logger logger() {
        return logger;
    }
    
    public T delegate() {
        return delegate;
    }

    @Override
    public void starting() {
        logger.debug("STARTING ({})", delegate);
    }

    @Override
    public void running() {
        logger.debug("RUNNING ({})", delegate);
    }

    @Override
    public void stopping(Service.State from) {
        logger.debug("STOPPING ({}) ({})", delegate, from);
    }
    
    @Override
    public void terminated(Service.State from) {
        logger.debug("TERMINATED ({}) ({})", delegate, from);
    }

    @Override
    public void failed(Service.State from, Throwable failure) {
        logger.debug("FAILED ({}) ({})", delegate, from, failure);
    }
}
