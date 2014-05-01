package edu.uw.zookeeper.safari.common;

import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;

public class LockingCachedFunction<I,O> extends CachedFunction<I,O> {

    public static <I,O> LockingCachedFunction<I,O> create(
            Lock lock,
            Function<? super I,O> cached,
            AsyncFunction<? super I,O> async, 
            Logger logger) {
        return new LockingCachedFunction<I,O>(lock, cached, async, logger);
    }
    
    private final Lock lock;
    
    public LockingCachedFunction(
            Lock lock,
            Function<? super I,O> cached,
            AsyncFunction<? super I,O> async, 
            Logger logger) {
        super(cached, async, logger);
        this.lock = lock;
    }
    
    public Lock lock() {
        return lock;
    }

    @Override
    public ListenableFuture<O> apply(I input) throws Exception {
        lock.lock();
        try {
            return super.apply(input);
        } finally {
            lock.unlock();
        }
    }
}