package edu.uw.zookeeper.safari.common;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.AbstractPair;

public class CachedFunction<I,O> extends AbstractPair<Function<? super I,O>, AsyncFunction<? super I,O>> implements AsyncFunction<I,O> {

    public static <I,O> CachedFunction<I,O> create(
            Function<? super I,O> cached, 
            AsyncFunction<? super I,O> async,
            Logger logger) {
        return new CachedFunction<I,O>(cached, async, logger);
    }
    
    protected final Logger logger;
    
    public CachedFunction(
            Function<? super I,O> cached, AsyncFunction<? super I,O> async,
            Logger logger) {
        super(cached, async);
        this.logger = logger;
    }
    
    public Function<? super I,O> cached() {
        return first;
    }
    
    public AsyncFunction<? super I,O> async() {
        return second;
    }

    @Override
    public ListenableFuture<O> apply(I input) throws Exception {
        logger.trace("Applying {} =>", input);
        O output = first.apply(input);
        ListenableFuture<O> future;
        if (output != null) {
            logger.trace("Cached result {} => {} ({})", input, output, first);
            future = Futures.immediateFuture(output);
        } else {
            logger.trace("Looking up {} => ? ({})", input, second);
            future = second.apply(input);
        }
        return future;
    }
}
