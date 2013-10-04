package edu.uw.zookeeper.safari.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;

public class CachedFunction<I,O> extends Pair<Function<? super I,O>, AsyncFunction<? super I,O>> implements AsyncFunction<I,O> {

    public static <I,O> CachedFunction<I,O> create(
            Function<? super I,O> first, 
            AsyncFunction<? super I,O> second) {
        return new CachedFunction<I,O>(first, second,  
                LogManager.getLogger(CachedFunction.class));
    }
    
    protected final Logger logger;
    
    public CachedFunction(
            Function<? super I,O> first, AsyncFunction<? super I,O> second,
            Logger logger) {
        super(first, second);
        this.logger = logger;
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
