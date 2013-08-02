package edu.uw.zookeeper.orchestra;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;

public class CachedFunction<I,O> extends Pair<Function<I,O>, AsyncFunction<I,O>> implements AsyncFunction<I,O> {

    public static <I,O> CachedFunction<I,O> create(
            Function<I,O> first, AsyncFunction<I,O> second) {
        return new CachedFunction<I,O>(first, second);
    }
    
    public CachedFunction(
            Function<I,O> first, AsyncFunction<I,O> second) {
        super(first, second);
    }

    @Override
    public ListenableFuture<O> apply(I input) throws Exception {
        O output = first.apply(input);
        if (output != null) {
            return Futures.immediateFuture(output);
        } else {
            return second.apply(input);
        }
    }
}
