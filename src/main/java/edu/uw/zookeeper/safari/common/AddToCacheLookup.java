package edu.uw.zookeeper.safari.common;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Pair;

public class AddToCacheLookup<K,V> extends Pair<ConcurrentMap<K, V>, AsyncFunction<? super K,V>> implements AsyncFunction<K,V> {
    
    public static <K,V> AddToCacheLookup<K,V> create(
            ConcurrentMap<K,V> cache,
            AsyncFunction<? super K,V> delegate) {
        return new AddToCacheLookup<K,V>(cache, delegate);
    }

    protected final static Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

    public AddToCacheLookup(
            ConcurrentMap<K,V> cache,
            AsyncFunction<? super K,V> delegate) {
        super(cache, delegate);
    }

    @Override
    public ListenableFuture<V> apply(@Nullable K input) throws Exception {
        return Futures.transform(
                second().apply(input), 
                new AddToCacheFunction(input), 
                sameThreadExecutor);
    }
    
    protected class AddToCacheFunction implements Function<V,V> {

        protected final K key;
        
        public AddToCacheFunction(
                K key) {
            this.key = key;
        }

        @Override
        public @Nullable V apply(@Nullable V input) {
            if (input != null) {
                V prev = first().putIfAbsent(key, input);
                if (prev != null) {
                    input = prev;
                }
            }
            return input;
        }
    }
}