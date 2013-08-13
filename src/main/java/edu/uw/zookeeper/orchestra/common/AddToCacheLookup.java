package edu.uw.zookeeper.orchestra.common;

import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;

public class AddToCacheLookup<K,V> extends Pair<ConcurrentMap<K, V>, AsyncFunction<? super K,V>> implements AsyncFunction<K,V> {
    
    public static <K,V> AddToCacheLookup<K,V> create(
            ConcurrentMap<K,V> cache,
            AsyncFunction<? super K,V> delegate) {
        return new AddToCacheLookup<K,V>(cache, delegate);
    }
    
    public AddToCacheLookup(
            ConcurrentMap<K,V> cache,
            AsyncFunction<? super K,V> delegate) {
        super(cache, delegate);
    }

    @Override
    public ListenableFuture<V> apply(@Nullable K input) throws Exception {
        return Futures.transform(second().apply(input), addToCache(input));
    }
    
    protected Function<V,V> addToCache(K key) {
        return AddToCacheFunction.create(key, first());
    }
    
    public static class AddToCacheFunction<K,V> extends Pair<K, ConcurrentMap<K, V>> implements Function<V,V> {

        public static <K,V> AddToCacheFunction<K,V> create(
                K key,
                ConcurrentMap<K,V> cache) {
            return new AddToCacheFunction<K,V>(key, cache);
        }
        
        public AddToCacheFunction(
                K key,
                ConcurrentMap<K,V> cache) {
            super(key, cache);
        }

        @Override
        public @Nullable V apply(@Nullable V input) {
            if (input != null) {
                V prev = second().putIfAbsent(first(), input);
                if (prev != null) {
                    input = prev;
                }
            }
            return input;
        }
    }
}