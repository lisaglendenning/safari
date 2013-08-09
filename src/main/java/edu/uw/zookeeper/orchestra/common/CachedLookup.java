package edu.uw.zookeeper.orchestra.common;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;

import edu.uw.zookeeper.common.AbstractPair;

public class CachedLookup<K,V> extends AbstractPair<ConcurrentMap<K,V>, CachedFunction<K,V>> {

    public static <K,V> Function<K,V> newLookup(
            final Map<K,V> cache) {
        return new Function<K,V>() {
            @Override 
            public @Nullable V apply(@Nullable K input) {
                return cache.get(input);
            }
        };
    }
    
    public static <K,V> CachedFunction<K,V> newLookup(
            Map<K,V> cache, AsyncFunction<K,V> async) {
        return CachedFunction.create(newLookup(cache), async);
    }
    
    public static <K,V> CachedLookup<K,V> create(AsyncFunction<K,V> async) {
        ConcurrentMap<K,V> cache = new MapMaker().makeMap();
        return new CachedLookup<K,V>(cache, newLookup(cache, async));
    }

    public static <K,V> CachedLookup<K,V> create(
            ConcurrentMap<K,V> cache,
            CachedFunction<K,V> lookup) {
        return new CachedLookup<K,V>(cache, lookup);
    }

    protected CachedLookup(
            ConcurrentMap<K,V> cache,
            CachedFunction<K,V> lookup) {
        super(cache, lookup);
    }
    
    public ConcurrentMap<K,V> asCache() {
        return first;
    }
    
    public CachedFunction<K,V> asLookup() {
        return second;
    }
}