package edu.uw.zookeeper.safari.common;

import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Reference;

public class CachedLookup<K,V> extends AbstractPair<ConcurrentMap<K,V>, CachedFunction<K,V>> {

    public static class CacheFunction<K,V> implements Function<K,V>, Reference<ConcurrentMap<K,V>> {
        
        public static <K,V> CacheFunction<K,V> create(ConcurrentMap<K,V> cache) {
            return new CacheFunction<K,V>(cache);
        }
        
        protected final ConcurrentMap<K,V> cache;
        
        public CacheFunction(ConcurrentMap<K,V> cache) {
            this.cache = cache;
        }
        
        @Override
        public ConcurrentMap<K,V> get() {
            return cache;
        }
        
        @Override
        public V apply(K input) {
            return cache.get(input);
        }
    }
    
    public static <K,V> CachedLookup<K,V> sharedAndAdded(
            AsyncFunction<? super K, V> async,
            Logger logger) {
        ConcurrentMap<K,V> cache = new MapMaker().makeMap();
        return fromCache(cache, 
                SharedLookup.create(
                        AddToCacheLookup.create(cache, async)),
                logger);
    }

    public static <K,V> CachedLookup<K,V> shared(
            AsyncFunction<? super K, V> async,
            Logger logger) {
        return withAsync(SharedLookup.create(async), logger);
    }
    
    public static <K,V> CachedLookup<K,V> withAsync(
            AsyncFunction<? super K, V> async,
            Logger logger) {
        ConcurrentMap<K,V> cache = new MapMaker().makeMap();
        return fromCache(cache, async, logger);
    }
    
    public static <K,V> CachedLookup<K,V> fromCache(
            ConcurrentMap<K,V> cache,
            AsyncFunction<? super K, V> async,
            Logger logger) {
        CachedFunction<K,V> lookup =  
                CachedFunction.<K,V>create(
                        CacheFunction.create(cache), 
                        async,
                        logger);
        return create(cache, lookup);
    }
    
    public static <K,V> CachedLookup<K,V> create(
            ConcurrentMap<K,V> cache,
            CachedFunction<K,V> lookup) {
        return new CachedLookup<K,V>(cache, lookup);
    }
    
    public CachedLookup(
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