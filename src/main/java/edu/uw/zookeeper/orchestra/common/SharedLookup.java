package edu.uw.zookeeper.orchestra.common;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Pair;

public class SharedLookup<K,V> extends Pair<ConcurrentMap<K, ListenableFuture<V>>, AsyncFunction<K,V>> implements AsyncFunction<K,V> {
    
    public static <K,V> SharedLookup<K,V> create(AsyncFunction<K,V> delegate) {
        ConcurrentMap<K, ListenableFuture<V>> lookups = new MapMaker().makeMap();
        return new SharedLookup<K,V>(lookups, delegate);
    }
    
    public SharedLookup(
            ConcurrentMap<K, ListenableFuture<V>> lookups,
            AsyncFunction<K,V> delegate) {
        super(lookups, delegate);
    }

    @Override
    public ListenableFuture<V> apply(K input) throws Exception {
        ListenableFuture<V> future = first().get(input);
        if (future == null) {
            synchronized (this) {
                future = first().get(input);
                if (future == null) {
                    future = second().apply(input);
                    if (! future.isDone()) {
                        first().put(input, future);
                        new FutureListener(input, future);
                    }
                }
            }
        }
        return future;
    }
    
    protected class FutureListener extends Pair<K, ListenableFuture<V>> implements Runnable {

        public FutureListener(
                K key,
                ListenableFuture<V> value) {
            super(key, value);
            
            second().addListener(this, MoreExecutors.sameThreadExecutor());
        }
        
        @Override
        public void run() {
            SharedLookup.this.first().remove(first(), second());
        }
    }
}