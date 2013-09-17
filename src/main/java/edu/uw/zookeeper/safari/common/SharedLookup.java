package edu.uw.zookeeper.safari.common;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Pair;

public class SharedLookup<K,V> extends Pair<ConcurrentMap<K, ListenableFuture<V>>, AsyncFunction<? super K,V>> implements AsyncFunction<K,V> {
    
    public static <K,V> SharedLookup<K,V> create(AsyncFunction<? super K,V> delegate) {
        ConcurrentMap<K, ListenableFuture<V>> lookups = new MapMaker().makeMap();
        return new SharedLookup<K,V>(lookups, delegate);
    }
    
    public SharedLookup(
            ConcurrentMap<K, ListenableFuture<V>> lookups,
            AsyncFunction<? super K,V> delegate) {
        super(lookups, delegate);
    }
    
    protected final static Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

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
            
            second().addListener(this, sameThreadExecutor);
        }
        
        @Override
        public void run() {
            SharedLookup.this.first().remove(first(), second());
        }
    }
}