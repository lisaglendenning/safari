package edu.uw.zookeeper.safari.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Pair;

public class SharedLookup<K,V> extends Pair<ConcurrentMap<K, ListenableFuture<V>>, AsyncFunction<? super K,V>> implements AsyncFunction<K,V> {
    
    public static <K,V> SharedLookup<K,V> create(AsyncFunction<? super K,V> delegate) {
        ConcurrentMap<K, ListenableFuture<V>> lookups = new MapMaker().makeMap();
        return new SharedLookup<K,V>(lookups, delegate,
                LogManager.getLogger(SharedLookup.class));
    }
    
    protected final static Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

    protected final Logger logger;
    
    public SharedLookup(
            ConcurrentMap<K, ListenableFuture<V>> lookups,
            AsyncFunction<? super K,V> delegate,
            Logger logger) {
        super(lookups, delegate);
        this.logger = logger;
    }
    
    @Override
    public ListenableFuture<V> apply(K input) throws Exception {
        checkNotNull(input);
        ListenableFuture<V> future = first().get(input);
        if (future == null) {
            synchronized (this) {
                future = first().get(input);
                if (future == null) {
                    logger.trace("Looking up {} ({})", input, second);
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