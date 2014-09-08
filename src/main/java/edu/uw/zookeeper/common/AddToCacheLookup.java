package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.common.Pair;


public class AddToCacheLookup<K,V> extends Pair<ConcurrentMap<K, V>, AsyncFunction<? super K,V>> implements AsyncFunction<K,V> {
    
    public static <K,V> AddToCacheLookup<K,V> create(
            ConcurrentMap<K,V> cache,
            AsyncFunction<? super K,V> delegate) {
        return new AddToCacheLookup<K,V>(cache, delegate, 
                LogManager.getLogger(AddToCacheLookup.class));
    }

    protected final Logger logger;
    
    public AddToCacheLookup(
            ConcurrentMap<K,V> cache,
            AsyncFunction<? super K,V> delegate,
            Logger logger) {
        super(cache, delegate);
        this.logger = logger;
    }

    @Override
    public ListenableFuture<V> apply(K input) throws Exception {
        checkNotNull(input);
        return Futures.transform(
                second().apply(input), 
                new AddToCacheFunction(input));
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
                    logger.trace("Ignoring {} ({} => {})", input, key, prev);
                    input = prev;
                } else {
                    logger.trace("Caching {} => {}", key, input);
                }
            }
            return input;
        }
    }
}