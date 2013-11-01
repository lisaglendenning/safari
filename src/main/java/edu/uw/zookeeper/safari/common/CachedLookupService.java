package edu.uw.zookeeper.safari.common;

import com.google.common.util.concurrent.AbstractIdleService;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Reference;

public abstract class CachedLookupService<K,V> extends AbstractIdleService implements Reference<CachedLookup<K,V>>, Materializer.CacheSessionListener {

    protected final Materializer<?> materializer;
    protected final CachedLookup<K,V> cache;
    
    public CachedLookupService(
            Materializer<?> materializer,
            CachedLookup<K,V> cache) {
        this.materializer = materializer;
        this.cache = cache;
    }
    
    @Override
    public CachedLookup<K,V> get() {
        return cache;
    }

    @Override
    protected void startUp() throws Exception {
        materializer.subscribe(this);
    }
    
    @Override
    protected void shutDown() throws Exception {
        materializer.unsubscribe(this);
    }
}