package edu.uw.zookeeper.orchestra.common;

import com.google.common.util.concurrent.AbstractIdleService;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Reference;

public class CachedLookupService<K,V> extends AbstractIdleService implements Reference<CachedLookup<K,V>> {

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
        materializer.register(this);
    }
    
    @Override
    protected void shutDown() throws Exception {
        materializer.unregister(this);
    }
}