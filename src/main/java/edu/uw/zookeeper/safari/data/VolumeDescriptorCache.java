package edu.uw.zookeeper.safari.data;

import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;

import com.google.common.collect.Iterators;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.CachedLookup;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.volume.VolumeDescriptor;

public final class VolumeDescriptorCache implements FutureCallback<VolumeDescriptor> {

    public static VolumeDescriptorCache create() {
        return new VolumeDescriptorCache();
    }
    
    private final CachedLookup<Identifier, ZNodePath> cache;
    private final ConcurrentMap<Identifier, Lookup> lookups;

    protected VolumeDescriptorCache() {
        this.lookups = new MapMaker().makeMap();
        this.cache = CachedLookup.withAsync(
                new AsyncFunction<Identifier, ZNodePath>() {
                    @Override
                    public ListenableFuture<ZNodePath> apply(
                            final Identifier id) {
                        Lookup lookup = lookups.get(id);
                        if (lookup == null) {
                            lookup = new Lookup(id, SettableFuturePromise.<ZNodePath>create());
                            Lookup existing = lookups.putIfAbsent(id, lookup);
                            if (existing != null) {
                                lookup.cancel(false);
                                lookup = existing;
                            }
                        }
                        ZNodePath value = cache.asLookup().cached().apply(id);
                        if (value != null) {
                            lookup.set(value);
                        }
                        return lookup;
                    }
                }, 
                LogManager.getLogger(getClass()));
    }
    
    public ConcurrentMap<Identifier, ZNodePath> asCache() {
        return cache.asCache();
    }
    
    public CachedFunction<Identifier, ZNodePath> asLookup() {
        return cache.asLookup();
    }
    
    public ZNodePath remove(Identifier id) {
        Lookup lookup = lookups.get(id);
        if (lookup != null) {
            lookup.cancel(true);
        }
        return asCache().remove(id);
    }
    
    public void clear() {
        onFailure(new CancellationException());
    }
    
    @Override
    public void onSuccess(VolumeDescriptor result) {
        if (asCache().putIfAbsent(result.getId(), result.getPath()) == null) {
            Lookup lookup = lookups.get(result.getId());
            if (lookup != null) {
                lookup.set(result.getPath());
            }
        }
    }

    @Override
    public void onFailure(Throwable t) {
        Iterator<Lookup> itr = Iterators.consumingIterator(lookups.values().iterator());
        while (itr.hasNext()) {
            if (t instanceof CancellationException) {
                itr.next().cancel(true);
            } else {
                itr.next().setException(t);
            }
        }
        asCache().clear();
    }

    protected final class Lookup extends PromiseTask<Identifier, ZNodePath> implements Runnable {

        protected Lookup(
                Identifier volume,
                Promise<ZNodePath> delegate) {
            super(volume, delegate);
            addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public boolean set(ZNodePath path) {
            ZNodePath prev = asCache().putIfAbsent(task(), path);
            assert ((prev == null) || prev.equals(path));
            return super.set(path);
        }
        
        @Override
        public void run() {
            if (isDone()) {
                lookups.remove(task(), this);
            }
        }
    }
}
