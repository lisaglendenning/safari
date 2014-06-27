package edu.uw.zookeeper.safari.region;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.Watcher;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;

public class VolumeEntryVoteListener extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> implements Runnable, ListenableFuture<Boolean> {

    public static VolumeEntryVoteListener listen(
            VolumeLogEntryPath path,
            Promise<Boolean> promise,
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            Service service, 
            WatchListeners watch) {
        VolumeEntryVoteListener listener = new VolumeEntryVoteListener(path, promise, cache, service, watch);
        listener.listen();
        return listener;
    }

    protected final VolumeLogEntryPath path;
    protected final LockableZNodeCache<ControlZNode<?>,?,?> cache;
    protected final Promise<Boolean> promise;
    
    protected VolumeEntryVoteListener(
            VolumeLogEntryPath path,
            Promise<Boolean> promise,
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            Service service, 
            WatchListeners watch) {
        super(cache, service, watch, 
                WatchMatcher.exact(
                        path.path().join(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote.LABEL), 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDataChanged));
        this.path = path;
        this.promise = promise;
        this.cache = cache;
        addListener(this, SameThreadExecutor.getInstance());
    }
    
    public VolumeLogEntryPath path() {
        return path;
    }
    
    @Override
    public void run() {
        if (!isDone()) {
            if (service.isRunning()) {
                cache.lock().readLock().lock();
                try {
                    Boolean vote = (Boolean) cache.cache().get(getWatchMatcher().getPath()).data().get();
                    if (vote != null) {
                        promise.set(vote);
                    }
                } finally {
                    cache.lock().readLock().unlock();
                }
            } else {
                cancel(false);
            }
        } else {
            stopping(service.state());
        }
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        run();
    }
    
    @Override
    public void running() {
        run();
    }
    
    @Override
    public void stopping(Service.State from) {
        super.stopping(from);
        if (!promise.isDone()) {
            promise.cancel(false);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return promise.cancel(mayInterruptIfRunning);
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        return promise.get();
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return promise.get(timeout, unit);
    }

    @Override
    public boolean isCancelled() {
        return promise.isCancelled();
    }

    @Override
    public boolean isDone() {
        return promise.isDone();
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        promise.addListener(listener, executor);
    }
}
