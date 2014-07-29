package edu.uw.zookeeper.safari.region;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class VolumeEntryCommitListener extends AbstractWatchListener implements ListenableFuture<Optional<Sequential<String,?>>>, Runnable {

    public static VolumeEntryCommitListener listen(
            Service service,
            WatchListeners watch,
            VersionedId volume,
            LockableZNodeCache<ControlZNode<?>, Records.Request, ?> cache) {
        return listen(service, watch, committed(volume, cache));
    }
    
    public static VolumeEntryCommitListener listen(
            Service service,
            WatchListeners watch,
            VotedEntriesCommitted callable) {
        VolumeEntryCommitListener listener = create(service, watch, callable);
        listener.listen();
        return listener;
    }
    
    public static VotedEntriesCommitted committed(
            VersionedId volume,
            LockableZNodeCache<ControlZNode<?>, Records.Request, ?> cache) {
        return new VotedEntriesCommitted(volume, cache);
    }

    public static VolumeEntryCommitListener create(
            Service service,
            WatchListeners watch,
            VotedEntriesCommitted callable) {
        return new VolumeEntryCommitListener(
                CallablePromiseTask.create(
                        callable, 
                        SettableFuturePromise.<Optional<Sequential<String, ?>>>create()), 
                service, watch);
    }
    
    protected final CallablePromiseTask<VotedEntriesCommitted,Optional<Sequential<String,?>>> delegate;
    
    protected VolumeEntryCommitListener(
            CallablePromiseTask<VotedEntriesCommitted,Optional<Sequential<String,?>>> delegate,
            Service service, 
            WatchListeners watch) {
        super(service, watch, 
                WatchMatcher.prefix(
                        ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit.pathOf(delegate.task().volume().getValue(), delegate.task().volume().getVersion()), 
                        Watcher.Event.EventType.NodeCreated, Watcher.Event.EventType.NodeDataChanged));
        this.delegate = delegate;
        addListener(this, SameThreadExecutor.getInstance());
    }

    @Override
    public void run() {
        if (!isDone()) {
            if (isRunning()) {
                delegate.run();
            } else {
                cancel(false);
            }
        } else {
            stopping(state());
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
        if (!isDone()) {
            cancel(false);
        }
    }

    @Override
    public boolean cancel(boolean arg0) {
        return delegate.cancel(arg0);
    }

    @Override
    public Optional<Sequential<String, ?>> get() throws InterruptedException,
            ExecutionException {
        return delegate.get();
    }

    @Override
    public Optional<Sequential<String, ?>> get(long arg0, TimeUnit arg1)
            throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(arg0, arg1);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        delegate.addListener(listener, executor);
    }

    public static final class VotedEntriesCommitted implements Callable<Optional<Optional<Sequential<String,?>>>> {
        
        protected final Logger logger;
        protected final VersionedId volume;
        protected final LockableZNodeCache<ControlZNode<?>, Records.Request, ?> cache;
        
        public VotedEntriesCommitted(
                VersionedId volume,
                LockableZNodeCache<ControlZNode<?>, Records.Request, ?> cache) {
            this.logger = LogManager.getLogger(this);
            this.volume = volume;
            this.cache = cache;
        }
        
        public VersionedId volume() {
            return volume;
        }
        
        @Override
        public Optional<Optional<Sequential<String,?>>> call() throws Exception {
            Optional<Sequential<String,?>> committed = Optional.absent();
            cache.lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes.Volume.Log.Version node = ControlSchema.Safari.Volumes.Volume.Log.Version.fromTrie(
                        cache.cache(), volume.getValue(), volume.getVersion());
                if (node != null) {
                    for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry entry: node.entries().values()) {
                        if ((entry.vote() != null) && ((entry.commit() == null) || (entry.commit().data().get() == null))) {
                            return Optional.absent();
                        }
                        if (entry.commit().data().get().booleanValue()) {
                            assert (!committed.isPresent());
                            committed = Optional.<Sequential<String,?>>of(entry.name());
                            logger.info("{} COMMITTED for volume {}", committed.get(), volume());
                        }
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
            return Optional.of(committed);
        }
    }
}