package edu.uw.zookeeper.safari.region;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;

public class VolumeEntryVoteListener extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> implements Runnable, ListenableFuture<Boolean> {

    public static VolumeEntryVoteListener listen(
            VolumeLogEntryPath path,
            ControlClientService control,
            Service service) {
        VolumeEntryVoteListener listener = new VolumeEntryVoteListener(
                path, control, service);
        listener.listen();
        return listener;
    }

    protected final VolumeLogEntryPath path;
    protected final FixedQuery<?> query;
    protected final ControlClientService control;
    protected final Promise<Boolean> promise;
    
    @SuppressWarnings("unchecked")
    protected VolumeEntryVoteListener(
            VolumeLogEntryPath path,
            ControlClientService control,
            Service service) {
        super(control.materializer().cache(), 
                service, 
                control.cacheEvents(), 
                WatchMatcher.exact(
                        path.path().join(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote.LABEL), 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDataChanged,
                        Watcher.Event.EventType.NodeDeleted));
        this.control = control;
        this.path = path;
        this.promise = SettableFuturePromise.<Boolean>create();
        this.query = FixedQuery.forIterable(
                control.materializer(), 
                PathToRequests.forRequests(
                        Operations.Requests.sync(), 
                        Operations.Requests.getData().setWatch(true))
                        .apply(getWatchMatcher().getPath()));
        addListener(this, SameThreadExecutor.getInstance());
    }
    
    public VolumeLogEntryPath path() {
        return path;
    }
    
    @Override
    public void run() {
        if (!isDone()) {
            if (isRunning()) {
                Optional<Boolean> vote;
                cache.lock().readLock().lock();
                try {
                    final ControlZNode<?> node = cache.cache().get(getWatchMatcher().getPath());
                    vote = Optional.fromNullable((Boolean) ((node == null) ? null : node.data().get()));
                } finally {
                    cache.lock().readLock().unlock();
                }
                if (vote.isPresent()) {
                    promise.set(vote.get());
                }
            } else {
                cancel(false);
            }
        } else {
            stopping(state());
        }
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        if (event.getEventType() == Watcher.Event.EventType.NodeDeleted) {
            stopping(state());
        } else {
            run();
        }
    }
    
    @Override
    public void running() {
        run();
        if (!isDone()) {
            final Watchers.RunnableWatcher<?> watcher = Watchers.RunnableWatcher.listen(
                    new Runnable() {
                        @Override
                        public void run() {
                            Watchers.Query.call(query, VolumeEntryVoteListener.this);
                        }
                    }, 
                    service, 
                    control.notifications(), 
                    WatchMatcher.exact(
                            getWatchMatcher().getPath(), 
                            Watcher.Event.EventType.NodeCreated,
                            Watcher.Event.EventType.NodeDeleted,
                            Watcher.Event.EventType.NodeDataChanged));
            addListener(
                    new Runnable() {
                        @Override
                        public void run() {
                            watcher.stopping(watcher.state());
                        }
                    },
                    SameThreadExecutor.getInstance());
        }
    }
    
    @Override
    public void stopping(Service.State from) {
        super.stopping(from);
        if (!isDone()) {
            cancel(false);
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
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(path).addValue(ToStringListenableFuture.toString(this)).toString();
    }
}
