package edu.uw.zookeeper.safari.data;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;

public class VolumeBranchListener extends PromiseTask<Pair<Identifier,UnsignedLong>,VersionedVolume> implements Runnable, Callable<Optional<VersionedVolume>> {
    
    public static VolumeBranchListener fromCache(
            final Pair<Identifier,UnsignedLong> version, 
            final @Nullable VolumeState state,
            final CachedFunction<Identifier, ZNodePath> idToPath) {

        ImmutableMap.Builder<Identifier, ListenableFuture<ZNodePath>> builder = ImmutableMap.builder();
        try {
            builder.put(version.first(), idToPath.apply(version.first()));
            for (Identifier leaf: state.getLeaves()) {
                builder.put(leaf, idToPath.apply(leaf));
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        ImmutableMap<Identifier, ListenableFuture<ZNodePath>> paths = builder.build();
        
        VolumeBranchListener instance = new VolumeBranchListener(version, state, paths, SettableFuturePromise.<VersionedVolume>create());
        instance.run();
        return instance;
    }
    
    private final CallablePromiseTask<VolumeBranchListener, VersionedVolume> delegate;
    private final VolumeState state;
    private final ImmutableMap<Identifier, ListenableFuture<ZNodePath>> paths;
    private final ListenableFuture<?> future;
    
    public VolumeBranchListener(
            Pair<Identifier,UnsignedLong> version, 
            @Nullable VolumeState state,
            ImmutableMap<Identifier, ListenableFuture<ZNodePath>> paths,
            Promise<VersionedVolume> delegate) {
        super(version, delegate);
        this.state = state;
        this.paths = paths;
        this.future = Futures.successfulAsList(paths.values());
        this.delegate = CallablePromiseTask.create(this, this);
        future.addListener(this, SameThreadExecutor.getInstance());
    }
    
    public @Nullable VolumeState getVolumeState() {
        return state;
    }

    @Override
    public Optional<VersionedVolume> call() throws Exception {
        if (! future.isDone()) {
            return Optional.absent();
        }
        try {
            future.get();
            final ZNodePath path = paths.get(task()).get();
            final VolumeDescriptor descriptor = VolumeDescriptor.valueOf(task().first(), path);
            final VersionedVolume volume;
            if (state != null) {
                final ImmutableBiMap.Builder<ZNodeName, Identifier> leaves = ImmutableBiMap.builder();
                for (Map.Entry<Identifier, ListenableFuture<ZNodePath>> e: paths.entrySet()) {
                    ZNodeName remaining = e.getValue().get().suffix(path.length());
                    if (remaining instanceof EmptyZNodeLabel) {
                        continue;
                    }
                    leaves.put(remaining, e.getKey());
                }
                volume = Volume.valueOf(descriptor, task.second(), state.getRegion(), leaves.build());
            } else {
                volume = EmptyVolume.valueOf(descriptor, task.second());
            }
            return Optional.of(volume);
        } catch (CancellationException e) {
            return Optional.absent();
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void run() {
        delegate.run();
    }
}