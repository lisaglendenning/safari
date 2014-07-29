package edu.uw.zookeeper.safari.data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.VersionedValue;
import edu.uw.zookeeper.safari.volume.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.volume.EmptyVolume;
import edu.uw.zookeeper.safari.volume.RegionAndBranches;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;
import edu.uw.zookeeper.safari.volume.VolumeDescriptor;
import edu.uw.zookeeper.safari.volume.VolumeVersion;

public abstract class VolumeBranchListener<T,V extends VolumeVersion<?>> extends SimpleToStringListenableFuture<T> implements Runnable, Callable<Optional<V>> {
    
    public static ListenableFuture<? extends VolumeVersion<?>> fromCache(
            final VersionedId version, 
            final Optional<RegionAndLeaves> state,
            final CachedFunction<Identifier, ZNodePath> idToPath) throws Exception {
        return state.isPresent() ? volumeFromCache(version, state.get(), idToPath) : emptyFromCache(version, idToPath);
    }

    public static ListenableFuture<EmptyVolume> emptyFromCache(
            final VersionedId version, 
            final CachedFunction<Identifier, ZNodePath> idToPath) throws Exception {
        final Promise<EmptyVolume> promise = SettableFuturePromise.create();
        final EmptyVolumeListener instance = new EmptyVolumeListener(version, idToPath.apply(version.getValue()), promise);
        instance.run();
        return promise;
    }

    public static ListenableFuture<AssignedVolumeBranches> volumeFromCache(
            final VersionedId version, 
            final RegionAndLeaves state,
            final CachedFunction<Identifier, ZNodePath> idToPath) throws Exception {
        final ImmutableMap.Builder<Identifier, ListenableFuture<ZNodePath>> paths = ImmutableMap.builder();
        paths.put(version.getValue(), idToPath.apply(version.getValue()));
        for (Identifier leaf: state.getLeaves()) {
            paths.put(leaf, idToPath.apply(leaf));
        }
        final Promise<AssignedVolumeBranches> promise = SettableFuturePromise.create();
        final VolumeListener instance = new VolumeListener(version, state, paths.build(), promise);
        instance.run();
        return promise;

    }
    
    protected final VersionedId version;
    protected final CallablePromiseTask<?, V> delegate;
    
    protected VolumeBranchListener(
            VersionedId version, 
            ListenableFuture<T> future,
            Promise<V> promise) {
        super(future);
        this.version = version;
        this.delegate = CallablePromiseTask.create(this, promise);
    }

    @Override
    public void run() {
        if (delegate.isDone()) {
            if (delegate.isCancelled()) {
                cancel(false);
            }
        } else {
            if (isDone()) {
                delegate.run();
            } else {
                delegate.addListener(this, SameThreadExecutor.getInstance());
                addListener(this, SameThreadExecutor.getInstance());
            }
        }
    }
    
    protected static final class EmptyVolumeListener extends VolumeBranchListener<ZNodePath, EmptyVolume> {

        protected EmptyVolumeListener(
                VersionedId version, 
                ListenableFuture<ZNodePath> future,
                Promise<EmptyVolume> promise) {
            super(version, future, promise);
        }

        @Override
        public Optional<EmptyVolume> call() throws Exception {
            if (isDone()) {
                final ZNodePath path = get();
                final VolumeDescriptor descriptor = VolumeDescriptor.valueOf(version.getValue(), path);
                return Optional.of(EmptyVolume.valueOf(descriptor, version.getVersion()));
            }
            return Optional.absent();
        }
    }
    
    protected static final class VolumeListener extends VolumeBranchListener<List<ZNodePath>, AssignedVolumeBranches> {

        private final ImmutableMap<Identifier, ListenableFuture<ZNodePath>> paths;
        private final RegionAndLeaves state;
        
        protected VolumeListener(
                VersionedId version, 
                RegionAndLeaves state,
                ImmutableMap<Identifier, ListenableFuture<ZNodePath>> paths,
                Promise<AssignedVolumeBranches> promise) {
            super(version, Futures.allAsList(paths.values()), promise);
            this.paths = paths;
            this.state = state;
        }

        @Override
        public Optional<AssignedVolumeBranches> call() throws Exception {
            if (isDone()) {
                get();
                final ZNodePath path = paths.get(version.getValue()).get();
                final VolumeDescriptor descriptor = VolumeDescriptor.valueOf(version.getValue(), path);
                final ImmutableBiMap.Builder<ZNodeName, Identifier> leaves = ImmutableBiMap.builder();
                for (Map.Entry<Identifier, ListenableFuture<ZNodePath>> e: paths.entrySet()) {
                    ZNodePath branch = e.getValue().get();
                    if (branch.length() > path.length()) {
                        leaves.put(branch.suffix(path), e.getKey());
                    }
                }
                return Optional.of(AssignedVolumeBranches.valueOf(descriptor, 
                        VersionedValue.valueOf(version.getVersion(), 
                                RegionAndBranches.valueOf(state.getRegion(), leaves.build()))));
            }
            return Optional.absent();
        }
    }
}
