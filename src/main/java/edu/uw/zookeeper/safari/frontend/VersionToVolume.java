package edu.uw.zookeeper.safari.frontend;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.CachedFunction;

import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.SharedLookup;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.volumes.VolumeBranchListener;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

public class VersionToVolume implements AsyncFunction<RegionAndLeaves, AssignedVolumeBranches> {

    public static CachedFunction<VersionedId, AssignedVolumeBranches> newCachedFunction(
            final CachedFunction<Identifier, ZNodePath> idToPath,
            final Function<? super Identifier, VolumeVersion<?>> idToVolume,
            final Materializer<ControlZNode<?>,?> materializer) {
        final Function<VersionedId, AssignedVolumeBranches> fromLatest = new Function<VersionedId, AssignedVolumeBranches>() {
            @Override
            public AssignedVolumeBranches apply(VersionedId input) {
                VolumeVersion<?> cached = idToVolume.apply(input.getValue());
                if ((cached != null) && cached.getState().getVersion().equals(input)) {
                    return (AssignedVolumeBranches) cached;
                } else {
                    return null;
                }
            }
        };
        return CachedFunction.create(
                fromLatest, 
                SharedLookup.create(
                        newVolumeLookup(idToPath, materializer)), 
                LogManager.getLogger(VersionToVolume.class));
    }

    public static AsyncFunction<VersionedId, AssignedVolumeBranches> newVolumeLookup(
            final CachedFunction<Identifier, ZNodePath> paths,
            final Materializer<ControlZNode<?>,?> materializer) {
        final Function<VersionedId, ZNodePath> versionToPaths = new Function<VersionedId, ZNodePath>() {
            @Override
            public AbsoluteZNodePath apply(VersionedId input) {
                return ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(input.getValue(), input.getVersion());
            }
        };
        final CachedFunction<VersionedId, RegionAndLeaves> toState = MaterializerValueLookup.newCachedFunction(versionToPaths, materializer);
        return new AsyncFunction<VersionedId, AssignedVolumeBranches>() {
            @Override
            public ListenableFuture<AssignedVolumeBranches> apply(
                    final VersionedId version) throws Exception {
                return Futures.transform(
                        toState.apply(version), 
                        new VersionToVolume(version, paths));
            }
        };
    }

    private final CachedFunction<Identifier, ZNodePath> paths;
    private final VersionedId version;
    
    protected VersionToVolume(
            VersionedId version,
            CachedFunction<Identifier, ZNodePath> paths) {
        this.version = version;
        this.paths = paths;
    }

    @Override
    public ListenableFuture<AssignedVolumeBranches> apply(RegionAndLeaves state) throws Exception {
        return VolumeBranchListener.forVolume(version, state, paths, SettableFuturePromise.<VolumeVersion<?>>create());
    }
}
