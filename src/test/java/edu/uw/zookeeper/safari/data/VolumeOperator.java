package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;

@Singleton
public class VolumeOperator {
    
    protected final Logger logger;
    protected final VolumeCacheService volumes;
    protected final ControlClientService control;
    
    @Inject
    public VolumeOperator(
            VolumeCacheService volumes,
            ControlClientService control) {
        this.logger = LogManager.getLogger(getClass());
        this.volumes = volumes;
        this.control = control;
    }
    
    public ListenableFuture<Identifier> create(
            final ZNodePath path) {
        return ControlZNode.CreateEntity.call(
                        ControlSchema.Safari.Volumes.PATH, 
                        path, 
                        control.materializer());
    }
    
    public ListenableFuture<Message.ServerResponse<?>> difference(
            final VolumeDescriptor descriptor,
            final UnsignedLong version,
            final Identifier region) throws Exception {
        ListenableFuture<Volume> parent = descriptor.getPath().isRoot() ?
                Futures.<Volume>immediateFuture(null) : volumes.pathToVolume().apply(((AbsoluteZNodePath) descriptor.getPath()).parent());
        return Futures.transform(
                parent, 
                new AsyncFunction<Volume,Message.ServerResponse<?>>() {
                    @Override
                    public ListenableFuture<Message.ServerResponse<?>> apply(Volume parent)
                            throws Exception {
                        Operations.Requests.Multi multi = Operations.Requests.multi()
                                .add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.pathOf(descriptor.getId())).get());
                        ImmutableSet.Builder<Identifier> leaves = ImmutableSet.<Identifier>builder();
                        if (parent != null) {
                            ZNodeName prefix = descriptor.getPath().suffix(parent.getDescriptor().getPath().isRoot() ? 0 : parent.getDescriptor().getPath().length());
                            ImmutableSet.Builder<Identifier> siblings = ImmutableSet.<Identifier>builder()
                                    .add(descriptor.getId());
                            for (Entry<ZNodeName, Identifier> branch: parent.getBranches().entrySet()) {
                                if (branch.getKey().startsWith(prefix)) {
                                    leaves.add(branch.getValue());
                                } else {
                                    siblings.add(branch.getValue());
                                }
                            }
                            VolumeState state = VolumeState.valueOf(parent.getRegion(), siblings.build());
                            multi = multi.add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(parent.getDescriptor().getId(), version)).get())
                                    .add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(parent.getDescriptor().getId(), version), state).get());
                        }
                        VolumeState state = VolumeState.valueOf(region, leaves.build());
                        multi = multi.add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(descriptor.getId(), version)).get())
                                .add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(descriptor.getId(), version), state).get());
                        return control.materializer().submit(multi.build());
                    }
            },
            SameThreadExecutor.getInstance());
    }
    
    public ListenableFuture<Message.ServerResponse<?>> union(
            final Volume volume,
            final UnsignedLong version) throws Exception {
        checkArgument(!volume.getDescriptor().getPath().isRoot());
        ListenableFuture<Volume> parent =  volumes.pathToVolume().apply(((AbsoluteZNodePath) volume.getDescriptor().getPath()).parent());
        return Futures.transform(
                parent,
                new AsyncFunction<Volume,Message.ServerResponse<?>>() {
                    @Override
                    public ListenableFuture<Message.ServerResponse<?>> apply(Volume parent)
                            throws Exception {
                        ImmutableSet<Identifier> leaves = Sets.union(
                                Sets.difference(parent.getBranches().values(), 
                                        ImmutableSet.of(volume.getDescriptor().getId())),
                                volume.getBranches().values()).immutableCopy();
                        VolumeState state = VolumeState.valueOf(parent.getRegion(), leaves);
                        Operations.Requests.Multi multi = Operations.Requests.multi();
                        multi = multi.add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(volume.getDescriptor().getId(), version)).get())
                                .add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(volume.getDescriptor().getId(), version)).get())
                                .add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(parent.getDescriptor().getId(), version)).get())
                                .add(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(parent.getDescriptor().getId(), version), state).get());
                        return control.materializer().submit(multi.build());
                    }
                },
                SameThreadExecutor.getInstance());
    }
}