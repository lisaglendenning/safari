package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.volume.MergeParameters;
import edu.uw.zookeeper.safari.volume.VolumeOperation;
import edu.uw.zookeeper.safari.volume.VolumeOperator;

public class VolumeOperationProposer implements Function<List<ListenableFuture<?>>,Optional<? extends ListenableFuture<?>>> {

    public static ListenableFuture<Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>> create(
            VolumeOperationCoordinatorEntry proposal,
            Materializer<ControlZNode<?>,?> materializer,
            Promise<Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>> promise) {
        return ChainedFutures.run(
                ChainedFutures.process(
                        ChainedFutures.chain(
                                new VolumeOperationProposer(proposal, materializer), 
                                Lists.<ListenableFuture<?>>newArrayListWithCapacity(2)), 
                        new Processor<List<ListenableFuture<?>>,Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>>() {
                            @SuppressWarnings("unchecked")
                            @Override
                            public Pair<VolumeLogEntryPath, Set<VolumeLogEntryPath>> apply(
                                    List<ListenableFuture<?>> input)
                                    throws Exception {
                                final VolumeLogEntryPath coordinator = (VolumeLogEntryPath) input.get(0).get();
                                final Set<VolumeLogEntryPath> participants = (Set<VolumeLogEntryPath>) input.get(1).get();
                                return Pair.create(coordinator, participants);
                            }
                        }), 
                promise);
    }
    
    protected final Materializer<ControlZNode<?>,?> materializer;
    protected final VolumeOperationCoordinatorEntry proposal;
    
    protected VolumeOperationProposer(
            VolumeOperationCoordinatorEntry proposal,
            Materializer<ControlZNode<?>,?> materializer) {
        this.proposal = proposal;
        this.materializer = materializer;
    }
    
    @Override
    public Optional<? extends ListenableFuture<?>> apply(List<ListenableFuture<?>> input) {
        switch (input.size()) {
        case 0:
            return Optional.of(proposal);
        case 1:
            VolumeLogEntryPath path;
            try {
                path = proposal.get();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
            ListenableFuture<?> future = VolumeOperationEntryLinks.forOperation(
                    path, 
                    proposal.operation(), 
                    materializer,
                    SettableFuturePromise.<Set<VolumeLogEntryPath>>create());
            LoggingFutureListener.listen(
                    LogManager.getLogger(this), future);
            return Optional.of(future);
        case 2:
            return Optional.absent();
        default:
            throw new AssertionError();
        }
    }

    public static class VolumeOperationEntryLinks implements Function<List<ListenableFuture<?>>,Optional<? extends ListenableFuture<?>>> {

        public static ListenableFuture<Set<VolumeLogEntryPath>> forOperation(
                VolumeLogEntryPath entry, VolumeOperation<?> operation, 
                Materializer<ControlZNode<?>,?> materializer,
                Promise<Set<VolumeLogEntryPath>> promise) {
            final ImmutableSet<VersionedId> volumes;
            if (operation.getOperator().getOperator() == VolumeOperator.MERGE) {
                volumes = ImmutableSet.of(((MergeParameters) operation.getOperator().getParameters()).getParent());
            } else {
                volumes = ImmutableSet.of();
            }
            final RelativeZNodePath link = entry.path().relative(ControlSchema.Safari.Volumes.PATH);
            return create(link, volumes, materializer, promise);
        }
        
        public static ListenableFuture<Set<VolumeLogEntryPath>> create(
                RelativeZNodePath link,
                ImmutableSet<VersionedId> volumes,
                Materializer<ControlZNode<?>,?> materializer,
                Promise<Set<VolumeLogEntryPath>> promise) {
            return ChainedFutures.run(
                    ChainedFutures.process(
                        ChainedFutures.chain(
                                new VolumeOperationEntryLinks(Pair.create(link, volumes), materializer), 
                                Lists.<ListenableFuture<?>>newLinkedList()), 
                        ChainedFutures.<Set<VolumeLogEntryPath>>castLast()), 
                    promise);
        }
        
        protected final Pair<RelativeZNodePath,ImmutableSet<VersionedId>> entry;
        protected final Materializer<ControlZNode<?>,?> materializer;
        
        protected VolumeOperationEntryLinks(
                Pair<RelativeZNodePath,ImmutableSet<VersionedId>> entry,
                Materializer<ControlZNode<?>,?> materializer) {
            this.entry = entry;
            this.materializer = materializer;
        }

        @Override
        public Optional<? extends ListenableFuture<?>> apply(List<ListenableFuture<?>> input) {
            if (input.isEmpty()) {
                if (entry.second().isEmpty()) {
                    return Optional.of(Futures.immediateFuture(ImmutableSet.<VolumeLogEntryPath>of()));
                } else {
                    ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
                    for (VersionedId volume: entry.second()) {
                        requests.addAll(VolumesSchemaRequests.create(materializer).volume(volume.getValue()).version(volume.getVersion()).children());
                    }
                    return Optional.of(SubmittedRequests.submit(materializer, requests.build()));
                }
            } 
            try {
                if (input.get(input.size() - 1).get() instanceof Set) {
                    return Optional.absent();
                }
            } catch (Exception e) {
                return Optional.absent();
            }
            List<Records.Request> requests = Lists.newLinkedList();
            Map<VersionedId, VolumeLogEntryPath> links = Maps.newHashMapWithExpectedSize(entry.second().size());
            materializer.cache().lock().readLock().lock();
            try {
                for (VersionedId volume: entry.second()) {
                    ControlSchema.Safari.Volumes.Volume.Log.Version node = ControlSchema.Safari.Volumes.Volume.Log.Version.fromTrie(materializer.cache().cache(), volume.getValue(), volume.getVersion());
                    for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry e: node.entries().values()) {
                        if (e.data().get() == null) {
                            requests.addAll(VolumesSchemaRequests.create(materializer).volume(volume.getValue()).version(volume.getVersion()).children());
                        } else if (e.data().get().equals(entry.first())) {
                            links.put(volume, VolumeLogEntryPath.valueOf(VersionedId.valueOf(e.version().name(), e.version().log().volume().name()), e.name()));
                        }
                    }
                }
            } finally {
                materializer.cache().lock().readLock().unlock();
            }
            if (requests.isEmpty()) {
                Set<VersionedId> difference = Sets.difference(entry.second(), links.keySet());
                ImmutableList.Builder<Records.MultiOpRequest> multi = ImmutableList.builder();
                for (VersionedId volume: difference) {
                    multi.add(VolumesSchemaRequests.create(materializer).volume(volume.getValue()).version(volume.getVersion()).logLink(entry.first()));
                }
                requests.add(new IMultiRequest(multi.build()));
            }
            if (!requests.isEmpty()) {
                return Optional.<ListenableFuture<?>>of(SubmittedRequests.submit(materializer, requests));
            } else {
                assert (links.size() == entry.second().size());
                return Optional.of(Futures.immediateFuture(ImmutableSet.copyOf(links.values())));
            }
        }
    }
}
