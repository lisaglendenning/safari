package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeState;

public class ScanLatestResidentLogs<O extends Operation.ProtocolResponse<?>> implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain<ListenableFuture<?>>> {
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Boolean> call(
            Predicate<? super AssignedVolumeState> isAssigned,
            Materializer<ControlZNode<?>,O> materializer) {
        final ScanLatestResidentLogs<O> instance = new ScanLatestResidentLogs<O>(
                isAssigned, 
                VolumesSchemaRequests.create(materializer));
        return ChainedFutures.run(
                ChainedFutures.<Boolean>castLast(
                    ChainedFutures.arrayDeque(
                            instance)));
    }
    
    private final VolumesSchemaRequests<O> schema;
    private final Predicate<? super AssignedVolumeState> isAssigned;
    
    protected ScanLatestResidentLogs(
            Predicate<? super AssignedVolumeState> isAssigned,
            VolumesSchemaRequests<O> schema) {
        this.isAssigned = isAssigned;
        this.schema = schema;
    }
    
    @Override
    public Optional<? extends ListenableFuture<?>> apply(
            FutureChain<ListenableFuture<?>> input) throws Exception {
        if (input.isEmpty()) {
            return Optional.of(GetVolumes.call(schema));
        }
        ListenableFuture<?> last = input.getLast();
        if (last instanceof GetVolumes) {
            for (Operation.ProtocolResponse<?> response: ((GetVolumes<?>) last).get()) {
                Operations.unlessError(response.record());
            }
            return Optional.of(GetLatestAssignedVersions.call(schema, isAssigned));
        } else if (last instanceof GetLatestAssignedVersions) {
            ImmutableList.Builder<VersionedId> versions = ImmutableList.builder();
            for (Optional<VersionedId> version: ((GetLatestAssignedVersions<?>) last).get()) {
                if (version.isPresent()) {
                    versions.add(version.get());
                }
            }
            return Optional.of(GetEntries.call(
                    versions.build(), schema.getMaterializer()));
        } else if (last instanceof GetEntries) {
            for (Operation.ProtocolResponse<?> response: ((GetEntries<?>) last).get()) {
                Operations.maybeError(response.record(), KeeperException.Code.NONODE);
            }
            return Optional.of(Futures.immediateFuture(Boolean.TRUE));
        }
        return Optional.absent();
    }
    
    protected static final class GetVolumes<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> {

        public static <O extends Operation.ProtocolResponse<?>> GetVolumes<O> call(
                VolumesSchemaRequests<O> schema) {
            return new GetVolumes<O>(
                    SubmittedRequests.submit(
                            schema.getMaterializer(), 
                            schema.children()));
        }
        
        protected GetVolumes(ListenableFuture<List<O>> delegate) {
            super(delegate);
        }
    }
    
    protected static final class GetLatestAssignedVersions<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<Optional<VersionedId>>> {

        public static <O extends Operation.ProtocolResponse<?>> GetLatestAssignedVersions<O> call(
                VolumesSchemaRequests<O> schema,
                Predicate<? super AssignedVolumeState> isAssigned) {
            ImmutableList.Builder<ListenableFuture<Optional<VersionedId>>> futures = ImmutableList.builder();
            schema.getMaterializer().cache().lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.fromTrie(schema.getMaterializer().cache().cache());
                for (ControlZNode<?> node: volumes.values()) {
                    Identifier id = ((ControlSchema.Safari.Volumes.Volume) node).name();
                    futures.add(GetLatestAssignedVersion.create(schema.volume(id), isAssigned));
                }
            } finally {
                schema.getMaterializer().cache().lock().readLock().unlock();
            }
            return new GetLatestAssignedVersions<O>(Futures.allAsList(futures.build()));
        }
        
        protected GetLatestAssignedVersions(ListenableFuture<List<Optional<VersionedId>>> delegate) {
            super(delegate);
        }
        
        protected final static class GetLatestAssignedVersion<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<List<O>, Optional<VersionedId>>, Callable<ListenableFuture<Optional<VersionedId>>> {
            
            public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<VersionedId>> create(
                    VolumesSchemaRequests<O>.VolumeSchemaRequests schema,
                    Predicate<? super AssignedVolumeState> isAssigned) {
                return new GetLatestAssignedVersion<O>(schema, isAssigned).call();
            }
            
            private final VolumesSchemaRequests<O>.VolumeSchemaRequests schema;
            private final Predicate<? super AssignedVolumeState> isAssigned;
            
            public GetLatestAssignedVersion(
                    VolumesSchemaRequests<O>.VolumeSchemaRequests schema,
                    Predicate<? super AssignedVolumeState> isAssigned) {
                this.schema = schema;
                this.isAssigned = isAssigned;
            }
            
            @Override
            public ListenableFuture<Optional<VersionedId>> call() {
                try {
                    return Futures.transform(
                            SubmittedRequests.submit(
                                schema.volumes().getMaterializer(), 
                                schema.latest().get()), 
                            this);
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            }

            @Override
            public ListenableFuture<Optional<VersionedId>> apply(List<O> input)
                    throws Exception {
                Optional<Operation.Error> error = Optional.absent();
                for (O response: input) {
                    error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                }
                if (!error.isPresent()) {
                    schema.volumes().getMaterializer().cache().lock().readLock().lock();
                    try {
                        ControlSchema.Safari.Volumes.Volume.Log.Latest latest = (ControlSchema.Safari.Volumes.Volume.Log.Latest) schema.volumes().getMaterializer().cache().cache().get(schema.latest().getPath());
                        if ((latest != null) && (latest.data().get() != null)) {
                            ControlSchema.Safari.Volumes.Volume.Log.Version version = latest.log().version(latest.data().get());
                            if ((version != null) && (version.state() != null) && (version.state().data().get() != null)) {
                                return Futures.immediateFuture(
                                        isAssigned.apply(version.state().data().get()) ? 
                                        Optional.of(version.id()) : 
                                            Optional.<VersionedId>absent());
                            } else {
                                return Futures.transform(
                                        SubmittedRequests.submit(
                                                schema.volumes().getMaterializer(), 
                                                schema.version(latest.data().get()).state().get()), 
                                        this);
                            }
                        }
                    } finally {
                        schema.volumes().getMaterializer().cache().lock().readLock().unlock();
                    }
                }
                return Futures.immediateFuture(Optional.<VersionedId>absent());
            }
            
        }
    }
    
    protected static final class GetEntries<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> {

        public static <O extends Operation.ProtocolResponse<?>> GetEntries<O> call(
                List<VersionedId> versions,
                ClientExecutor<? super Records.Request, O, ?> client) {
            @SuppressWarnings("unchecked")
            PathToRequests requests = PathToRequests.forRequests(
                    Operations.Requests.sync(), Operations.Requests.getChildren());
            ImmutableList.Builder<Records.Request> builder = ImmutableList.builder();
            for (VersionedId version: versions) {
                builder.addAll(requests.apply(
                        ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(
                                version.getValue(), version.getVersion())));
            }
            return new GetEntries<O>(
                    SubmittedRequests.submit(
                            client, 
                            builder.build()));
        }
        
        protected GetEntries(ListenableFuture<List<O>> delegate) {
            super(delegate);
        }
    }
}
