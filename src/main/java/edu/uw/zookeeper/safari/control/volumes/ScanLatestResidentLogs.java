package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ChainedFutures.ChainedProcessor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public class ScanLatestResidentLogs<O extends Operation.ProtocolResponse<?>> implements ChainedProcessor<ListenableFuture<?>> {
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Boolean> call(
            AsyncFunction<VersionedId, Boolean> isResident,
            Materializer<ControlZNode<?>,O> materializer) {
        final ScanLatestResidentLogs<O> instance = new ScanLatestResidentLogs<O>(
                isResident, 
                VolumesSchemaRequests.create(materializer));
        return ChainedFutures.run(
                ChainedFutures.process(
                    ChainedFutures.chain(
                            instance, 
                            Lists.<ListenableFuture<?>>newLinkedList()),
                    ChainedFutures.<Boolean>castLast()),
                SettableFuturePromise.<Boolean>create());
    }
    
    private final VolumesSchemaRequests<O> schema;
    private final AsyncFunction<VersionedId, Boolean> isResident;
    
    protected ScanLatestResidentLogs(
            AsyncFunction<VersionedId, Boolean> isResident,
            VolumesSchemaRequests<O> schema) {
        this.isResident = isResident;
        this.schema = schema;
    }
    
    @Override
    public Optional<? extends ListenableFuture<?>> apply(
            List<ListenableFuture<?>> input) throws Exception {
        if (input.isEmpty()) {
            return Optional.of(GetVolumes.call(schema));
        }
        ListenableFuture<?> last = input.get(input.size()-1);
        if (last instanceof GetVolumes) {
            for (Operation.ProtocolResponse<?> response: ((GetVolumes<?>) last).get()) {
                Operations.unlessError(response.record());
            }
            return Optional.of(GetLatest.call(schema));
        } else if (last instanceof GetLatest) {
            for (Operation.ProtocolResponse<?> response: ((GetLatest<?>) last).get()) {
                Operations.maybeError(response.record(), KeeperException.Code.NONODE);
            }
            return Optional.of(
                    GetResidentVersions.call(
                            schema.getMaterializer(), 
                            isResident));
        } else if (last instanceof GetResidentVersions) {
            List<VersionedId> versions = ((GetResidentVersions) last).get();
            return Optional.of(GetEntries.call(
                    versions, schema.getMaterializer()));
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
    
    protected static final class GetLatest<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> {

        public static <O extends Operation.ProtocolResponse<?>> GetLatest<O> call(
                VolumesSchemaRequests<O> schema) {
            ImmutableList.Builder<Records.Request> builder = ImmutableList.builder();
            schema.getMaterializer().cache().lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.fromTrie(schema.getMaterializer().cache().cache());
                for (ControlZNode<?> node: volumes.values()) {
                    Identifier id = ((ControlSchema.Safari.Volumes.Volume) node).name();
                    builder.addAll(schema.volume(id).latest().get());
                }
            } finally {
                schema.getMaterializer().cache().lock().readLock().unlock();
            }
            return new GetLatest<O>(
                    SubmittedRequests.submit(
                            schema.getMaterializer(), 
                            builder.build()));
        }
        
        protected GetLatest(ListenableFuture<List<O>> delegate) {
            super(delegate);
        }
    }
    
    protected static final class GetResidentVersions extends SimpleToStringListenableFuture<List<VersionedId>> {

        public static <O extends Operation.ProtocolResponse<?>> GetResidentVersions call(
                Materializer<ControlZNode<?>,O> materializer,
                AsyncFunction<VersionedId, Boolean> isResident) {
            List<VersionedId> versions = Lists.newLinkedList();
            materializer.cache().lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.fromTrie(materializer.cache().cache());
                for (ControlZNode<?> node: volumes.values()) {
                    ControlSchema.Safari.Volumes.Volume volume = (ControlSchema.Safari.Volumes.Volume) node;
                    if ((volume.getLog() != null) && (volume.getLog().latest() != null)) {
                        versions.add(VersionedId.valueOf(volume.getLog().latest().data().get(), volume.name()));
                    }
                }
            } finally {
                materializer.cache().lock().readLock().unlock();
            }
            ListenableFuture<List<VersionedId>> future;
            try {
                ImmutableList.Builder<Pair<VersionedId,ListenableFuture<Boolean>>> futures = ImmutableList.builder();
                for (VersionedId version: versions) {
                    futures.add(Pair.create(version, isResident.apply(version)));
                }
                CallablePromiseTask<Call,List<VersionedId>> task = CallablePromiseTask.create(
                        new Call(futures.build()),
                        SettableFuturePromise.<List<VersionedId>>create());
                task.task().addListener(task, SameThreadExecutor.getInstance());
                future = task;
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return new GetResidentVersions(future);
        }
        
        protected GetResidentVersions(ListenableFuture<List<VersionedId>> delegate) {
            super(delegate);
        }
        
        protected static final class Call extends ToStringListenableFuture<List<Boolean>> implements Callable<Optional<List<VersionedId>>> {

            private final ImmutableList<Pair<VersionedId, ListenableFuture<Boolean>>> futures;
            
            protected Call(ImmutableList<Pair<VersionedId, ListenableFuture<Boolean>>> futures) {
                this.futures = futures;
            }
            
            @Override
            public Optional<List<VersionedId>> call() throws Exception {
                ImmutableList.Builder<VersionedId> builder = ImmutableList.builder();
                for (Pair<VersionedId, ListenableFuture<Boolean>> future: futures) {
                    if (!future.second().isDone()) {
                        return Optional.absent();
                    }
                    if (future.second().get().booleanValue()) {
                        builder.add(future.first());
                    }
                }
                return Optional.<List<VersionedId>>of(builder.build());
            }

            @Override
            protected ListenableFuture<List<Boolean>> delegate() {
                List<ListenableFuture<Boolean>> futures = Lists.newLinkedList();
                for (Pair<VersionedId, ListenableFuture<Boolean>> future: this.futures) {
                    futures.add(future.second());
                }
                return Futures.allAsList(futures);
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
