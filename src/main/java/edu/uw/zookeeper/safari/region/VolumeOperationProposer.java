package edu.uw.zookeeper.safari.region;

import java.util.List;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.LinkVolumeLogEntry;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.volume.MergeParameters;
import edu.uw.zookeeper.safari.volume.VolumeOperation;
import edu.uw.zookeeper.safari.volume.VolumeOperator;

public class VolumeOperationProposer<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<VolumeLogEntryPath,Pair<VolumeLogEntryPath, Optional<VolumeLogEntryPath>>> {

    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Pair<VolumeLogEntryPath,Optional<VolumeLogEntryPath>>> forProposal(
            VolumeOperationCoordinatorEntry proposal,
            Materializer<ControlZNode<?>,O> materializer) {
        return Futures.transform(
                proposal, 
                new VolumeOperationProposer<O>(proposal.operation(), materializer),
                SameThreadExecutor.getInstance());
    }
    
    protected final Materializer<ControlZNode<?>,O> materializer;
    protected final VolumeOperation<?> operation;
    
    protected VolumeOperationProposer(
            VolumeOperation<?> operation,
            Materializer<ControlZNode<?>,O> materializer) {
        this.operation = operation;
        this.materializer = materializer;
    }
    
    @Override
    public ListenableFuture<Pair<VolumeLogEntryPath, Optional<VolumeLogEntryPath>>> apply(
            final VolumeLogEntryPath proposal) throws Exception {
        return Futures.transform(
                LoggingFutureListener.listen(
                    LogManager.getLogger(this),
                    VolumeOperationEntryLinks.forOperation(
                            proposal, 
                            operation, 
                            materializer)),
                new Function<Optional<VolumeLogEntryPath>,Pair<VolumeLogEntryPath, Optional<VolumeLogEntryPath>>>() {
                    @Override
                    public Pair<VolumeLogEntryPath, Optional<VolumeLogEntryPath>> apply(
                            Optional<VolumeLogEntryPath> input) {
                        return Pair.create(proposal, input);
                    }
                }, SameThreadExecutor.getInstance());
    }

    public static final class VolumeOperationEntryLinks<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<List<O>,VolumeLogEntryPath> {

        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<VolumeLogEntryPath>> forOperation(
                VolumeLogEntryPath entry, 
                VolumeOperation<?> operation, 
                Materializer<ControlZNode<?>,O> materializer) {
            if (operation.getOperator().getOperator() != VolumeOperator.MERGE) {
                return Futures.immediateFuture(Optional.<VolumeLogEntryPath>absent());
            }
            final VersionedId volume = ((MergeParameters) operation.getOperator().getParameters()).getParent();
            final RelativeZNodePath link = ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.PATH.relative(entry.path());
            return Futures.transform(
                    create(link, volume, materializer),
                    new Function<VolumeLogEntryPath, Optional<VolumeLogEntryPath>>() {
                        @Override
                        public Optional<VolumeLogEntryPath> apply(
                                VolumeLogEntryPath input) {
                            return Optional.of(input);
                        }
                    }, SameThreadExecutor.getInstance());
        }
        
        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<VolumeLogEntryPath> create(
                RelativeZNodePath link,
                VersionedId volume,
                Materializer<ControlZNode<?>,O> materializer) {
            VolumesSchemaRequests<O>.VolumeSchemaRequests.VolumeVersionSchemaRequests schema = 
                    VolumesSchemaRequests.create(materializer)
                    .version(volume);
            return Futures.transform(
                    SubmittedRequests.submit(
                            materializer, 
                            schema.children()),
                    new VolumeOperationEntryLinks<O>(
                            link,
                            schema),
                    SameThreadExecutor.getInstance());
        }
        
        private final RelativeZNodePath link;
        private final VolumesSchemaRequests<O>.VolumeSchemaRequests.VolumeVersionSchemaRequests schema;
        
        protected VolumeOperationEntryLinks(
                RelativeZNodePath link,
                VolumesSchemaRequests<O>.VolumeSchemaRequests.VolumeVersionSchemaRequests schema) {
            this.link = link;
            this.schema = schema;
        }

        @Override
        public ListenableFuture<VolumeLogEntryPath> apply(
                List<O> input) throws Exception {
            for (Operation.ProtocolResponse<?> response: input) {
                Operations.unlessError(response.record());
            }
            final Materializer<ControlZNode<?>,O> materializer = schema.volume().volumes().getMaterializer();
            Optional<VolumeLogEntryPath> entry = Optional.absent();
            List<Records.Request> requests = ImmutableList.of();
            materializer.cache().lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes.Volume.Log.Version node = ControlSchema.Safari.Volumes.Volume.Log.Version.fromTrie(
                        materializer.cache().cache(), schema.volume().getVolume(), schema.getVersion());
                for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry e: node.entries().values()) {
                    if (e.data().get() == null) {
                        if (requests.isEmpty()) {
                            requests = Lists.newLinkedList();
                        }
                        requests.addAll(schema.entry(e.name()).get());
                    } else if ((e.data().get() instanceof LinkVolumeLogEntry) && e.data().get().get().equals(link)) {
                        entry = Optional.of(
                                VolumeLogEntryPath.valueOf(
                                        VersionedId.valueOf(
                                                e.version().name(), 
                                                e.version().log().volume().name()), 
                                        e.name()));
                        break;
                    }
                }
            } finally {
                materializer.cache().lock().readLock().unlock();
            }

            if (entry.isPresent()) {
                return Futures.immediateFuture(entry.get());
            }
            
            if (requests.isEmpty()) {
                requests = ImmutableList.<Records.Request>of(schema.logLink(link));
            }
            return Futures.transform(
                    SubmittedRequests.submit(
                            materializer, 
                            requests),
                    this,
                    SameThreadExecutor.getInstance());
        }
    }
}
