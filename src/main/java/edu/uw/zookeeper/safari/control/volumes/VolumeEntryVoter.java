package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class VolumeEntryVoter<O extends Operation.ProtocolResponse<?>> implements Callable<Optional<SubmittedRequests<Records.Request,?>>> {

    public static <O extends Operation.ProtocolResponse<?>> VolumeEntryVoter<O> selectFirst(
            VersionedId volume,
            Materializer<ControlZNode<?>,O> materializer) {
        return create(volume, materializer, SelectFirst.<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>create());
    }
    
    public static <O extends Operation.ProtocolResponse<?>> VolumeEntryVoter<O> create(
            VersionedId volume,
            Materializer<ControlZNode<?>,O> materializer,
            Function<List<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>, Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>> select) {
        return new VolumeEntryVoter<O>(volume, materializer, select);
    }
    
    private final Logger logger;
    private final Function<List<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>, Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>> select;
    private final VolumesSchemaRequests<O>.VolumeSchemaRequests.VolumeVersionSchemaRequests version;
    private final Materializer<ControlZNode<?>,O> materializer;
    
    protected VolumeEntryVoter(
            VersionedId volume,
            Materializer<ControlZNode<?>,O> materializer,
            Function<List<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>, Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>> select) {
        this.logger = LogManager.getLogger(this);
        this.select = select;
        this.materializer = materializer;
        this.version = VolumesSchemaRequests.create(materializer).volume(volume.getValue()).version(volume.getVersion());
    }
    
    public VersionedId volume() {
        return VersionedId.valueOf(version.getVersion(), version.volume().getVolume());
    }
    
    public Materializer<ControlZNode<?>,O> materializer() {
        return materializer;
    }

    /**
     * Assumes that any possibly committed entries are cached.
     */
    @Override
    public Optional<SubmittedRequests<Records.Request,?>> call() throws Exception {
        List<Records.Request> requests = ImmutableList.of();
        materializer.cache().lock().readLock().lock();
        try {
            ControlSchema.Safari.Volumes.Volume.Log.Version node = ControlSchema.Safari.Volumes.Volume.Log.Version.fromTrie(
                    materializer.cache().cache(), version.volume().getVolume(), version.getVersion());
            if (node != null) {
                List<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> candidates = ImmutableList.of();
                Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> selected = Optional.absent();
                for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry entry: node.entries().values()) {
                    final ControlZNode<?> action = (entry.commit() != null) ? entry.commit() : entry.vote();
                    if (action != null) {
                        Boolean value = (Boolean) action.data().get();
                        if (value != null) {
                            if (value.equals(Boolean.TRUE)) {
                                if (selected.isPresent()) {
                                    if (selected.get().vote() == null) {
                                        if (candidates.isEmpty()) {
                                            candidates = Lists.newLinkedList();
                                        }
                                        candidates.add(selected.get());
                                    }
                                }
                                selected = Optional.of(entry);
                            }
                        } else {
                            if (requests.isEmpty()) {
                                requests = Lists.newLinkedList();
                            }
                            requests.addAll((action == entry.commit()) ? 
                                    version.entry(entry.name()).commit().get() :
                                        version.entry(entry.name()).vote().get());
                        }
                    } else {
                        if (candidates.isEmpty()) {
                            candidates = Lists.newLinkedList();
                        }
                        candidates.add(entry);
                    }
                }
                if (requests.isEmpty()) {
                    if (!selected.isPresent() && !candidates.isEmpty()) {
                        selected = select.apply(candidates);
                    }
                    final List<Records.MultiOpRequest> votes = Lists.newLinkedList();
                    if (selected.isPresent()) {
                        logger.info("SELECTED {} for volume {}", selected.get().name(), volume());
                        if (selected.get().vote() == null) {
                            logger.info("VOTING YES on {} for volume {}", selected.get().name(), volume());
                            votes.add(version.entry(selected.get().name()).vote().create(Boolean.TRUE));
                        }
                    }
                    for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry candidate: candidates) {
                        if (selected.orNull() != candidate) {
                            logger.info("VOTING NO on {} for volume {}", candidate.name(), volume());
                            votes.add(version.entry(candidate.name()).vote().create(Boolean.FALSE));
                        }
                    }
                    if (!votes.isEmpty()) {
                        requests = ImmutableList.<Records.Request>of(new IMultiRequest(votes));
                    }
                }
            }
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
        
        if (!requests.isEmpty()) {
            return Optional.<SubmittedRequests<Records.Request,?>>of(
                    SubmittedRequests.submit(materializer, requests));
        } else {
            return Optional.absent();
        }
    }
    
    public static final class SelectFirst<T> implements Function<List<T>, Optional<T>> {

        public static <T> SelectFirst<T> create() {
            return new SelectFirst<T>();
        }
        
        public SelectFirst() {}
        
        @Override
        public Optional<T> apply(List<T> input) {
            return Optional.of(input.get(0));
        }
    }
}
