package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;

public class VolumeEntryVoter implements Callable<Optional<SubmittedRequests<Records.Request,?>>> {

    public static VolumeEntryVoter selectFirst(
            VersionedId volume,
            Materializer<ControlZNode<?>,?> materializer) {
        return create(volume, materializer, SelectFirst.<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>create());
    }
    
    public static VolumeEntryVoter create(
            VersionedId volume,
            Materializer<ControlZNode<?>,?> materializer,
            Function<List<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>, Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>> select) {
        return new VolumeEntryVoter(volume, materializer, select);
    }
    
    protected final Logger logger;
    protected final Function<List<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>, Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>> select;
    protected final VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests version;
    protected final Materializer<ControlZNode<?>,?> materializer;
    
    protected VolumeEntryVoter(
            VersionedId volume,
            Materializer<ControlZNode<?>,?> materializer,
            Function<List<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>, Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>> select) {
        this.logger = LogManager.getLogger(this);
        this.select = select;
        this.materializer = materializer;
        this.version = (VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests) VolumesSchemaRequests.create(materializer).volume(volume.getValue()).version(volume.getVersion());
    }
    
    public VersionedId volume() {
        return VersionedId.valueOf(version.getVersion(), version.volume().getVolume());
    }
    
    public Materializer<ControlZNode<?>,?> materializer() {
        return materializer;
    }

    @Override
    public Optional<SubmittedRequests<Records.Request,?>> call() throws Exception {
        final List<Records.Request> requests = Lists.newLinkedList();
        materializer.cache().lock().readLock().lock();
        try {
            ControlSchema.Safari.Volumes.Volume.Log.Version node = ControlSchema.Safari.Volumes.Volume.Log.Version.fromTrie(materializer.cache().cache(), version.volume().getVolume(), version.getVersion());
            if (node != null) {
                final List<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> candidates = Lists.newLinkedList();
                Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> selected = Optional.absent();
                for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry entry: node.entries().values()) {
                    final ControlZNode<?> action = (entry.commit() != null) ? entry.commit() : entry.vote();
                    if (action != null) {
                        Boolean value = (Boolean) action.data().get();
                        if (value != null) {
                            if (value.equals(Boolean.TRUE)) {
                                if (selected.isPresent()) {
                                    if (selected.get().vote() == null) {
                                        candidates.add(selected.get());
                                    }
                                }
                                selected = Optional.of(entry);
                            }
                        } else {
                            requests.addAll((action == entry.commit()) ? 
                                    version.entry(entry.name()).commit().get() :
                                        version.entry(entry.name()).vote().get());
                        }
                    } else {
                        candidates.add(entry);
                    }
                }
                if (requests.isEmpty()) {
                    if (!selected.isPresent() && !candidates.isEmpty()) {
                        selected = select.apply(candidates);
                        if (selected.isPresent()) {
                            candidates.remove(selected.get());
                        }
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
                        assert (selected.orNull() != candidate);
                        logger.info("VOTING NO on {} for volume {}", candidate.name(), volume());
                        votes.add(version.entry(candidate.name()).vote().create(Boolean.FALSE));
                    }
                    if (!votes.isEmpty()) {
                        requests.add(new IMultiRequest(votes));
                    }
                }
            }
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
        if (! requests.isEmpty()) {
            return Optional.<SubmittedRequests<Records.Request,?>>of(SubmittedRequests.submit(materializer, requests));
        } else {
            return Optional.absent();
        }
    }
    
    public static class SelectFirst<T> implements Function<List<T>, Optional<T>> {

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
