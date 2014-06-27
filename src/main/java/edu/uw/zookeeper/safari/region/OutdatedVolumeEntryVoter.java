package edu.uw.zookeeper.safari.region;

import java.util.Map;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;

public class OutdatedVolumeEntryVoter extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> {
    
    public static OutdatedVolumeEntryVoter listen(
            Predicate<Identifier> isAssigned,
            ControlClientService control,
            Service service) {
        OutdatedVolumeEntryVoter voter = new OutdatedVolumeEntryVoter(
                isAssigned, 
                VolumesSchemaRequests.create(control.materializer()),
                service,
                control.cacheEvents());
        voter.listen();
        return voter;
    }

    protected final VolumesSchemaRequests<?> schema;
    protected final Predicate<Identifier> isAssigned;
    
    protected OutdatedVolumeEntryVoter(
            Predicate<Identifier> isAssigned,
            VolumesSchemaRequests<?> schema,
            Service service,
            WatchListeners watch) {
        super(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.PATH,
                schema.getMaterializer().cache(), service, watch);
        this.schema = schema;
        this.isAssigned = isAssigned;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        ControlSchema.Safari.Volumes.Volume.Log.Version.Entry entry = (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry) cache.cache().get(event.getPath());
        Optional<UnsignedLong> latest = (entry.version().log().latest() == null) ? Optional.<UnsignedLong>absent() : Optional.fromNullable(entry.version().log().latest().data().get());
        if (latest.isPresent() && (entry.version().name().compareTo(latest.get()) < 0)) {
            assert ((entry.version().state() != null) && (entry.version().state().data().stamp() > 0L));
            if ((entry.vote() == null) && (entry.version().state().data().get() != null) && isAssigned.apply(entry.version().state().data().get().getRegion())) {
                schema.getMaterializer().submit(schema.version(entry.version().id()).entry(entry.name()).vote().create(Boolean.FALSE));
            }
        }
    }
    
    @Override
    public void starting() {
        super.starting();
        new LatestListener(cache, service, watch).listen();
    }
    
    protected class LatestListener extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> {

        public LatestListener(
                LockableZNodeCache<ControlZNode<?>, ?, ?> cache,
                Service service, 
                WatchListeners watch) {
            super(cache, service, watch, 
                    WatchMatcher.exact(
                            ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH, 
                            Watcher.Event.EventType.NodeCreated, 
                            Watcher.Event.EventType.NodeDataChanged));
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            ControlSchema.Safari.Volumes.Volume.Log.Latest latest = (ControlSchema.Safari.Volumes.Volume.Log.Latest) cache.cache().get(event.getPath());
            Map.Entry<ZNodeName,ControlSchema.Safari.Volumes.Volume.Log.Version> previous = latest.log().versions().lowerEntry(latest.log().version(latest.data().get()).parent().name());
            if (previous != null) {
                // replay previous version's entries just in case
                for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry entry: previous.getValue().entries().values()) {
                    OutdatedVolumeEntryVoter.this.handleWatchEvent(NodeWatchEvent.nodeCreated(entry.path()));
                }
            }
        }
    }
}
