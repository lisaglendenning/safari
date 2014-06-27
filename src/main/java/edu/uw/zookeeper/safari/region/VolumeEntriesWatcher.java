package edu.uw.zookeeper.safari.region;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;

/**
 * Watches and queries entries for a volume version.
 */
public class VolumeEntriesWatcher extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> {

    public static Predicate<RegionAndLeaves> regionEquals(
            final Identifier region) {
        return new Predicate<RegionAndLeaves>() {
            @Override
            public boolean apply(RegionAndLeaves input) {
                return input.getRegion().equals(region);
            }
        };
    }
    
    public static Function<ControlSchema.Safari.Volumes.Volume.Log.Version.State, Optional<VolumeEntriesWatcher>> fromState(
            final Predicate<? super RegionAndLeaves> doWatch,
            final Service service, 
            final ControlClientService control) {
        return new Function<ControlSchema.Safari.Volumes.Volume.Log.Version.State, Optional<VolumeEntriesWatcher>>() {
            @Override
            public Optional<VolumeEntriesWatcher> apply(ControlSchema.Safari.Volumes.Volume.Log.Version.State input) {
                RegionAndLeaves state = input.data().get();
                if ((state != null) && doWatch.apply(state)) {
                    final VersionedId volume = VersionedId.valueOf(input.version().name(), input.version().log().volume().name());
                    return Optional.of(create(volume, service, control));
                } else {
                    return Optional.absent();
                }
            }
        };
    }

    public static VolumeEntriesWatcher listen(
            final VersionedId volume,
            final Service service, 
            final ControlClientService control) {
        VolumeEntriesWatcher listener = create(volume, service, control);
        listener.listen();
        return listener;
    }
    
    public static VolumeEntriesWatcher create(
            final VersionedId volume,
            final Service service, 
            final ControlClientService control) {
        final AbsoluteZNodePath path = 
                ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(
                        volume.getValue(), volume.getVersion());
        @SuppressWarnings("unchecked")
        final PathToQuery<?,?> query = 
            PathToQuery.forRequests(
                    control.materializer(), 
                    Operations.Requests.sync(), 
                    Operations.Requests.getData());
        final Function<ZNodePath, AbstractWatchListener> watchChildren =
                new Function<ZNodePath, AbstractWatchListener>() {
                    @Override
                    public AbstractWatchListener apply(ZNodePath input) {
                        return Watchers.childrenWatcher(
                                input, 
                                control.materializer(), 
                                service, 
                                control.notifications());
                    }
        };
        return new VolumeEntriesWatcher(
                query, 
                watchChildren,
                path.join(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.LABEL),
                control.materializer().cache(),
                service,
                control.cacheEvents());
    }
    
    protected final Function<ZNodePath, ? extends AbstractWatchListener> watchChildren;
    protected final PathToQuery<?,?> query;

    protected VolumeEntriesWatcher(
            PathToQuery<?,?> query,
            Function<ZNodePath, ? extends AbstractWatchListener> watchChildren,
            AbsoluteZNodePath path,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service, 
            WatchListeners watch) {
        super(path, cache, service, watch);
        this.query = query;
        this.watchChildren = watchChildren;
    }
    
    @Override
    public void handleWatchEvent(WatchEvent event) {
        if (service.isRunning()) {
            ControlZNode<?> node = cache.cache().get(event.getPath());
            if (!node.schema().isEmpty()) {
                watchChildren.apply(node.path());
            }
            if (node.data().stamp() < 0L) {
                query.apply(event.getPath()).call();
            }
        }
    }
    
    @Override
    public void starting() {
        super.starting();
        final ZNodePath parent = ((AbsoluteZNodePath) matcher.getPath()).parent();
        watchChildren.apply(parent);
        Watchers.StopOnDeleteListener.listen(this, parent);
    }
}
