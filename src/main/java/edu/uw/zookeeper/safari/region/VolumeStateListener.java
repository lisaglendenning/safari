package edu.uw.zookeeper.safari.region;

import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public class VolumeStateListener<T extends AbstractWatchListener> extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> {
    
    public static <T extends AbstractWatchListener> VolumeStateListener<T> listen(
            Function<? super ControlSchema.Safari.Volumes.Volume.Log.Version.State, ? extends Optional<? extends T>> doWatch,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service, 
            WatchListeners watch) {
        VolumeStateListener<T> listener = new VolumeStateListener<T>(doWatch, cache, service, watch);
        listener.listen();
        return listener;
    }

    protected final ConcurrentMap<ZNodePath, T> watchers;
    protected final Function<? super ControlSchema.Safari.Volumes.Volume.Log.Version.State, ? extends Optional<? extends T>> doWatch;
    
    protected VolumeStateListener(
            Function<? super ControlSchema.Safari.Volumes.Volume.Log.Version.State, ? extends Optional<? extends T>> doWatch,
                    LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service, 
            WatchListeners watch) {
        super(cache, service, watch, WatchMatcher.exact(
                        ControlSchema.Safari.Volumes.Volume.Log.Version.State.PATH,
                        Watcher.Event.EventType.NodeCreated,
                        Watcher.Event.EventType.NodeDataChanged));
        this.doWatch = doWatch;
        this.watchers = new MapMaker().weakValues().makeMap();
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        AbsoluteZNodePath path = (AbsoluteZNodePath) event.getPath();
        if (!watchers.containsKey(path)) {
            Optional<? extends T> watcher = doWatch.apply((ControlSchema.Safari.Volumes.Volume.Log.Version.State) cache.cache().get(path));
            if (watcher.isPresent()) {
                if (watchers.putIfAbsent(path, watcher.get()) == null) {
                    watcher.get().listen();
                }
            }
        }
    }
}
