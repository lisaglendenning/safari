package edu.uw.zookeeper.safari.data;

import org.apache.zookeeper.Watcher;

import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class VolumeLatestListener<O extends Operation.ProtocolResponse<?>> extends AbstractWatchListener {

    public static <O extends Operation.ProtocolResponse<?>> VolumeLatestListener<O> listen(
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        VolumeLatestListener<O> listener = new VolumeLatestListener<O>(client, cache, service, watch);
        listener.listen();
        return listener;
    }
    
    private final LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache;
    private final VolumeVersionCreatedListener<O> delegate;
    
    public VolumeLatestListener(
            ClientExecutor<? super Records.Request,O,?> client,
            LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
            Service service,
            WatchListeners watch) {
        super(service, watch, WatchMatcher.exact(
                ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH, 
                Watcher.Event.EventType.NodeCreated, 
                Watcher.Event.EventType.NodeDataChanged));
        this.cache = cache;
        this.delegate = new VolumeVersionCreatedListener<O>(client, cache, service, watch);
    }
    
    @Override
    public void handleWatchEvent(WatchEvent event) {
        if (service.isRunning()) {
            ZNodePath path = null;
            cache.lock().readLock().lock();
            try {
                final ControlSchema.Safari.Volumes.Volume.Log.Latest latest = 
                        (ControlSchema.Safari.Volumes.Volume.Log.Latest) 
                        cache.cache().get(event.getPath());
                if (latest != null) {
                    UnsignedLong version = latest.data().get();
                    if (version != null) {
                        path = ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(latest.log().volume().name(), version);
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
            if (path != null) {
                delegate.handleWatchEvent(NodeWatchEvent.nodeCreated(path));
            }
        }
    }

    @Override
    public void running() {
        cache.lock().readLock().lock();
        try {
            final ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.fromTrie(
                    cache.cache());
            if (volumes != null) {
                for (ControlZNode<?> v : volumes.values()) {
                    ControlSchema.Safari.Volumes.Volume.Log log = ((ControlSchema.Safari.Volumes.Volume) v).getLog();
                    if (log != null) {
                        ControlSchema.Safari.Volumes.Volume.Log.Latest latest = log.latest();
                        if (latest != null) {
                            handleWatchEvent(NodeWatchEvent.nodeCreated(latest.path()));
                        }
                    }
                }
            }
        } finally {
            cache.lock().readLock().unlock();
        }
    }
}