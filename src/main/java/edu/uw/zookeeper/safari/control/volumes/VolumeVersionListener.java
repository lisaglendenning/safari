package edu.uw.zookeeper.safari.control.volumes;

import java.util.Iterator;

import org.apache.logging.log4j.Logger;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.proto.Records.Request;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public class VolumeVersionListener extends DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Volumes.Volume.Log.Version> {

    public static VolumeVersionListener create(
            SchemaClientService<ControlZNode<?>,?> client,
            Iterable<? extends WatchMatchListener> listeners,
            Service service,
            Logger logger) {
        return new VolumeVersionListener(
                client.materializer().schema().apply(ControlSchema.Safari.Volumes.Volume.Log.Version.class),
                listeners,
                client.materializer().cache(),
                service,
                client.cacheEvents(),
                logger);
    }
    
    protected VolumeVersionListener(
            ValueNode<ZNodeSchema> schema,
            Iterable<? extends WatchMatchListener> listeners,
            LockableZNodeCache<ControlZNode<?>, Request, ?> cache,
            Service service, 
            WatchListeners cacheEvents, 
            Logger logger) {
        super(schema, listeners, cache, service, cacheEvents, logger);
    }

    @Override
    protected Iterator<? extends WatchEvent> replay() {
        return ReplayCachedVolumeVersions.fromTrie(cache.cache());
    }
    
    public static class ReplayCachedVolumeVersions extends AbstractIterator<NodeWatchEvent> {

        public static ReplayCachedVolumeVersions fromTrie(NameTrie<ControlZNode<?>> trie) {
            final ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.fromTrie(
                    trie);
            return new ReplayCachedVolumeVersions(volumes.values().iterator());
        }

        private final Iterator<ControlZNode<?>> volumes;
        private Iterator<ControlSchema.Safari.Volumes.Volume.Log.Version> versions = null;
        
        protected ReplayCachedVolumeVersions(Iterator<ControlZNode<?>> volumes) {
            this.volumes = volumes;
            this.versions = null;
        }

        @Override
        protected NodeWatchEvent computeNext() {
            while (volumes.hasNext() || (versions != null)) {
                if (versions != null) {
                    if (versions.hasNext()) {
                        return NodeWatchEvent.nodeCreated(versions.next().path());
                    } else {
                        versions = null;
                    }
                } else {
                    ControlZNode<?> volume = volumes.next();
                    ControlSchema.Safari.Volumes.Volume.Log log = ((ControlSchema.Safari.Volumes.Volume) volume).getLog();
                    if (log != null) {
                        versions = log.versions().values().iterator();
                    }
                    
                }
            }
            return endOfData();
        }
    }
}
