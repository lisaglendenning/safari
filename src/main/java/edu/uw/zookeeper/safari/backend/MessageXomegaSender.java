package edu.uw.zookeeper.safari.backend;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageXomega;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnection;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class MessageXomegaSender extends Watchers.StopServiceOnFailure<StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega,Service> {

    public static Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
            Iterable<ServerPeerConnection<?>> connections,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        final MessageXomegaSender instance = new MessageXomegaSender(
                connections, service);
        return Watchers.CacheNodeCreatedListener.listen(
                cache, 
                service, 
                cacheEvents, 
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(instance, cache.cache())), 
                        WatchMatcher.exact(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega.PATH, 
                                Watcher.Event.EventType.NodeDataChanged), 
                            logger), 
                logger);
    }
    
    private final Iterable<ServerPeerConnection<?>> connections;
    
    protected MessageXomegaSender(
            Iterable<ServerPeerConnection<?>> connections,
            Service service) {
        super(service);
        this.connections = connections;
    }

    @Override
    public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega result) {
        if ((result != null) && (result.data().get() != null)) {
            MessagePacket<MessageXomega> message = MessagePacket.valueOf(MessageXomega.valueOf(result.version().id(), result.data().get()));
            for (ServerPeerConnection<?> connection: connections) {
                // TODO handle error?
                connection.write(message);
            }
        }
    }
}
