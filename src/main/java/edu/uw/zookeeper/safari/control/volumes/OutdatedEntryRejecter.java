package edu.uw.zookeeper.safari.control.volumes;

import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

/**
 * Assumes all versions and all resident logs are watched
 */
public class OutdatedEntryRejecter<O extends Operation.ProtocolResponse<?>> extends Watchers.StopServiceOnFailure<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> {
    
    public static <O extends Operation.ProtocolResponse<?>> OutdatedEntryRejecter<O> listen(
            AsyncFunction<VersionedId, Boolean> isResident,
            SchemaClientService<ControlZNode<?>,O> client,
            Service service,
            Logger logger) {
        final OutdatedEntryRejecter<O> instance = new OutdatedEntryRejecter<O>(
                isResident, 
                VolumesSchemaRequests.create(client.materializer()),
                service);
        Watchers.CacheNodeCreatedListener.listen(
                client.materializer().cache(), 
                service, 
                client.cacheEvents(), 
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        instance, 
                                        client.materializer().cache().cache())), 
                        WatchMatcher.exact(
                            ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.PATH, 
                            Watcher.Event.EventType.NodeCreated)), 
                logger);
        LatestListener.listen(
                instance,
                client.materializer().cache(),
                client.cacheEvents(),
                service,
                logger);
        return instance;
    }

    protected final VolumesSchemaRequests<O> schema;
    protected final AsyncFunction<VersionedId, Boolean> isResident;
    
    protected OutdatedEntryRejecter(
            AsyncFunction<VersionedId, Boolean> isResident,
            VolumesSchemaRequests<O> schema,
            Service service) {
        super(service);
        this.schema = schema;
        this.isResident = isResident;
    }

    @Override
    public void onSuccess(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry result) {
        if (result.vote() == null) {
            Optional<UnsignedLong> latest = (result.version().log().latest() == null) ? Optional.<UnsignedLong>absent() : Optional.fromNullable(result.version().log().latest().data().get());
            if (latest.isPresent()) {
                if (result.version().name().compareTo(latest.get()) < 0) {
                    try {
                        ListenableFuture<Boolean> resident = isResident.apply(result.version().id());
                        if (resident.isDone()) {
                            if (resident.get().booleanValue()) {
                                new Reject(SubmittedRequest.submit(
                                            schema.getMaterializer(),
                                            schema.version(result.version().id()).entry(result.name()).vote().create(Boolean.FALSE))).run();
                            }
                        } else {
                            new Replay(result.path(), resident).run();
                        }
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }
            }
        }
    }
    
    protected abstract class Callback<V> extends SimpleToStringListenableFuture<V> implements Runnable, Processor<V, Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry>> {

        protected Callback(
                ListenableFuture<V> future) {
            super(future);
        }

        @Override
        public void run() {
            if (isDone()){
                Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> result;
                try {
                    result = apply(get());
                } catch (Exception e) {
                    onFailure(e);
                    return;
                }
                if (result.isPresent()) {
                    onSuccess(result.get());
                }
            } else {
                addListener(this, SameThreadExecutor.getInstance());
            }
        }
    }
    
    protected final class Reject extends Callback<O> {
        
        protected Reject(ListenableFuture<O> future) {
            super(future);
        }

        @Override
        public Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> apply(O input) throws Exception {
            Operations.maybeError(input.record(), KeeperException.Code.NONODE, KeeperException.Code.NODEEXISTS);
            return Optional.absent();
        }
    }
    
    protected final class Replay extends Callback<Boolean> {
        
        protected final ZNodePath path;
        
        protected Replay(
                ZNodePath path,
                ListenableFuture<Boolean> future) {
            super(future);
            this.path = path;
        }

        @Override
        public void run() {
            schema.getMaterializer().cache().lock().readLock().lock();
            try {
                super.run();
            } finally {
                schema.getMaterializer().cache().lock().readLock().unlock();
            }   
        }
        
        @Override
        public Optional<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> apply(Boolean input) throws Exception {
            if (input.booleanValue()) {
                ControlSchema.Safari.Volumes.Volume.Log.Version.Entry node = (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry) schema.getMaterializer().cache().cache().get(path);
                if (node != null) {
                    return Optional.of(node);
                }
            }
            return Optional.absent();
        }
    }
    
    protected static final class LatestListener implements FutureCallback<ControlSchema.Safari.Volumes.Volume.Log.Latest> {

        public static LatestListener listen(
                FutureCallback<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> callback,
                LockableZNodeCache<ControlZNode<?>, ?, ?> cache,
                WatchListeners watch,
                Service service, 
                Logger logger) {
            final LatestListener instance = new LatestListener(callback);
            Watchers.CacheNodeCreatedListener.listen(cache, service, watch, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                            instance, cache.cache())), 
                            WatchMatcher.exact(
                                ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH,
                                Watcher.Event.EventType.NodeDataChanged), 
                            logger), 
                    logger);
            return instance;
        }
        
        private final FutureCallback<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> callback;
        
        protected LatestListener(
                FutureCallback<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> callback) {
            this.callback = callback;
        }

        @Override
        public void onSuccess(ControlSchema.Safari.Volumes.Volume.Log.Latest result) {
            if ((result != null) && (result.data().stamp() > 0L)) {
                Map.Entry<ZNodeName,ControlSchema.Safari.Volumes.Volume.Log.Version> previous = result.log().versions().lowerEntry(result.log().version(result.data().get()).parent().name());
                if (previous != null) {
                    // replay previous version's entries
                    for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry entry: previous.getValue().entries().values()) {
                        callback.onSuccess(entry);
                    }
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
}
