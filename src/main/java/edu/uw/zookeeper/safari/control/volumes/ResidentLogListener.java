package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeState;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;

/**
 * Watches log of resident versions. Queries on any log entry notification.
 */
public final class ResidentLogListener implements FutureCallback<ControlSchema.Safari.Volumes.Volume.Log.Version.State> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public ResidentLogListener getResidentLogListener(
                final @Region Predicate<AssignedVolumeState> isAssigned,
                final SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> client,
                final RegionRoleService service) {
            return ResidentLogListener.listen(
                    isAssigned, 
                    client, 
                    service, 
                    service.logger());
        }

        @Override
        public Key<?> getKey() {
            return Key.get(ResidentLogListener.class);
        }
        
        @Override
        protected void configure() {
        }
    }
    
    public static <O extends Operation.ProtocolResponse<?>> ResidentLogListener listen(
            Predicate<? super RegionAndLeaves> isResident,
            SchemaClientService<ControlZNode<?>,O> client,
            Service service,
            Logger logger) {
        return listen(
                isResident, 
                client, 
                service, 
                logger, 
                Watchers.MaybeErrorProcessor.maybeNoNode(), 
                Watchers.StopServiceOnFailure.create(service,logger));
    }
    
    @SuppressWarnings("unchecked")
    public static <O extends Operation.ProtocolResponse<?>, V> ResidentLogListener listen(
            Predicate<? super RegionAndLeaves> isResident,
            SchemaClientService<ControlZNode<?>,O> client,
            Service service,
            Logger logger,
            Processor<? super List<O>, V> processor,
            FutureCallback<? super V> callback) {
        for (ValueNode<ZNodeSchema> child: client.materializer().schema().apply(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.class).values()) {
            Watchers.FutureCallbackServiceListener.listen(
                    Watchers.EventToPathCallback.create(
                            Watchers.PathToQueryCallback.create(
                                    PathToQuery.forRequests(
                                            client.materializer(), 
                                            Operations.Requests.sync(), 
                                            Operations.Requests.getData()), 
                                    processor, 
                                    callback)), 
                    service, 
                    client.notifications(),
                    WatchMatcher.exact(
                            child.path(), 
                            Watcher.Event.EventType.NodeCreated),
                    logger);
        }
        Watchers.CacheNodeCreatedListener.listen(
                client.materializer().cache(), 
                service, 
                client.cacheEvents(), 
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToQueryCallback.create(
                                        PathToQuery.forFunction(
                                                client.materializer(), 
                                                LogEntryQuery.create()), 
                                        processor, 
                                        callback)), 
                        WatchMatcher.exact(
                            ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.PATH, 
                            Watcher.Event.EventType.NodeCreated), 
                        logger), 
                logger);
        Watchers.FutureCallbackServiceListener.listen(
                Watchers.EventToPathCallback.create(
                        Watchers.PathToQueryCallback.create(
                                PathToQuery.forRequests(
                                        client.materializer(), 
                                        Operations.Requests.sync(), 
                                        Operations.Requests.getChildren().setWatch(true)), 
                                processor, 
                                callback)), 
                service, 
                client.notifications(),
                WatchMatcher.exact(
                        ControlSchema.Safari.Volumes.Volume.Log.Version.PATH, 
                        Watcher.Event.EventType.NodeChildrenChanged),
                logger);
        final ResidentLogListener instance = create(
                isResident, 
                client.materializer(), 
                processor, 
                callback);
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
                            ControlSchema.Safari.Volumes.Volume.Log.Version.State.PATH, 
                            Watcher.Event.EventType.NodeDataChanged), 
                        logger), 
                logger);
        return instance;
    }
    
    protected static <O extends Operation.ProtocolResponse<?>, V> ResidentLogListener create(
            Predicate<? super RegionAndLeaves> isResident,
            Materializer<ControlZNode<?>,O> materializer,
            Processor<? super List<O>, V> processor,
            FutureCallback<? super V> callback) {
        @SuppressWarnings("unchecked")
        final Watchers.PathToQueryCallback<O,V> query = Watchers.PathToQueryCallback.create(
                PathToQuery.forRequests(
                        materializer, 
                        Operations.Requests.sync(), 
                        Operations.Requests.getChildren().setWatch(true)), 
                processor, 
                callback);
        return new ResidentLogListener(isResident, query);
    }
    
    private final Predicate<? super RegionAndLeaves> isResident;
    private final FutureCallback<ZNodePath> callback;
    
    protected ResidentLogListener(
            Predicate<? super RegionAndLeaves> isResident,
            FutureCallback<ZNodePath> callback) {
        this.callback = callback;
        this.isResident = isResident;
    }

    @Override
    public void onSuccess(ControlSchema.Safari.Volumes.Volume.Log.Version.State result) {
        if (result != null) {
            Optional<RegionAndLeaves> state = Optional.fromNullable((RegionAndLeaves) result.data().get());
            if (state.isPresent() && isResident.apply(state.get())) {
                callback.onSuccess(result.parent().get().path());
            }
        }
    }

    @Override
    public void onFailure(Throwable t) {
        callback.onFailure(t);
    }
    
    protected static final class LogEntryQuery implements Function<ZNodePath, List<? extends Records.Request>> {
        
        public static LogEntryQuery create() {
            return new LogEntryQuery();
        }
        
        private static final ZNodeLabel[] LABELS = { 
            ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote.LABEL, 
            ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit.LABEL };
        
        private final Operations.Requests.Sync sync;
        private final Operations.Requests.GetData getData;
        
        protected LogEntryQuery() {
            this.sync = Operations.Requests.sync(); 
            this.getData = Operations.Requests.getData();
        }
        
        @Override
        public List<? extends Records.Request> apply(ZNodePath input) {
            ImmutableList.Builder<Records.Request> builder = ImmutableList.builder();
            builder.add(sync.setPath(input).build());
            builder.add(getData.setPath(input).setWatch(false).build());
            for (ZNodeLabel label: LABELS) {
                ZNodePath path = input.join(label);
                builder.add(sync.setPath(path).build());
                builder.add(getData.setPath(path).setWatch(true).build());
            }
            return builder.build();
        }
    }
}
