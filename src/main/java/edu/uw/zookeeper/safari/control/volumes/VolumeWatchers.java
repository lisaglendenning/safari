package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.DirectoryWatcherService;
import edu.uw.zookeeper.safari.schema.ImmutableZNodeGetter;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public class VolumeWatchers extends AbstractModule implements SafariModule {

    public static VolumeWatchers create() {
        return new VolumeWatchers();
    }
        
    protected VolumeWatchers() {}

    @Override  
    public Key<? extends Service> getKey() {
        return Key.get(new TypeLiteral<DirectoryWatcherService<ControlSchema.Safari.Volumes>>(){});
    }
    
    @Provides @Singleton
    public DirectoryWatcherService<ControlSchema.Safari.Volumes> getVolumesWatcherService(
            final Injector injector,
            final SchemaClientService<ControlZNode<?>,?> client,
            final ServiceMonitor monitor) {
        DirectoryWatcherService<ControlSchema.Safari.Volumes> instance = DirectoryWatcherService.listen(
                ControlSchema.Safari.Volumes.class,
                client,
                ImmutableList.of(
                        new Service.Listener() {
                            @Override
                            public void starting() {
                                injector.getInstance(
                                        Key.get(new TypeLiteral<DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Volumes.Volume>>(){}));
                            }
                        }));
        monitor.add(instance);
        return instance;
    }
    
    @Provides @Singleton
    public DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Volumes.Volume.Log.Version> getVolumeVersionListener(
            SchemaClientService<ControlZNode<?>,?> client,
            DirectoryWatcherService<ControlSchema.Safari.Volumes> service) {
        final VolumeVersionListener instance = VolumeVersionListener.create(
                client, 
                ImmutableList.<WatchMatchListener>of(),
                service,
                LogManager.getLogger(ControlSchema.Safari.Volumes.Volume.Log.Version.class));
        // always get version state
        ImmutableZNodeGetter.listen(
                ControlSchema.Safari.Volumes.Volume.Log.Version.State.class,
                client.materializer(),
                instance);
        instance.listen();
        return instance;
    }
    
    @SuppressWarnings("unchecked")
    @Provides @Singleton
    public static DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Volumes.Volume> getVolumeListener(
            SchemaClientService<ControlZNode<?>,?> client,
            DirectoryWatcherService<ControlSchema.Safari.Volumes> service) {
        final DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Volumes.Volume> instance = DirectoryEntryListener.create(
                ControlSchema.Safari.Volumes.Volume.class,
                client,
                service,
                LogManager.getLogger(ControlSchema.Safari.Volumes.Volume.class));
        // always get volume path
        ImmutableZNodeGetter.listen(
                ControlSchema.Safari.Volumes.Volume.Path.class,
                client.materializer(),
                instance);
        // watch all log versions and latest
        final Watchers.MaybeErrorProcessor maybeNoNode = Watchers.MaybeErrorProcessor.maybeNoNode();
        final Watchers.FailWatchListener<Object> failInstance = Watchers.FailWatchListener.create(instance);
        for (Pair<? extends PathToRequests, WatchMatcher> query: ImmutableList.of(
                Pair.create(
                        PathToRequests.forRequests(
                                Operations.Requests.sync(), 
                                Operations.Requests.getData().setWatch(true)), 
                        WatchMatcher.exact(
                                ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH, 
                                Watcher.Event.EventType.NodeCreated,
                                Watcher.Event.EventType.NodeDataChanged)),
                Pair.create(
                        PathToRequests.forRequests(
                                Operations.Requests.sync(), 
                                Operations.Requests.getChildren().setWatch(true)),
                        WatchMatcher.exact(
                                ControlSchema.Safari.Volumes.Volume.Log.PATH, 
                                Watcher.Event.EventType.NodeCreated,
                                Watcher.Event.EventType.NodeChildrenChanged)))) {
            Watchers.FutureCallbackServiceListener.listen(
                    Watchers.EventToPathCallback.create(
                            Watchers.PathToQueryCallback.create(
                                    PathToQuery.forFunction(
                                            client.materializer(),
                                            query.first()), 
                                    maybeNoNode, 
                                    failInstance)),
                    service, 
                    client.notifications(), 
                    query.second(),
                    instance.logger());   
        }
        // query log for new volumes
        for (Function<ZNodePath, ? extends List<? extends Records.Request>> query: ImmutableList.of(
                        Functions.compose(
                            PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.getData().setWatch(true)), 
                            Functions.compose(
                                JoinToPath.forName(ControlSchema.Safari.Volumes.Volume.Log.Latest.LABEL),
                                JoinToPath.forName(ControlSchema.Safari.Volumes.Volume.Log.LABEL))),
                        Functions.compose(
                            PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.getChildren().setWatch(true)),
                            JoinToPath.forName(ControlSchema.Safari.Volumes.Volume.Log.LABEL)))) {
            DirectoryEntryListener.entryCreatedQuery(
                    PathToQuery.forFunction(
                            client.materializer(), 
                            query), 
                    instance); 
        }
        instance.listen();
        return instance;
    }

    @Override
    protected void configure() {
    }
}
