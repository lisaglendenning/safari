package edu.uw.zookeeper.safari.frontend;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
import edu.uw.zookeeper.safari.volume.AssignedVolumeBranches;
import edu.uw.zookeeper.server.ProcessorTaskExecutor;
import edu.uw.zookeeper.server.SessionManager;
import edu.uw.zookeeper.server.SimpleSessionManager;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class FrontendServerExecutor extends AbstractModule {

    public static FrontendServerExecutor create() {
        return new FrontendServerExecutor();
    }
    
    protected FrontendServerExecutor() {}
    
    @Override
    protected void configure() {
        bind(Key.get(ZxidGenerator.class, Module.annotation())).to(Key.get(ZxidEpochIncrementer.class, Module.annotation()));
        bind(Key.get(ZxidReference.class, Module.annotation())).to(Key.get(ZxidGenerator.class, Module.annotation()));
        bind(Key.get(SessionManager.class, Module.annotation())).to(Key.get(new TypeLiteral<SimpleSessionManager<FrontendSessionExecutor>>() {}, Module.annotation()));
        bind(Key.get(new TypeLiteral<TaskExecutor<ConnectMessage.Request, ConnectMessage.Response>>(){}, Module.annotation())).to(FrontendConnectExecutor.class);
    }
    
    @Provides @Singleton @Frontend
    public ZxidEpochIncrementer getZxids() {
        return ZxidEpochIncrementer.fromZero();
    }

    @Provides @Frontend @Singleton
    public ConcurrentMap<Long, FrontendSessionExecutor> getSessionExecutors() {
        return new MapMaker().makeMap();
    }
    
    @Provides @Frontend @Singleton
    public TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor() {
        return ProcessorTaskExecutor.of(
                FourLetterRequestProcessor.newInstance());
    }
    
    @Provides @Singleton
    public ClientPeerConnectionDispatchers getDispatchers(
            RegionsConnectionsService connections) {
        return ClientPeerConnectionDispatchers.create(connections);
    }

    @Provides @Singleton
    public ResponseProcessor newResponseProcessor(
            @Frontend SessionManager sessions,
            @Frontend ZxidGenerator zxids) {
        return ResponseProcessor.create(sessions, zxids);
    }
    
    @Provides @Frontend @Singleton
    public CachedFunction<VersionedId, AssignedVolumeBranches> newVolumeVersionLookup(
            VolumeCacheService volumes,
            @Control Materializer<ControlZNode<?>,?> materializer) {
        return VersionToVolume.newCachedFunction(volumes, materializer);
    }

    @Provides @Frontend @Singleton
    public CachedFunction<VersionedId, Long> newVolumeZxidLookup(
            @Control Materializer<ControlZNode<?>,?> materializer) {
        final Function<VersionedId, ZNodePath> paths = new Function<VersionedId, ZNodePath>() {
            @Override
            public AbsoluteZNodePath apply(VersionedId input) {
                return ControlSchema.Safari.Volumes.Volume.Log.Version.Xomega.pathOf(input.getValue(), input.getVersion());
            }
        };
        return MaterializerValueLookup.newCachedFunction(paths, materializer);
    }
    
    @Provides @Frontend @Singleton
    public SimpleSessionManager<FrontendSessionExecutor> getSessionManager(
            Configuration configuration,
            @Frontend ServerInetAddressView address,
            @Frontend ConcurrentMap<Long, FrontendSessionExecutor> executors,
            Provider<ResponseProcessor> processor,
            VolumeCacheService volumes,
            @Frontend CachedFunction<VersionedId, AssignedVolumeBranches> versionToVolume,
            @Frontend CachedFunction<VersionedId, Long> versionToZxid,
            ClientPeerConnectionDispatchers dispatchers,
            RegionsConnectionsService regions,
            ScheduledExecutorService scheduler,
            Executor executor) {
        FrontendSessionExecutor.Factory factory = newFrontendSessionExecutorFactory(
                false,
                processor,
                volumes,
                versionToVolume,
                versionToZxid,
                dispatchers,
                regions,
                scheduler, 
                executor);
        return SimpleSessionManager.fromConfiguration(
                (short) address.get().hashCode(), 
                executors, factory, configuration);
    }
    
    @Provides @Singleton
    public FrontendConnectExecutor getConnectExecutor(
            @Frontend ZxidReference lastZxid,
            @Frontend ConcurrentMap<Long, FrontendSessionExecutor> executors,
            Provider<ResponseProcessor> processor,
            VolumeCacheService volumes,
            @Frontend CachedFunction<VersionedId, AssignedVolumeBranches> versionToVolume,
            @Frontend CachedFunction<VersionedId, Long> versionToZxid,
            ClientPeerConnectionDispatchers dispatchers,
            RegionsConnectionsService regions,
            ScheduledExecutorService scheduler,
            Executor executor,
            @Frontend SimpleSessionManager<FrontendSessionExecutor> sessions) {
        FrontendSessionExecutor.Factory factory = newFrontendSessionExecutorFactory(
                true,
                processor,
                volumes,
                versionToVolume,
                versionToZxid,
                dispatchers,
                regions,
                scheduler, 
                executor);
        return FrontendConnectExecutor.defaults(
                factory, executors, sessions, lastZxid);
    }

    @Provides @Singleton @Frontend
    public ServerExecutor<FrontendSessionExecutor> getServerExecutor(
            @Frontend TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor,
            @Frontend TaskExecutor<ConnectMessage.Request, ConnectMessage.Response> connectExecutor,
            @Frontend ConcurrentMap<Long, FrontendSessionExecutor> sessionExecutors) {
        return new SimpleServerExecutor<FrontendSessionExecutor>(sessionExecutors, connectExecutor, anonymousExecutor);
    }
    
    protected static FrontendSessionExecutor.Factory newFrontendSessionExecutorFactory(
            boolean renew,
            Provider<ResponseProcessor> processor,
            VolumeCacheService volumes,
            AsyncFunction<VersionedId, AssignedVolumeBranches> versionToVolume,
            AsyncFunction<VersionedId, Long> versionToZxid,
            ClientPeerConnectionDispatchers dispatchers,
            RegionsConnectionsService regions,
            ScheduledExecutorService scheduler,
            Executor executor) {
        return FrontendSessionExecutor.factory(
                renew,
                processor,
                volumes.idToVolume(),
                volumes.pathToVolume(),
                versionToVolume,
                versionToZxid,
                getRegions(regions),
                dispatchers.get(),
                scheduler, 
                executor);
    }
    
    protected static Supplier<Set<Identifier>> getRegions(
            final RegionsConnectionsService regions) {
        return new Supplier<Set<Identifier>>() {
            @Override
            public Set<Identifier> get() {
                return regions.regions().keySet();
            }  
        };
    }
}
