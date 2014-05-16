package edu.uw.zookeeper.safari.frontend;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Supplier;
import com.google.common.collect.MapMaker;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
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

    @Provides @Singleton
    public FrontendSessionExecutor.Factory getFrontendSessionExecutorFactory(
            Provider<ResponseProcessor> processor,
            VolumeCacheService volumes,
            ClientPeerConnectionDispatchers dispatchers,
            RegionsConnectionsService regions,
            ScheduledExecutorService scheduler,
            Executor executor) {
        FrontendSessionExecutor.Factory factory = FrontendSessionExecutor.factory(
                false,
                processor,
                volumes.idToVolume(),
                volumes.pathToVolume(),
                getRegions(regions),
                dispatchers.get(),
                scheduler, 
                executor);
        return factory;
    }
    
    @Provides @Frontend @Singleton
    public SimpleSessionManager<FrontendSessionExecutor> getSessionManager(
            Configuration configuration,
            @Frontend ServerInetAddressView address,
            @Frontend ConcurrentMap<Long, FrontendSessionExecutor> executors,
            FrontendSessionExecutor.Factory factory) {
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
            ClientPeerConnectionDispatchers dispatchers,
            RegionsConnectionsService regions,
            ScheduledExecutorService scheduler,
            Executor executor,
            @Frontend SimpleSessionManager<FrontendSessionExecutor> sessions) {
        FrontendSessionExecutor.Factory factory = FrontendSessionExecutor.factory(
                true,
                processor,
                volumes.idToVolume(),
                volumes.pathToVolume(),
                getRegions(regions),
                dispatchers.get(),
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
