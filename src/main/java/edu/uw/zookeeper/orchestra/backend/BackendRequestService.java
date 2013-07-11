package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.VolumeLookupService;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.peer.PeerConfiguration;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionClose;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpen;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.TimeValue;

@DependsOn({ControlMaterializerService.class, BackendConnectionsService.class})
public class BackendRequestService<C extends Connection<? super Operation.Request>> extends DependentService.SimpleDependentService implements ParameterizedFactory<MessageSessionOpen, ShardedClientConnectionExecutor<C>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(BackendConnectionsService.module());
            TypeLiteral<BackendRequestService<?>> generic = new TypeLiteral<BackendRequestService<?>>() {};
            bind(BackendRequestService.class).to(generic);
            bind(generic).to(new TypeLiteral<BackendRequestService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>() {});
        }

        @Provides @Singleton
        public BackendRequestService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getBackendRequestService(
                ServiceLocator locator,
                BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections,
                VolumeLookupService volumes,
                RuntimeModule runtime) throws Exception {
            BackendRequestService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = 
                    BackendRequestService.newInstance(locator, volumes, connections);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> BackendRequestService<C> newInstance(
            ServiceLocator locator,
            final VolumeLookupService volumes,
            BackendConnectionsService<C> connections) {

        Function<ZNodeLabel.Path, Identifier> lookup = new Function<ZNodeLabel.Path, Identifier>() {
            @Override
            public Identifier apply(ZNodeLabel.Path input) {
                return volumes.get(input).getVolume().getId();
            }
        };
        VolumeShardedOperationTranslators translator = new VolumeShardedOperationTranslators(
                new Function<Identifier, Volume>() {
                    @Override
                    public Volume apply(Identifier input) {
                        return volumes.byVolumeId(input).getVolume();
                    }
                });
        BackendRequestService<C> instance = new BackendRequestService<C>(locator, connections, lookup, translator);
        instance.new Advertiser(MoreExecutors.sameThreadExecutor());
        return instance;
    }

    protected final BackendConnectionsService<C> connections;
    protected final ConcurrentMap<Long, ShardedClientConnectionExecutor<C>> sessions;
    protected final ShardedOperationTranslators translator;
    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    
    protected BackendRequestService(
            ServiceLocator locator,
            BackendConnectionsService<C> connections,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ShardedOperationTranslators translator) {
        super(locator);
        this.connections = connections;
        this.sessions = Maps.newConcurrentMap();
        this.lookup = lookup;
        this.translator = translator;
    }

    @Override
    public ShardedClientConnectionExecutor<C> get(MessageSessionOpen message) {
        ConnectMessage.Request request = ConnectMessage.Request.NewRequest.newInstance(TimeValue.create(message.getTimeOutMillis(), TimeUnit.MILLISECONDS), connections.zxids().get());
        C connection = connections.get();
        ShardedClientConnectionExecutor<C> client = 
                ShardedClientConnectionExecutor.newInstance(
                        connection, translator, lookup, request, connection);
        sessions.putIfAbsent(message.getSessionId(), client);
        // TODO if not absent
        return client;
    }

    public ShardedClientConnectionExecutor<C> remove(MessageSessionClose message) {
        ShardedClientConnectionExecutor<C> client = sessions.remove(message.getSessionId());
        if (client != null) {
            // TODO close
        }
        return client;
    }
    
    public ShardedClientConnectionExecutor<C> get(Long sessionId) {
        return sessions.get(sessionId);
    }
    
    public class Advertiser implements Service.Listener {

        public Advertiser(Executor executor) {
            addListener(this, executor);
        }
        
        @Override
        public void starting() {
        }

        @Override
        public void running() {
            Materializer<?,?> materializer = locator().getInstance(ControlMaterializerService.class).materializer();
            Identifier myEntity = locator().getInstance(PeerConfiguration.class).getView().id();
            BackendView view = locator().getInstance(BackendConfiguration.class).getView();
            try {
                BackendConfiguration.advertise(myEntity, view, materializer);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void stopping(State from) {
        }

        @Override
        public void terminated(State from) {
        }

        @Override
        public void failed(State from, Throwable failure) {
        }
    }
}
