package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.ConductorService;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.VolumeLookupService;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionClose;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpen;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.TimeValue;

public class BackendRequestService<C extends Connection<? super Operation.Request>> extends AbstractIdleService implements ParameterizedFactory<MessageSessionOpen, ShardedClientConnectionExecutor<C>> {

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
            BackendRequestService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = new BackendRequestService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>(locator, volumes, connections);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }

    protected final ServiceLocator locator;
    protected final VolumeLookupService volumes;
    protected final BackendConnectionsService<C> connections;
    protected final ConcurrentMap<Long, ShardedClientConnectionExecutor<C>> sessions;
    protected final VolumeShardedOperationTranslators translator;
    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    
    protected BackendRequestService(
            ServiceLocator locator,
            final VolumeLookupService volumes,
            BackendConnectionsService<C> connections) {
        this.locator = locator;
        this.connections = connections;
        this.volumes = volumes;
        this.sessions = Maps.newConcurrentMap();
        this.lookup = new Function<ZNodeLabel.Path, Identifier>() {
            @Override
            public Identifier apply(ZNodeLabel.Path input) {
                return volumes.get(input).getVolume().getId();
            }
        };
        this.translator = new VolumeShardedOperationTranslators(
                new Function<Identifier, Volume>() {
                    @Override
                    public Volume apply(Identifier input) {
                        return volumes.byVolumeId(input).getVolume();
                    }
                });
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
    
    protected void register() throws InterruptedException, ExecutionException, KeeperException {
        Materializer<?,?> materializer = locator.getInstance(ControlClientService.class).materializer();
        Orchestra.Peers.Entity entityNode = Orchestra.Peers.Entity.of(locator.getInstance(ConductorService.class).view().id());
        Orchestra.Peers.Entity.Backend backendNode = Orchestra.Peers.Entity.Backend.create(connections.view(), entityNode, materializer);
        if (! connections.view().equals(backendNode.get())) {
            throw new IllegalStateException(backendNode.get().toString());
        }
    }
    
    @Override
    protected void startUp() throws Exception {
        connections.start().get();
        
        register();
    }

    @Override
    protected void shutDown() throws Exception {
    }

}
