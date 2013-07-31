package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.orchestra.VolumeAssignmentService;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.CachedFunction;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.VolumeLookupService;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.server.ConnectTableProcessor;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpiringSessionService;
import edu.uw.zookeeper.server.ExpiringSessionTable;
import edu.uw.zookeeper.server.SessionParametersPolicy;
import edu.uw.zookeeper.server.SessionTable;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Generator;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.TaskExecutor;

@DependsOn({EnsembleConnectionsService.class, VolumeLookupService.class, VolumeAssignmentService.class})
public class FrontendServerExecutor extends DependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            bind(ServerTaskExecutor.class).to(FrontendServerTaskExecutor.class).in(Singleton.class);
            bind(new TypeLiteral<Generator<Long>>() {}).to(ZxidEpochIncrementer.class).in(Singleton.class);
            bind(new TypeLiteral<Reference<Long>>() {}).to(ZxidEpochIncrementer.class).in(Singleton.class);
        }

        @Provides @Singleton
        public ExpiringSessionTable getSessionTable(
                Configuration configuration,
                Factory<Publisher> publishers,
                ScheduledExecutorService executor,
                ServiceMonitor monitor) {
            SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(configuration);
            ExpiringSessionTable sessions = ExpiringSessionTable.newInstance(publishers.get(), policy);
            ExpiringSessionService expires = ExpiringSessionService.newInstance(sessions, executor, configuration);   
            monitor.add(expires);
            return sessions;
        }
        
        @Provides @Singleton
        public ZxidEpochIncrementer getZxids() {
            return ZxidEpochIncrementer.fromZero();
        }

        @Provides @Singleton
        public FrontendServerExecutor getServerExecutor(
                ServiceLocator locator,
                VolumeLookupService volumes,
                VolumeAssignmentService assignments,
                EnsembleConnectionsService peers,
                Executor executor,
                ExpiringSessionTable sessions,
                DependentServiceMonitor monitor,
                Generator<Long> zxids) {
            return monitor.add(FrontendServerExecutor.newInstance(
                    volumes, assignments, peers, executor, sessions, zxids, locator));
        }

        @Provides @Singleton
        public FrontendServerTaskExecutor getServerTaskExecutor(
                FrontendServerExecutor server) {
            return server.asTaskExecutor();
        }
    }
    
    public static FrontendServerExecutor newInstance(
            VolumeLookupService volumes,
            VolumeAssignmentService assignments,
            EnsembleConnectionsService peers,
            Executor executor,
            SessionTable sessions,
            Generator<Long> zxids,
            ServiceLocator locator) {
        ConcurrentMap<Long, FrontendSessionExecutor> handlers = new MapMaker().makeMap();
        FrontendServerTaskExecutor server = FrontendServerTaskExecutor.newInstance(handlers, volumes, assignments, peers, executor, sessions, zxids);
        return new FrontendServerExecutor(handlers, server, locator);
    }
    
    protected final ServiceLocator locator;
    protected final FrontendServerTaskExecutor executor;
    protected final ConcurrentMap<Long, FrontendSessionExecutor> handlers;
    
    protected FrontendServerExecutor(
            ConcurrentMap<Long, FrontendSessionExecutor> handlers,
            FrontendServerTaskExecutor executor,
            ServiceLocator locator) {
        this.locator = locator;
        this.handlers = handlers;
        this.executor = executor;
        
        new ClientPeerConnectionListener(locator().getInstance(PeerConnectionsService.class));
    }
    
    public FrontendServerTaskExecutor asTaskExecutor() {
        return executor;
    }
    
    @Override
    protected ServiceLocator locator() {
        return locator;
    }

    protected static class FrontendServerTaskExecutor extends ServerTaskExecutor {
        public static FrontendServerTaskExecutor newInstance(
                ConcurrentMap<Long, FrontendSessionExecutor> handlers,
                VolumeLookupService volumes,
                VolumeAssignmentService assignments,
                EnsembleConnectionsService connections,
                Executor executor,
                SessionTable sessions,
                Generator<Long> zxids) {
            TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                    ServerTaskExecutor.ProcessorExecutor.of(
                            FourLetterRequestProcessor.getInstance());
            TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> connectExecutor = 
                    ServerTaskExecutor.ProcessorExecutor.of(
                            new ConnectProcessor(
                                handlers,
                                volumes.lookup(),
                                assignments.lookup(),
                                connections.getEnsembleForPeer(),
                                connections.getConnectionForEnsemble(),
                                ConnectTableProcessor.create(sessions, zxids),
                                executor));
            SessionTaskExecutor sessionExecutor = 
                    new SessionTaskExecutor(handlers);
            return new FrontendServerTaskExecutor(
                    anonymousExecutor,
                    connectExecutor,
                    sessionExecutor);
        }
        
        public FrontendServerTaskExecutor(
                TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
                TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> connectExecutor,
                SessionTaskExecutor sessionExecutor) {
            super(anonymousExecutor, connectExecutor, sessionExecutor);
        }
        
        @Override
        public SessionTaskExecutor getSessionExecutor() {
            return (SessionTaskExecutor) sessionExecutor;
        }
    }
    
    protected static class SessionTaskExecutor implements TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> {

        protected final ConcurrentMap<Long, FrontendSessionExecutor> handlers;

        public SessionTaskExecutor(
                ConcurrentMap<Long, FrontendSessionExecutor> handlers) {
            this.handlers = handlers;
        }
        
        @Override
        public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
                SessionOperation.Request<Records.Request> request) {
            FrontendSessionExecutor executor = handlers.get(request.getSessionId());
            // TODO: remove handler from map on disconnect!?
            return executor.submit(ProtocolRequestMessage.of(request.getXid(), request.getRecord()));
        }
    }

    protected static class ConnectProcessor implements Processor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> {

        protected final ConnectTableProcessor processor;
        protected final ConcurrentMap<Long, FrontendSessionExecutor> handlers;
        protected final CachedFunction<ZNodeLabel.Path, Volume> volumeLookup;
        protected final CachedFunction<Identifier, Identifier> assignmentLookup;
        protected final Function<Identifier, Identifier> ensembleForPeer;
        protected final CachedFunction<Identifier, ClientPeerConnection> connectionLookup;
        protected final Executor executor;
        
        public ConnectProcessor(
                ConcurrentMap<Long, FrontendSessionExecutor> handlers,
                CachedFunction<ZNodeLabel.Path, Volume> volumeLookup,
                CachedFunction<Identifier, Identifier> assignmentLookup,
                Function<Identifier, Identifier> ensembleForPeer,
                CachedFunction<Identifier, ClientPeerConnection> connectionLookup,
                ConnectTableProcessor processor,
                Executor executor) {
            this.processor = processor;
            this.volumeLookup = volumeLookup;
            this.assignmentLookup = assignmentLookup;
            this.ensembleForPeer = ensembleForPeer;
            this.connectionLookup = connectionLookup;
            this.executor = executor;
            this.handlers = handlers;
        }
        
        @Override
        public ConnectMessage.Response apply(Pair<ConnectMessage.Request, Publisher> input) {
            ConnectMessage.Response output = processor.apply(input.first());
            if (output instanceof ConnectMessage.Response.Valid) {
                Session session = output.toSession();
                handlers.putIfAbsent(
                        session.id(), 
                        new FrontendSessionExecutor(
                                session, 
                                input.second(),
                                volumeLookup,
                                assignmentLookup,
                                ensembleForPeer,
                                connectionLookup,
                                executor));
                // TODO: what about reconnects?
            }
            input.second().post(output);
            return output;
        }
    }
    
    protected class ClientPeerConnectionListener {
        
        public ClientPeerConnectionListener(
                PeerConnectionsService<?> connections) {
            connections.clients().register(this);
        }
        
        @Subscribe
        public void handleConnection(ClientPeerConnection connection) {
            new ClientPeerConnectionDispatcher(connection);
        }
    }
    
    protected class ClientPeerConnectionDispatcher {
        protected final ClientPeerConnection connection;
        
        public ClientPeerConnectionDispatcher(
                ClientPeerConnection connection) {
            this.connection = connection;
            
            connection.register(this);
        }
        
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                for (FrontendSessionExecutor handler: handlers.values()) {
                    handler.handleTransition(connection.remoteAddress().getIdentifier(), event);
                }
                try {
                    connection.unregister(this);
                } catch (IllegalArgumentException e) {}
            }
        }
        
        @Subscribe
        public void handleMessage(MessagePacket message) {
            switch (message.first().type()) {
            case MESSAGE_TYPE_SESSION_RESPONSE:
            {
                MessageSessionResponse body = message.getBody(MessageSessionResponse.class);
                FrontendSessionExecutor handler = handlers.get(body.getSessionId());
                handler.handleResponse(connection.remoteAddress().getIdentifier(), body.getResponse());
                break;
            }
            default:
                break;
            }
        }
    }
}
