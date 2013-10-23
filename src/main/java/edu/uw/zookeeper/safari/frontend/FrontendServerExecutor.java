package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.References;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.server.SessionManager;
import edu.uw.zookeeper.server.SimpleServerExecutor;
import edu.uw.zookeeper.server.SimpleServerExecutor.SimpleConnectExecutor;

@DependsOn({RegionConnectionsService.class, VolumeCacheService.class, AssignmentCacheService.class})
public class FrontendServerExecutor extends DependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            bind(ZxidGenerator.class).to(ZxidEpochIncrementer.class).in(Singleton.class);
            bind(ZxidReference.class).to(ZxidGenerator.class).in(Singleton.class);
            bind(SessionManager.class).to(SimpleConnectExecutor.class).in(Singleton.class);
            bind(ServerExecutor.class).to(SimpleServerExecutor.class).in(Singleton.class);
            bind(ResponseProcessor.class).in(Singleton.class);
            bind(ClientPeerConnectionListener.class).in(Singleton.class);
            bind(FrontendServerExecutor.class).in(Singleton.class);
        }
        
        @Provides @Singleton
        public ConcurrentMap<Long, FrontendSessionExecutor> getSessionExecutors() {
            return new MapMaker().makeMap();
        }
        
        @Provides @Singleton
        public TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor() {
            return SimpleServerExecutor.ProcessorTaskExecutor.of(
                    FourLetterRequestProcessor.newInstance());
        }
        
        @Provides @Singleton
        public SimpleConnectExecutor<FrontendSessionExecutor> getConnectExecutor(
                Configuration configuration,
                ZxidReference lastZxid,
                ConcurrentMap<Long, FrontendSessionExecutor> sessions,
                Provider<ResponseProcessor> processor,
                VolumeCacheService volumes,
                AssignmentCacheService assignments,
                PeerToEnsembleLookup peerToEnsemble,
                RegionConnectionsService connections,
                ScheduledExecutorService scheduler,
                Executor executor) {
            ParameterizedFactory<Pair<Session, PubSubSupport<Object>>, FrontendSessionExecutor> factory = FrontendSessionExecutor.factory(
                    processor,
                    volumes.byPath(),
                    assignments.get().asLookup(),
                    peerToEnsemble.get().asLookup().cached(),
                    connections.getConnectionForEnsemble(),
                    scheduler, 
                    executor);
            return SimpleConnectExecutor.defaults(
                    sessions, factory, configuration, lastZxid);
        }

        @Provides @Singleton
        public ZxidEpochIncrementer getZxids() {
            return ZxidEpochIncrementer.fromZero();
        }

        @Provides @Singleton
        public SimpleServerExecutor<FrontendSessionExecutor> getServerExecutor(
                TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor,
                SimpleConnectExecutor<FrontendSessionExecutor> connectExecutor,
                ConcurrentMap<Long, FrontendSessionExecutor> sessionExecutors) {
            return new SimpleServerExecutor<FrontendSessionExecutor>(sessionExecutors, connectExecutor, anonymousExecutor);
        }
    }
    
    public static FrontendServerExecutor newInstance(
            Injector injector) {
        return new FrontendServerExecutor(injector);
    }
    
    @Inject
    protected FrontendServerExecutor(
            Injector injector) {
        super(injector);
    }
    
    @Override
    protected void startUp() throws Exception {
        super.startUp();
        injector.getInstance(ClientPeerConnectionListener.class);
    }

    public static class ResponseProcessor implements Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> {

        public static ResponseProcessor create(
                SessionManager sessions,
                ZxidGenerator zxids) {
            return new ResponseProcessor(sessions, zxids);
        }
        
        protected final SessionManager sessions;
        protected final AssignZxidProcessor zxids;

        @Inject
        public ResponseProcessor(
                SessionManager sessions,
                ZxidGenerator zxids) {
            this(sessions, AssignZxidProcessor.newInstance(zxids));
        }
        
        public ResponseProcessor(
                SessionManager sessions,
                AssignZxidProcessor zxids) {
            this.sessions = sessions;
            this.zxids = zxids;
        }
        
        @Override
        public Message.ServerResponse<?> apply(Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>> input) {
            Optional<Operation.ProtocolRequest<?>> request = input.second().first();
            Records.Response response = input.second().second();
            int xid;
            if (response instanceof Operation.RequestId) {
                xid = ((Operation.RequestId) response).xid();
            } else {
                xid = request.get().xid();
            }
            OpCode opcode;
            if (OpCodeXid.has(xid)) {
                opcode = OpCodeXid.of(xid).opcode();
            } else {
                opcode = request.get().record().opcode();
            }
            long zxid = zxids.apply(opcode);
            if (opcode == OpCode.CLOSE_SESSION) {
                sessions.remove(input.first());
            }
            return ProtocolResponseMessage.of(xid, zxid, response);
        }
    }

    @net.engio.mbassy.listener.Listener(references = References.Strong)
    protected static class ClientPeerConnectionListener extends Service.Listener {

        protected final Logger logger = LogManager.getLogger(getClass());
        protected final ClientPeerConnections connections;
        protected final ServerExecutor<FrontendSessionExecutor> sessions;
        protected final ConcurrentMap<ClientPeerConnection<?>, ClientPeerConnectionDispatcher> dispatchers;
        
        @Inject
        public ClientPeerConnectionListener(
                ServerExecutor<FrontendSessionExecutor> sessions,
                ClientPeerConnections connections) {
            this.sessions = sessions;
            this.connections = connections;
            // TODO: can be weak references?
            this.dispatchers = new MapMaker().makeMap();
            
            connections.addListener(this, MoreExecutors.sameThreadExecutor());
            if (connections.isRunning()) {
                running();
            }
        }

        @Override
        public void running() {
            connections.subscribe(this);
            for (ClientPeerConnection<?> c: connections) {
                handleConnection(c);
            }
        }

        @Override
        public void stopping(State from) {
            connections.unsubscribe(this);
        }
        
        @Handler
        public void handleConnection(ClientPeerConnection<?> connection) {
            new ClientPeerConnectionDispatcher(connection);
        }

        @net.engio.mbassy.listener.Listener(references = References.Strong)
        protected class ClientPeerConnectionDispatcher extends Factories.Holder<ClientPeerConnection<?>> {

            public ClientPeerConnectionDispatcher(
                    ClientPeerConnection<?> connection) {
                super(connection);
                
                if (dispatchers.putIfAbsent(connection, this) == null) {
                    connection.subscribe(this);
                    
                    if (connection.state().compareTo(Connection.State.CONNECTION_CLOSED) >= 0) {
                        handleTransition(Automaton.Transition.create(Connection.State.CONNECTION_CLOSED, connection.state()));
                    }
                }
            }

            @Handler
            public void handleTransition(Automaton.Transition<?> event) {
                if (Connection.State.CONNECTION_CLOSED == event.to()) {
                    get().unsubscribe(this);
                    dispatchers.remove(get(), this);
                    for (FrontendSessionExecutor e: sessions) {
                        e.handleTransition(Pair.<Identifier, Automaton.Transition<?>>create(
                                get().remoteAddress().getIdentifier(), event));
                    }
                }
            }

            @Handler
            public void handleMessage(MessagePacket<?> message) {
                switch (message.getHeader().type()) {
                case MESSAGE_TYPE_SESSION_RESPONSE:
                {
                    MessageSessionResponse body = (MessageSessionResponse) message.getBody();
                    FrontendSessionExecutor e = sessions.sessionExecutor(body.getIdentifier());
                    if (e != null) {
                        e.handleResponse(Pair.<Identifier, ShardedResponseMessage<?>>create(
                                get().remoteAddress().getIdentifier(), body.getValue()));
                    } else {
                        logger.warn("Ignoring {}", message);
                    }
                    break;
                }
                default:
                    break;
                }
            }
        }
    }
}
