package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Optional;
import com.google.common.collect.MapMaker;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.TaskExecutor;
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
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
import edu.uw.zookeeper.server.SessionManager;
import edu.uw.zookeeper.server.SimpleServerExecutor;
import edu.uw.zookeeper.server.SimpleServerExecutor.SimpleConnectExecutor;

@DependsOn({RegionsConnectionsService.class, VolumeCacheService.class, AssignmentCacheService.class})
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
            bind(SessionManager.class).to(new TypeLiteral<SimpleConnectExecutor<FrontendSessionExecutor>>() {}).in(Singleton.class);
            bind(ResponseProcessor.class).in(Singleton.class);
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
        public ClientPeerConnectionDispatchers getDispatchers(
                RegionsConnectionsService connections) {
            return ClientPeerConnectionDispatchers.newInstance(connections);
        }
        
        @Provides @Singleton
        public SimpleConnectExecutor<FrontendSessionExecutor> getConnectExecutor(
                Configuration configuration,
                ZxidReference lastZxid,
                ConcurrentMap<Long, FrontendSessionExecutor> sessions,
                Provider<ResponseProcessor> processor,
                VolumeCacheService volumes,
                AssignmentCacheService assignments,
                ClientPeerConnectionDispatchers dispatchers,
                ScheduledExecutorService scheduler,
                Executor executor) {
            ParameterizedFactory<Session, FrontendSessionExecutor> factory = FrontendSessionExecutor.factory(
                    processor,
                    volumes.byPath(),
                    assignments.get().asLookup(),
                    dispatchers.dispatchers(),
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
        public ServerExecutor<FrontendSessionExecutor> getServerExecutor(
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
        injector.getInstance(ClientPeerConnectionDispatchers.class);
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
}
