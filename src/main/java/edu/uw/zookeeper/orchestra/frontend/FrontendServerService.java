package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.orchestra.peer.EnsemblePeerService;
import edu.uw.zookeeper.orchestra.peer.PeerConfiguration;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.protocol.server.ConnectTableProcessor;
import edu.uw.zookeeper.protocol.server.ExpiringSessionRequestExecutor;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ProtocolResponseProcessor;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.protocol.server.ToTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpiringSessionService;
import edu.uw.zookeeper.server.ExpiringSessionTable;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.server.SessionParametersPolicy;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Processors;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.TaskExecutor;

@DependsOn({EnsemblePeerService.class})
public class FrontendServerService extends DependentService.SimpleDependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(FrontendConfiguration.module());
        }

        @Provides @Singleton
        public ExpiringSessionTable getSessionManager(
                RuntimeModule runtime) {
            SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(runtime.configuration());
            ExpiringSessionTable sessions = ExpiringSessionTable.newInstance(runtime.publisherFactory().get(), policy);
            ExpiringSessionService expires = ExpiringSessionService.newInstance(sessions, runtime.executors().asScheduledExecutorServiceFactory().get(), runtime.configuration());   
            runtime.serviceMonitor().addOnStart(expires);
            return sessions;
        }

        @Provides @Singleton
        public ServerTaskExecutor getServerExecutor(
                EnsemblePeerService peers,
                ExpiringSessionTable sessions,
                RuntimeModule runtime) {
            ZxidEpochIncrementer zxids = ZxidEpochIncrementer.fromZero();
            final ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
            TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                    ServerTaskExecutor.ProcessorExecutor.of(
                            FourLetterRequestProcessor.getInstance());
            TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> connectExecutor = 
                    ServerTaskExecutor.ProcessorExecutor.of(
                            FrontendConnectProcessor.newInstance(
                                    peers,
                                    ConnectTableProcessor.create(sessions, zxids), 
                                    listeners));
            Processor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> processor = 
                    Processors.bridge(
                            ToTxnRequestProcessor.create(
                                    AssignZxidProcessor.newInstance(zxids)), 
                            ProtocolResponseProcessor.create(
                                    ServerApplicationModule.defaultTxnProcessor(ZNodeDataTrie.newInstance(), sessions,
                                            new Function<Long, Publisher>() {
                                                @Override
                                                public @Nullable Publisher apply(@Nullable Long input) {
                                                    return listeners.get(input);
                                                }
                                    })));
            TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> sessionExecutor = 
                    ExpiringSessionRequestExecutor.newInstance(sessions, runtime.executors().asListeningExecutorServiceFactory().get(), listeners, processor);
            return ServerTaskExecutor.newInstance(anonymousExecutor, connectExecutor, sessionExecutor);
        }
        
        @Provides @Singleton
        public FrontendServerService getFrontendServerService(
                FrontendConfiguration configuration, 
                ServerTaskExecutor serverExecutor,
                ServiceLocator locator,
                NettyModule netModule,
                RuntimeModule runtime) throws Exception {
            ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                    netModule.servers().get(
                            ServerApplicationModule.codecFactory(),
                            ServerApplicationModule.connectionFactory()).get(configuration.getAddress().get());
            runtime.serviceMonitor().addOnStart(serverConnections);
            ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> server = ServerConnectionExecutorsService.newInstance(serverConnections, serverExecutor);
            runtime.serviceMonitor().addOnStart(server);
            FrontendServerService instance = new FrontendServerService(configuration.getAddress(), server, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    public static FrontendServerService newInstance(
            ServerInetAddressView address,
            ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections,
            ServiceLocator locator) {
        FrontendServerService instance = new FrontendServerService(address, serverConnections, locator);
        instance.new Advertiser(MoreExecutors.sameThreadExecutor());
        return instance;
    }
    
    protected final ServerInetAddressView address;
    protected final ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections;
    
    protected FrontendServerService(
            ServerInetAddressView address,
            ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections,
            ServiceLocator locator) {
        super(locator);
        this.address = address;
        this.serverConnections = serverConnections;
    }
    
    public ServerInetAddressView address() {
        return address;
    }
    
    public ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections() {
        return serverConnections;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        final ZNodeLabel.Path VOLUMES_PATH = Control.path(Orchestra.Volumes.class);
        
        // Global barrier - Wait for all volumes to be assigned
        Predicate<Materializer<?,?>> allAssigned = new Predicate<Materializer<?,?>>() {
            @Override
            public boolean apply(@Nullable Materializer<?,?> input) {
                ZNodeLabel.Component label = Orchestra.Volumes.Entity.Ensemble.LABEL;
                boolean done = true;
                for (Materializer.MaterializedNode e: input.get(VOLUMES_PATH).values()) {
                    if (! e.containsKey(label)) {
                        done = false;
                        break;
                    }
                }
                return done;
            }
        };
        Control.FetchUntil.newInstance(VOLUMES_PATH, allAssigned, locator().getInstance(ControlMaterializerService.class).materializer(), MoreExecutors.sameThreadExecutor()).get();

        serverConnections().start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        serverConnections().stop().get();
        
        super.shutDown();
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
            Identifier peerId = locator().getInstance(PeerConfiguration.class).getView().id();
            ServerInetAddressView address = locator().getInstance(FrontendConfiguration.class).getAddress();
            try {
                FrontendConfiguration.advertise(peerId, address, materializer);
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