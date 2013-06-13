package edu.uw.zookeeper.orchestra;

import java.net.SocketAddress;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.Connection.CodecFactory;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class Conductor extends AbstractIdleService implements Publisher {

    public static Conductor newInstance(
            final RuntimeModule runtime,
            final ParameterizedFactory<CodecFactory<Message.ClientSessionMessage, Message.ServerSessionMessage, PingingClientCodecConnection>, Factory<ChannelClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection>>> clientConnectionFactory,
            final ParameterizedFactory<Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>>> serverConnectionFactory) {
        final ClientModule clientModule = ClientModule.newInstance(runtime, clientConnectionFactory);
        final ControlClientService controlClient = AbstractMain.monitors(runtime.serviceMonitor()).apply(
                ControlClientService.newInstance(runtime, clientModule));
        Factories.LazyHolder<BackendService> backend = Factories.synchronizedLazyFrom(new Factory<BackendService>() {
            @Override
            public BackendService get() {
                BackendService backend;
                try {
                    backend = AbstractMain.monitors(runtime.serviceMonitor()).apply(
                            BackendService.newInstance(runtime, clientModule));
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
                backend.startAndWait();
                return backend;
            }
        });
        FrontendService frontend = FrontendService.newInstance(runtime, serverConnectionFactory);
        return new Conductor(runtime, controlClient, frontend, backend);
    }
    
    protected final RuntimeModule runtime;
    protected final Publisher publisher;
    protected final ControlClientService controlClient;
    protected final FrontendService frontend;
    protected final Factories.LazyHolder<BackendService> backend;
    protected final WatchPromiseTrie watches;
    protected volatile EnsembleMember member;

    public Conductor(
            RuntimeModule runtime,
            ControlClientService controlClient, 
            FrontendService frontend,
            Factories.LazyHolder<BackendService> backend) {
        this.runtime = runtime;
        this.controlClient = controlClient;
        this.backend = backend;
        this.frontend = frontend;
        this.publisher = runtime.publisherFactory().get();
        this.watches = WatchPromiseTrie.newInstance();
        this.member = null;
    }
    
    public RuntimeModule runtime() {
        return runtime;
    }
    
    public ControlClientService controlClient() {
        return controlClient;
    }
    
    public FrontendService frontend() {
        return frontend;
    }
    
    public BackendService backend() {
        return backend.get();
    }
    
    public WatchPromiseTrie watches() {
        return watches;
    }
    
    public EnsembleMember member() {
        return member;
    }
    
    @Override
    protected void startUp() throws Exception {
        this.register(watches);
        WatchEventPublisher.newInstance(this, controlClient());
        
        Materializer materializer = controlClient().materializer();
        Materializer.Operator operator = materializer.operator();

        ServerInetAddressView myAddress = frontend().address();
        BackendView backendView = backend().view();

        // Create my entity
        Orchestra.Conductors.Entity myEntity = Orchestra.Conductors.Entity.create(myAddress, materializer);
        
        // Register presence
        Orchestra.Conductors.Entity.Presence entityPresence = Orchestra.Conductors.Entity.Presence.of(myEntity);
        Operation.SessionResult result = operator.create(entityPresence.path()).submit().get();
        Operations.unlessError(result.reply().reply(), result.toString());
        
        // Register backend
        Orchestra.Conductors.Entity.Backend entityBackend = Orchestra.Conductors.Entity.Backend.create(backendView, myEntity, materializer);
        if (! backendView.equals(entityBackend.get())) {
            throw new IllegalStateException(entityBackend.get().toString());
        }
        
        // Find my ensemble
        EnsembleView<ServerInetAddressView> myView = backendView.getEnsemble();
        Orchestra.Ensembles.Entity myEnsemble = Orchestra.Ensembles.Entity.create(myView, materializer);
        
        // Start ensemble member
        this.member = new EnsembleMember(this, myEntity.get(), myEnsemble.get());
        runtime().serviceMonitor().add(member);
        member.startAndWait();
        
        // TODO: server (?)
        runtime().serviceMonitor().add(frontend());
        frontend().startAndWait();
    }

    @Override
    protected void shutDown() throws Exception {
    }

    @Override
    public void register(Object object) {
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher.unregister(object);
    }

    @Override
    public void post(Object object) {
        publisher.post(object);
    }
}
