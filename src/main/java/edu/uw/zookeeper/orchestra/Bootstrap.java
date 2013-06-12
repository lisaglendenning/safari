package edu.uw.zookeeper.orchestra;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.Connection.CodecFactory;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class Bootstrap extends AbstractIdleService {

    public static Bootstrap newInstance(
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
        return new Bootstrap(runtime, controlClient, frontend, backend);
    }
    
    protected final RuntimeModule runtime;
    protected final ControlClientService controlClient;
    protected final FrontendService frontend;
    protected final Factories.LazyHolder<BackendService> backend;

    public Bootstrap(
            RuntimeModule runtime,
            ControlClientService controlClient, 
            FrontendService frontend,
            Factories.LazyHolder<BackendService> backend) {
        this.runtime = runtime;
        this.controlClient = controlClient;
        this.backend = backend;
        this.frontend = frontend;
    }
    
    public BackendService backend() {
        return backend.get();
    }
    
    @Override
    protected void startUp() throws Exception {
        Materializer materializer = controlClient.materializer();
        Materializer.Operator operator = materializer.operator();

        ServerInetAddressView myAddress = frontend.address();
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
        
        // Register my identifier
        Orchestra.Ensembles.Entity.Conductors ensembleConductors = Orchestra.Ensembles.Entity.Conductors.of(myEnsemble);
        result = operator.create(ensembleConductors.path()).submit().get();
        Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());
        Orchestra.Ensembles.Entity.Conductors.Member member = Orchestra.Ensembles.Entity.Conductors.Member.of(myEntity.get(), ensembleConductors);
        result = operator.create(member.path()).submit().get();
        Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());
        
        // Propose myself as leader if there is a vacancy
        Orchestra.Ensembles.Entity.Leader ensembleLeader = Orchestra.Ensembles.Entity.Leader.create(myEntity.get(), myEnsemble, materializer);

        // Global barrier - wait for every ensemble to elect a leader
        Predicate<Materializer> allLeaders = new Predicate<Materializer>() {
            @Override
            public boolean apply(@Nullable Materializer input) {
                ZNodeLabel.Path root = Control.path(Orchestra.Ensembles.class);
                ZNodeLabel.Component label = Control.path(Orchestra.Ensembles.Entity.Leader.class).tail();
                boolean done = true;
                for (Materializer.MaterializedNode e: input.get(root).values()) {
                    if (! e.containsKey(label)) {
                        done = false;
                        break;
                    }
                }
                return done;
            }
        };
        Control.FetchUntil.newInstance(Control.path(Orchestra.Ensembles.class), allLeaders, materializer, MoreExecutors.sameThreadExecutor()).get();

        Identifier.Space ensembles = Identifier.Space.newInstance();
        for (ZNodeLabel.Component label: materializer.get(Control.path(Orchestra.Ensembles.class)).keySet()) {
            ensembles.add(Identifier.valueOf(label.toString()));
        }
        
        if (myEntity.get().equals(ensembleLeader.get())) {
            // create root volume if there are no volumes
            ZNodeLabel.Path path = Control.path(Orchestra.Volumes.class);
            operator.getChildren(path).submit().get();
            if (materializer.get(path).isEmpty()) {
                VolumeDescriptor rootVolume = VolumeDescriptor.of(ZNodeLabel.Path.root());
                Orchestra.Volumes.Entity.create(rootVolume, materializer);
            }
            
            // Calculate "my" volumes
            List<Orchestra.Volumes.Entity> myVolumes = Lists.newLinkedList();
            for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> e: materializer.get(Control.path(Orchestra.Volumes.class)).entrySet()) {
                Orchestra.Volumes.Entity v = Orchestra.Volumes.Entity.valueOf(e.getKey().toString());
                if (ensembles.ceiling(v.get()).equals(myEnsemble.get())) {
                    myVolumes.add(v);
                }
            }
            
            // Try to acquire my volumes
            for (Orchestra.Volumes.Entity v: myVolumes) {
                Orchestra.Volumes.Entity.Ensemble.create(myEnsemble.get(), v, materializer);
            }
        }
        
        // Global barrier = Wait for all volumes to be assigned
        Predicate<Materializer> allAssigned = new Predicate<Materializer>() {
            @Override
            public boolean apply(@Nullable Materializer input) {
                ZNodeLabel.Path root = Control.path(Orchestra.Volumes.class);
                ZNodeLabel.Component label = Control.path(Orchestra.Volumes.Entity.Ensemble.class).tail();
                boolean done = true;
                for (Materializer.MaterializedNode e: input.get(root).values()) {
                    if (! e.containsKey(label)) {
                        done = false;
                        break;
                    }
                }
                return done;
            }
        };
        Control.FetchUntil.newInstance(Control.path(Orchestra.Volumes.class), allAssigned, materializer, MoreExecutors.sameThreadExecutor()).get();
        
        for (Materializer.MaterializedNode node: materializer.trie()) {
            System.out.println(node);
        }

        // TODO: server (?)
        runtime.serviceMonitor().add(frontend);
        frontend.startAndWait();
    }

    @Override
    protected void shutDown() throws Exception {

    }
}
