package edu.uw.zookeeper.orchestra;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.zookeeper.KeeperException;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.ConductorPeerService.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class EnsemblePeerService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public EnsemblePeerService getEnsemblePeerService(
                ServiceLocator locator,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            EnsemblePeerService instance = new EnsemblePeerService(locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }

    protected final ServiceLocator locator;
    protected final ControlClientService controlClient;
    protected final ConductorPeerService peerConnections;
    protected final ConcurrentMap<Identifier, Identifier> ensemblePeers;
    
    public EnsemblePeerService(
            ServiceLocator locator) {
        this.locator = locator;
        this.controlClient = locator.getInstance(ControlClientService.class);
        this.peerConnections = locator.getInstance(ConductorPeerService.class);
        this.ensemblePeers = new ConcurrentHashMap<Identifier, Identifier>();
    }
    
    public Identifier getPeerForEnsemble(Identifier ensemble) throws InterruptedException, ExecutionException, KeeperException {
        Identifier peer = ensemblePeers.get(ensemble);
        if (peer == null) {
            EnsembleMemberService member = locator.getInstance(EnsembleMemberService.class);
            if (ensemble.equals(member.ensemble())) {
                peer = member.id();
            } else {
                Orchestra.Ensembles.Entity.Conductors conductors = Orchestra.Ensembles.Entity.Conductors.of(Orchestra.Ensembles.Entity.of(ensemble));
                List<Orchestra.Ensembles.Entity.Conductors.Member> members = conductors.lookup(controlClient.materializer());
                Collections.shuffle(members);
                for (Orchestra.Ensembles.Entity.Conductors.Member e: members) {
                    Orchestra.Conductors.Entity.Presence presence = 
                            Orchestra.Conductors.Entity.Presence.of(
                                    Orchestra.Conductors.Entity.of(e.get()));
                    if (presence.exists(controlClient.materializer())) {
                        peer = e.get();
                        break;
                    }
                }
            }
            ensemblePeers.putIfAbsent(ensemble, peer);
            peer = ensemblePeers.get(ensemble);
        }
        return peer;
    }

    public ClientPeerConnection getConnectionForEnsemble(Identifier ensemble) throws InterruptedException, ExecutionException, KeeperException {
        ClientPeerConnection connection = null;
        Identifier peer = getPeerForEnsemble(ensemble);
        if (peer != null) {
            connection = peerConnections.getClientConnection(peer);
            if (connection == null) {
                connection = peerConnections.connect(peer).get();
            }
        }
        return connection;
    }

    @Override
    protected void startUp() throws Exception {
        peerConnections.start().get();
        
        ConnectToAll connectToAll = new ConnectToAll();
        connectToAll.call().get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
    
    protected class ConnectToAll extends TreeFetcher {
        
        
        protected ConnectToAll() {
            super(TreeFetcher.Parameters.of(EnumSet.of(OpCode.GET_CHILDREN), false), 
                    Control.path(Orchestra.Ensembles.class),
                    controlClient.materializer(), 
                    SettableFuturePromise.<ZNodeLabel.Path>create(),
                    MoreExecutors.sameThreadExecutor());
        }
        
        @Override
        protected TreeFetcherActor newActor() {
            return new ConnectToAllActor(promise, parameters, root, client, executor);
        }

        protected class ConnectToAllActor extends TreeFetcherActor {
        
            protected final Pending<ClientPeerConnection, ListenableFuture<ClientPeerConnection>> pendingConnects;
            
            protected ConnectToAllActor(
                    Promise<ZNodeLabel.Path> promise,
                    Parameters parameters, 
                    ZNodeLabel.Path root, 
                    ClientExecutor client,
                    Executor executor) {
                super(promise, parameters, root, client, executor);
                this.pendingConnects = Pending.newInstance();
            }

            @Override
            protected void runAll() throws Exception {
                ListenableFuture<ClientPeerConnection> next;
                while ((next = pendingConnects.poll()) != null) {
                    try {
                        next.get();
                    } catch (Exception e) {
                        // TODO
                        stop();
                    }
                }
                
                super.runAll();
            }
            
            @Override
            protected void handleResult(Operation.SessionResult result) throws KeeperException, InterruptedException, ExecutionException {
                Operation.Request request = result.request().request();
                Operation.Response reply = Operations.maybeError(result.reply().reply(), KeeperException.Code.NONODE, request.toString());
                if (reply instanceof Records.ChildrenHolder) {
                    ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathHolder) request).getPath());
                    ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
                    ZNodeLabel.Component pathTail = path.tail();
                    if (Orchestra.Ensembles.Entity.Conductors.LABEL.equals(pathTail)) {
                        Identifier ensemble = Identifier.valueOf(controlClient.materializer().get(pathHead).parent().orNull().label().toString());
                        ListenableFutureTask<ClientPeerConnection> task = ListenableFutureTask.create(new ConnectTask(ensemble));
                        pendingConnects.add(task);
                        task.addListener(this, executor);
                        executor.execute(task);
                    }
                    if (root.equals(path) 
                            || root.equals(pathHead) 
                            || Orchestra.Ensembles.Entity.Conductors.LABEL.equals(pathTail)) {
                        for (String child: ((Records.ChildrenHolder) reply).getChildren()) {
                            send(ZNodeLabel.Path.of(path, ZNodeLabel.Component.of(child)));
                        }
                    }
                }
            }
            
            @Override
            protected void runExit() {
                if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                    if (!mailbox.isEmpty() || !pending.isEmpty() || !pendingConnects.isEmpty()) {
                        schedule();
                    } else if (pending.delegate().isEmpty() && pendingConnects.delegate().isEmpty()) {
                        // We're done!
                        stop();
                    }
                }
            }
        }

        protected class ConnectTask implements Callable<ClientPeerConnection> {
            
            protected final Identifier ensemble;
            
            protected ConnectTask(Identifier ensemble) {
                this.ensemble = ensemble;
            }

            @Override
            public ClientPeerConnection call() throws Exception {
                return getConnectionForEnsemble(ensemble);
            }
        }
    }
}
