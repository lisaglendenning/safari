package edu.uw.zookeeper.orchestra.peer;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.DependentModule;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.orchestra.control.ControlSchema.Ensembles.Entity;
import edu.uw.zookeeper.orchestra.data.VolumeDescriptor;
import edu.uw.zookeeper.protocol.Message;

public class EnsembleMemberService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}

        @Provides @Singleton
        public EnsembleMemberService getEnsembleMember(
                EnsembleConfiguration ensembleConfiguration,
                PeerConfiguration peerConfiguration,
                ControlMaterializerService<?> control,
                ServiceLocator locator) {
            ControlSchema.Ensembles.Entity myEnsemble = ControlSchema.Ensembles.Entity.of(ensembleConfiguration.getEnsemble());
            ControlSchema.Ensembles.Entity.Peers.Member myMember = ControlSchema.Ensembles.Entity.Peers.Member.of(
                    peerConfiguration.getView().id(), 
                    ControlSchema.Ensembles.Entity.Peers.of(myEnsemble));
            EnsembleMemberService instance = 
                    new EnsembleMemberService(myMember, myEnsemble, control);
            return instance;
        }

        @Override
        protected com.google.inject.Module[] getModules() {
            com.google.inject.Module[] modules = { EnsembleConfiguration.module() };
            return modules;
        }
    }
    
    public static class AllEnsemblesHaveLeaders implements Processor<Object, Optional<Boolean>> {
        
        public static ZNodeLabel.Path root() {
            return ROOT;
        }
        
        public static ListenableFuture<Boolean> call(Materializer<?> materializer) {
            return Control.FetchUntil.newInstance(
                    AllEnsemblesHaveLeaders.root(), 
                    new AllEnsemblesHaveLeaders(materializer), 
                    materializer);
        }
        
        protected static final ZNodeLabel.Path ROOT = Control.path(ControlSchema.Ensembles.class);
        
        protected final Materializer<?> materializer;
        
        public AllEnsemblesHaveLeaders(Materializer<?> materializer) {
            this.materializer = materializer;
        }

        @Override
        public Optional<Boolean> apply(Object input) throws Exception {
            Materializer.MaterializedNode root = materializer.get(ROOT);
            if (root != null) {
                for (Materializer.MaterializedNode e: root.values()) {
                    if (! e.containsKey(ControlSchema.Ensembles.Entity.Leader.LABEL)) {
                        return Optional.absent();
                    }
                }
                return Optional.of(Boolean.valueOf(true));
            }
            return Optional.absent();
        }
    }

    protected final ControlMaterializerService<?> control;
    protected final ControlSchema.Ensembles.Entity.Peers.Member myMember;
    protected final ControlSchema.Ensembles.Entity myEnsemble;
    protected final RoleOverseer role;
    
    protected EnsembleMemberService(
            ControlSchema.Ensembles.Entity.Peers.Member myMember, 
            ControlSchema.Ensembles.Entity myEnsemble,
            ControlMaterializerService<?> control) {
        this.control = control;
        this.myEnsemble = myEnsemble;
        this.myMember = myMember;
        this.role = new RoleOverseer();
    }
    
    public Identifier id() {
        return myMember.get();
    }
    
    public Identifier ensemble() {
        return myEnsemble.get();
    }
    
    @Override
    protected void startUp() throws Exception {
        Materializer<Message.ServerResponse<?>> materializer = control.materializer();

        // Register my identifier
        Message.ServerResponse<?> result = materializer.operator().create(Control.path(myMember.parent())).submit().get();
        Operations.maybeError(result.getRecord(), KeeperException.Code.NODEEXISTS);
        result = materializer.operator().create(myMember.path()).submit().get();
        Operations.maybeError(result.getRecord(), KeeperException.Code.NODEEXISTS);

        // Propose myself as leader
        EnsembleRole role = this.role.elect();
        
        // Global barrier - Wait for every ensemble to elect a leader
        AllEnsemblesHaveLeaders.call(materializer).get();
    
        if (EnsembleRole.LEADING == role) {
            // create root volume if there are no volumes
            ZNodeLabel.Path path = Control.path(ControlSchema.Volumes.class);
            materializer.operator().getChildren(path).submit().get();
            if (materializer.get(path).isEmpty()) {
                VolumeDescriptor rootVolume = VolumeDescriptor.all();
                ControlSchema.Volumes.Entity.create(rootVolume, materializer).get();
            }
            
            // Calculate "my" volumes using distance in the identifier space
            Identifier.Space ensembles = Identifier.Space.newInstance();
            for (ZNodeLabel.Component label: materializer.get(Control.path(ControlSchema.Ensembles.class)).keySet()) {
                ensembles.add(Identifier.valueOf(label.toString()));
            }
            List<ControlSchema.Volumes.Entity> myVolumes = Lists.newLinkedList();
            for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> e: materializer.get(Control.path(ControlSchema.Volumes.class)).entrySet()) {
                ControlSchema.Volumes.Entity v = ControlSchema.Volumes.Entity.valueOf(e.getKey().toString());
                if (ensembles.ceiling(v.get()).equals(myEnsemble.get())) {
                    myVolumes.add(v);
                }
            }
            
            // Try to acquire my volumes
            for (ControlSchema.Volumes.Entity v: myVolumes) {
                ControlSchema.Volumes.Entity.Ensemble.create(myEnsemble.get(), v, materializer);
            }
        }        
    }

    @Override
    protected void shutDown() throws Exception {
    }

    protected class RoleOverseer implements FutureCallback<WatchEvent> {
    
        protected final ZNodeLabel.Path leaderPath;
        protected final ControlSchema.Ensembles.Entity.Leader.Proposer<?> proposer;
        protected final Automatons.SynchronizedEventfulAutomaton<EnsembleRole, EnsembleRole> myRole;
        protected final StampedReference.Updater<ControlSchema.Ensembles.Entity.Leader> leader;
        
        public RoleOverseer() {
            this.leaderPath = ZNodeLabel.Path.of(myEnsemble.path(), ControlSchema.Ensembles.Entity.Leader.LABEL);
            this.myRole = Automatons.createSynchronizedEventful(
                    control, Automatons.createSimple(EnsembleRole.LOOKING));
            this.leader = StampedReference.Updater.newInstance(StampedReference.<ControlSchema.Ensembles.Entity.Leader>of(0L, null));
            this.proposer = ControlSchema.Ensembles.Entity.Leader.Proposer.of(
                    control.materializer());
            myRole.register(this);
            control.materializer().register(this);
            subscribeLeaderWatch();
        }
        
        public EnsembleRole elect() throws InterruptedException, ExecutionException {
            ControlSchema.Ensembles.Entity.Leader ensembleLeader = proposer.apply(Entity.Leader.of(myMember.get(), myEnsemble)).get();
            return myRoleFor(ensembleLeader);
        }

        @Override
        public void onSuccess(WatchEvent event) {
            if (leaderPath.equals(event.getPath())) {
                switch (event.getType()) {
                case NodeCreated:
                case NodeDataChanged:
                    subscribeLeaderWatch();
                    control.materializer().operator().getData(leaderPath, true).submit();
                    break;
                case NodeDeleted:
                    subscribeLeaderWatch();
                    control.materializer().operator().exists(leaderPath, true).submit();
                    break;
                case NodeChildrenChanged:
                    throw new AssertionError();
                default:
                    break;
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            // TODO
        }

        @Subscribe
        public void handleViewUpdate(ZNodeViewCache.ViewUpdate event) {
            if (leaderPath.equals(event.path())) {
                Materializer.MaterializedNode node = control.materializer().get(leaderPath);
                Identifier value = (node != null) ? (Identifier) node.get().get() : null;
                setLeader(StampedReference.of(event.updated().stamp(), ControlSchema.Ensembles.Entity.Leader.of(value, myEnsemble)));
            }
        }

        @Subscribe
        public void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
            if (leaderPath.equals(event.path().get())) {
                if (ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED == event.type()) {
                    setLeader(StampedReference.<ControlSchema.Ensembles.Entity.Leader>of(event.path().stamp(), null));
                }
            }
        }
        
        @Subscribe
        public void handleTransition(Automaton.Transition<?> transition) {
            if (transition.type().isAssignableFrom(EnsembleRole.class)) {
                if (EnsembleRole.LOOKING == transition.to()) {
                    try {
                        elect();
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }
            }
        }
        
        protected EnsembleRole myRoleFor(ControlSchema.Ensembles.Entity.Leader leader) {
            if (leader == null) {
                return EnsembleRole.LOOKING;
            } else if (myMember.get().equals(leader.get())) {
                return EnsembleRole.LEADING;
            } else {
                return EnsembleRole.FOLLOWING;
            }
        }

        protected void setLeader(StampedReference<ControlSchema.Ensembles.Entity.Leader> newLeader) {
            StampedReference<ControlSchema.Ensembles.Entity.Leader> prevLeader = leader.setIfGreater(newLeader);
            if (prevLeader.stamp() < newLeader.stamp()) {
                myRole.apply(myRoleFor(newLeader.get()));
            }
        }

        protected void subscribeLeaderWatch() {
            Futures.addCallback(control.watches().subscribe(leaderPath, EnumSet.allOf(Watcher.Event.EventType.class)), this);
        }
    }
}
