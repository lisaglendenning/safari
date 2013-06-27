package edu.uw.zookeeper.orchestra;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeResponseCache;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Automatons;

public class EnsembleMember extends AbstractIdleService {

    protected final ServiceLocator locator;
    protected final Orchestra.Ensembles.Entity.Conductors.Member myMember;
    protected final Orchestra.Ensembles.Entity myEnsemble;
    protected final RoleOverseer role;
    
    public EnsembleMember(
            Orchestra.Ensembles.Entity.Conductors.Member myMember, 
            Orchestra.Ensembles.Entity myEnsemble,
            ServiceLocator locator) {
        this.locator = locator;
        this.myEnsemble = myEnsemble;
        this.myMember = myMember;
        this.role = new RoleOverseer();
    }
    
    @Override
    protected void startUp() throws Exception {        
        Materializer materializer = locator.getInstance(ControlClientService.class).materializer();
        Materializer.Operator operator = materializer.operator();

        // Register my identifier
        Operation.SessionResult result = operator.create(Control.path(myMember.parent())).submit().get();
        Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());
        result = operator.create(myMember.path()).submit().get();
        Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());

        // Propose myself as leader
        EnsembleRole role = this.role.elect();
        
        // Global barrier - Wait for every ensemble to elect a leader
        Predicate<Materializer> allLeaders = new Predicate<Materializer>() {
            @Override
            public boolean apply(@Nullable Materializer input) {
                ZNodeLabel.Path root = Control.path(Orchestra.Ensembles.class);
                ZNodeLabel.Component label = Orchestra.Ensembles.Entity.Leader.LABEL;
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
    
        if (EnsembleRole.LEADING == role) {
            // create root volume if there are no volumes
            ZNodeLabel.Path path = Control.path(Orchestra.Volumes.class);
            operator.getChildren(path).submit().get();
            if (materializer.get(path).isEmpty()) {
                VolumeDescriptor rootVolume = VolumeDescriptor.of(ZNodeLabel.Path.root());
                Orchestra.Volumes.Entity.create(rootVolume, materializer);
            }
            
            // Calculate "my" volumes using distance in the identifier space
            Identifier.Space ensembles = Identifier.Space.newInstance();
            for (ZNodeLabel.Component label: materializer.get(Control.path(Orchestra.Ensembles.class)).keySet()) {
                ensembles.add(Identifier.valueOf(label.toString()));
            }
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
        
        // Global barrier - Wait for all volumes to be assigned
        Predicate<Materializer> allAssigned = new Predicate<Materializer>() {
            @Override
            public boolean apply(@Nullable Materializer input) {
                ZNodeLabel.Path root = Control.path(Orchestra.Volumes.class);
                ZNodeLabel.Component label = Orchestra.Volumes.Entity.Ensemble.LABEL;
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
    }

    @Override
    protected void shutDown() throws Exception {
    }

    protected class RoleOverseer implements FutureCallback<WatchEvent> {
    
        protected final ControlClientService controlClient;
        protected final ZNodeLabel.Path leaderPath;
        protected final Orchestra.Ensembles.Entity.Leader.Proposer proposer;
        protected final Automatons.SynchronizedEventfulAutomaton<EnsembleRole, EnsembleRole> myRole;
        protected final StampedReference.Updater<Orchestra.Ensembles.Entity.Leader> leader;
        
        public RoleOverseer() {
            this.controlClient = locator.getInstance(ControlClientService.class);
            this.leaderPath = ZNodeLabel.Path.of(myEnsemble.path(), Orchestra.Ensembles.Entity.Leader.LABEL);
            this.myRole = Automatons.createSynchronizedEventful(
                    controlClient, Automatons.createSimple(EnsembleRole.LOOKING));
            this.leader = StampedReference.Updater.newInstance(StampedReference.<Orchestra.Ensembles.Entity.Leader>of(0L, null));
            this.proposer = new Orchestra.Ensembles.Entity.Leader.Proposer(myMember.get(), myEnsemble, controlClient.materializer());
            
            myRole.register(this);
            controlClient.materializer().register(this);
            subscribeLeaderWatch();
        }
        
        public ControlClientService controlClient() {
            return controlClient;
        }
        
        public EnsembleRole elect() throws InterruptedException, ExecutionException, KeeperException {
            Orchestra.Ensembles.Entity.Leader ensembleLeader = proposer.call();
            return myRoleFor(ensembleLeader);
        }

        @Override
        public void onSuccess(WatchEvent event) {
            if (leaderPath.equals(event.path())) {
                switch (event.type()) {
                case NodeCreated:
                case NodeDataChanged:
                    subscribeLeaderWatch();
                    controlClient().materializer().operator().getData(leaderPath, true).submit();
                    break;
                case NodeDeleted:
                    subscribeLeaderWatch();
                    controlClient().materializer().operator().exists(leaderPath, true).submit();
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
        public void handleViewUpdate(ZNodeResponseCache.ViewUpdate event) {
            if (leaderPath.equals(event.path())) {
                Materializer.MaterializedNode node = controlClient().materializer().get(leaderPath);
                Orchestra.Ensembles.Entity.Leader value = (node != null) ? (Orchestra.Ensembles.Entity.Leader) node.get().get() : null;
                setLeader(StampedReference.<Orchestra.Ensembles.Entity.Leader>of(event.updatedValue().stamp(), value));
            }
        }

        @Subscribe
        public void handleNodeUpdate(ZNodeResponseCache.NodeUpdate event) {
            if (leaderPath.equals(event.path().get())) {
                if (ZNodeResponseCache.NodeUpdate.UpdateType.NODE_REMOVED == event.type()) {
                    setLeader(StampedReference.<Orchestra.Ensembles.Entity.Leader>of(event.path().stamp(), null));
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
        
        protected EnsembleRole myRoleFor(Orchestra.Ensembles.Entity.Leader leader) {
            if (leader == null) {
                return EnsembleRole.LOOKING;
            } else if (myMember.get().equals(leader.get())) {
                return EnsembleRole.LEADING;
            } else {
                return EnsembleRole.FOLLOWING;
            }
        }

        protected void setLeader(StampedReference<Orchestra.Ensembles.Entity.Leader> newLeader) {
            StampedReference<Orchestra.Ensembles.Entity.Leader> prevLeader = leader.setIfGreater(newLeader);
            if (prevLeader.stamp() < newLeader.stamp()) {
                myRole.apply(myRoleFor(newLeader.get()));
            }
        }

        protected void subscribeLeaderWatch() {
            Futures.addCallback(controlClient().watches().subscribe(leaderPath, EnumSet.allOf(Watcher.Event.EventType.class)), this);
        }
    }
}
