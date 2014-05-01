package edu.uw.zookeeper.safari.peer;

import java.util.List;
import java.util.concurrent.Executor;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;

/**
 * Needs to be started before RegionRoleService;
 */
public class RegionMemberService extends AbstractIdleService implements Automatons.AutomatonListener<RegionRole> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}

        @Provides @Singleton
        public RegionRoleService getRegionRoleService(
                RegionConfiguration ensembleConfiguration,
                PeerConfiguration peerConfiguration,
                ControlMaterializerService control) {
            Identifier region = ensembleConfiguration.getRegion();
            Identifier peer = peerConfiguration.getView().id();
            RegionRoleService instance = RegionRoleService.newInstance(peer, region, control);
            return instance;
        }

        @Provides @Singleton
        public RegionMemberService getRegionMemberService(
                RegionRoleService role,
                ControlMaterializerService control) {
            RegionMemberService instance = RegionMemberService.newInstance(role, control);
            return instance;
        }
        
        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(RegionConfiguration.module());
        }
    }
    
    public static RegionMemberService newInstance(
            RegionRoleService role,
            ControlMaterializerService control) {
        return new RegionMemberService(role, control);
    }
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final ControlMaterializerService control;
    protected final RegionRoleService role;
    protected Service player;
    
    protected RegionMemberService(
            RegionRoleService role,
            ControlMaterializerService control) {
        this.control = control;
        this.role = role;
        this.player = null;
    }
    
    @Override
    public synchronized void handleAutomatonTransition(Automaton.Transition<RegionRole> transition) {
        if (isRunning()) {
            new RoleTransition(player, transition);
        }
    }

    @Override
    protected synchronized void startUp() throws Exception {
        // We assume here that the region already exists
        // Register myself as a region member
        ZNodePath path = ControlSchema.Safari.Regions.PATH.join(ZNodeLabel.fromString(role.getRegion().toString())).join(ControlSchema.Safari.Regions.Region.Members.LABEL);
        control.materializer().create(path).call();
        control.materializer().create(path.join(ZNodeLabel.fromString(role.getPeer().toString()))).call();
        
        role.getRole().subscribe(this);
    }

    @Override
    protected synchronized void shutDown() throws Exception {
        role.getRole().unsubscribe(this);
        
        if (player != null) {
            player.stopAsync();
        }
    }
    
    protected class RoleTransition extends Service.Listener {

        private final Automaton.Transition<RegionRole> transition;
        
        public RoleTransition(Service player, Automaton.Transition<RegionRole> transition) {
            this.transition = transition;
            
            if ((player != null) && player.isRunning()) {
                player.addListener(this, SAME_THREAD_EXECUTOR);
                player.stopAsync();
            } else {
                run();
            }
        }
        
        @Override
        public void terminated(State from) {
            run();
        }

        public void run() {
            synchronized (RegionMemberService.this) {
                if (RegionMemberService.this.isRunning()) {
                    switch (transition.to().getRole()) {
                    case LEADING:
                        player = RegionLeaderService.newInstance(role, control);
                        player.startAsync();
                        break;
                    case FOLLOWING:
                        break;
                    default:
                        break;
                    }
                }
            }
        }
    }
}
