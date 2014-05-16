package edu.uw.zookeeper.safari.region;

import java.util.LinkedList;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.Peer;

public class RegionPlayerService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}

        @Provides @Singleton
        public Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> getRegionRole(
                @Peer Identifier peer) {
            return Automatons.createSynchronizedEventful(
                    Automatons.createEventful(RegionRoleAutomaton.unknown(peer)));
        }
        
        @Provides @Singleton
        public ParameterizedFactory<RegionRole, Service> getPlayerFactory(
                final Provider<RegionLeaderService> leaderFactory) {
            return new ParameterizedFactory<RegionRole, Service>() {
                @Override
                public Service get(RegionRole value) {
                    switch (value.getRole()) {
                    case LEADING:
                        return leaderFactory.get();
                    default:
                        return null;
                    }
                }
            };
        }
        
        @Provides @Singleton
        public RegionPlayerService getPlayerRoleService(
                ParameterizedFactory<RegionRole, Service> playerFactory,
                Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
                ServiceMonitor monitor) {
            RegionPlayerService instance = RegionPlayerService.create(playerFactory, role);
            monitor.add(instance);
            return instance;
        }

        @Override
        protected void configure() {
        }
    }
    
    public static RegionPlayerService create(
            ParameterizedFactory<RegionRole, ? extends Service> playerFactory,
            Automatons.EventfulAutomaton<RegionRole,?> role) {
        return new RegionPlayerService(playerFactory, role);
    }
    
    private final Logger logger;
    private final RoleListener delegate;
    
    protected RegionPlayerService(
            ParameterizedFactory<RegionRole, ? extends Service> playerFactory,
            Automatons.EventfulAutomaton<RegionRole,?> role) {
        this.logger = LogManager.getLogger(this);
        this.delegate = new RoleListener(playerFactory, role);
        addListener(delegate, SameThreadExecutor.getInstance());
    }
    
    public Service get() {
        return delegate.get();
    }
    
    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }
    
    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
    }

    protected class RoleListener extends AbstractRoleListener implements Runnable {
        
        private final ParameterizedFactory<RegionRole, ? extends Service> playerFactory;
        private final LinkedList<Automaton.Transition<RegionRole>> transitions;
        private Optional<? extends Service> player;
        
        protected RoleListener(
                ParameterizedFactory<RegionRole, ? extends Service> playerFactory,
                Automatons.EventfulAutomaton<RegionRole,?> role) {
            super(role, RegionPlayerService.this);
            this.playerFactory = playerFactory;
            this.transitions = Lists.newLinkedList();
            this.player = Optional.absent();
        }
        
        public synchronized Service get() {
            return player.get();
        }
        
        @Override
        public void handleAutomatonTransition(
            Automaton.Transition<RegionRole> transition) {
            if (service.isRunning()) {
                transitions.add(transition);
                run();
            }
        }
    
        @Override
        public void stopping(State from) {
            super.stopping(from);

            Optional<? extends Service> player;
            synchronized (this) {
                player = this.player;
                if (player.isPresent()) {
                    switch (player.get().state()) {
                    case NEW:
                    case STARTING:
                    case RUNNING:
                        player.get().stopAsync();
                    default:
                        break;
                    }
                }
            }
            
            if (player.isPresent()) {
                player.get().awaitTerminated();
            }
        }
        
        @Override
        public void run() {
            Optional<? extends Service> previous = Optional.absent();
            synchronized (this) {
                if (service.isRunning() && !transitions.isEmpty()) {
                    Automaton.Transition<RegionRole> transition = transitions.peek();
                    logger.info("{}", transition);
                    if (player.isPresent()) {
                        switch (player.get().state()) {
                        case TERMINATED:
                        case FAILED:
                            player = Optional.absent();
                            break;
                        default:
                            previous = player;
                            break;
                        }
                    }
                    if (!previous.isPresent()) {
                        transitions.removeFirst();
                        player = Optional.fromNullable(playerFactory.get(transition.to()));
                        if (player.isPresent()) {
                            player.get().startAsync();
                        }
                    }
                }
            }
            if (previous.isPresent()) {
                previous.get().addListener(
                        new Service.Listener() {
                            @Override
                            public void terminated(State from) {
                                run();
                            }
                            
                            @Override
                            public void failed(State from, Throwable t) {
                                run();
                            }
                        }, 
                        SameThreadExecutor.getInstance());
                previous.get().stopAsync();
                switch (previous.get().state()) {
                case TERMINATED:
                case FAILED:
                    run();
                default:
                    break;
                }
            }
        }
    }
}