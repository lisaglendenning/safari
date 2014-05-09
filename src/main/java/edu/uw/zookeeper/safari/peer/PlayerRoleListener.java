package edu.uw.zookeeper.safari.peer;

import java.util.LinkedList;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.Listener;
import com.google.common.util.concurrent.Service.State;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.SameThreadExecutor;

public class PlayerRoleListener extends AbstractRoleListener implements Runnable {

    public static PlayerRoleListener create(
            ParameterizedFactory<RegionRole, ? extends Service> playerFactory,
            Automatons.EventfulAutomaton<RegionRole,?> role,
            Service service) {
        return new PlayerRoleListener(playerFactory, role, service);
    }
    
    private final ParameterizedFactory<RegionRole, ? extends Service> playerFactory;
    private final LinkedList<Automaton.Transition<RegionRole>> transitions;
    private Optional<? extends Service> player;
    
    protected PlayerRoleListener(
            ParameterizedFactory<RegionRole, ? extends Service> playerFactory,
            Automatons.EventfulAutomaton<RegionRole,?> role,
            Service service) {
        super(role, service);
        this.playerFactory = playerFactory;
        this.transitions = Lists.newLinkedList();
        this.player = Optional.absent();
        service.addListener(this, SameThreadExecutor.getInstance());
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
        
        if (player.isPresent() && player.get().isRunning()) {
            player.get().stopAsync();
        }
    }
    
    @Override
    public void run() {
        if (service.isRunning() && !transitions.isEmpty()) {
            Automaton.Transition<RegionRole> transition = transitions.peek();
            if (player.isPresent()) {
                Service previous = player.get();
                switch (previous.state()) {
                case STARTING:
                case RUNNING:
                    previous.addListener(
                            new Listener() {
                                @Override
                                public void terminated(State from) {
                                    run();
                                }
                            }, 
                            SameThreadExecutor.getInstance());
                    previous.stopAsync();
                    return;
                default:
                    break;
                }
            }
            transitions.removeFirst();
            player = Optional.fromNullable(playerFactory.get(transition.to()));
            if (player.isPresent()) {
                player.get().startAsync();
            }
        }
    }
}