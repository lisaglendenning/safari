package edu.uw.zookeeper.safari.peer;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;

import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.SameThreadExecutor;

public abstract class AbstractRoleListener extends Service.Listener implements Automatons.AutomatonListener<RegionRole> {

    protected final Service service;
    protected final Automatons.EventfulAutomaton<RegionRole,?> role;
    
    protected AbstractRoleListener(
            Automatons.EventfulAutomaton<RegionRole,?> role,
            Service service) {
        this.role = role;
        this.service = service;
        service.addListener(this, SameThreadExecutor.getInstance());
    }

    @Override
    public void starting() {
        role.subscribe(this);
    }

    @Override
    public void stopping(State from) {
        role.unsubscribe(this);
    }
}