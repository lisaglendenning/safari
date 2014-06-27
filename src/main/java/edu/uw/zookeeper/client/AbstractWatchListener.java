package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.ProtocolState;

public abstract class AbstractWatchListener extends Service.Listener implements
        WatchMatchListener {

    protected final Service service;
    protected final WatchListeners watch;
    protected final WatchMatcher matcher;

    protected AbstractWatchListener(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher) {
        this.service = service;
        this.watch = watch;
        this.matcher = matcher;
    }
    
    public Service getService() {
        return service;
    }
    
    public boolean isRunning() {
        return service.isRunning();
    }
    
    public Service.State state() {
        return service.state();
    }
    
    public WatchListeners getWatch() {
        return watch;
    }
    
    public void listen() {
        Services.listen(this, getService());
    }

    @Override
    public void handleAutomatonTransition(
            Automaton.Transition<ProtocolState> transition) {
        // TODO
    }
    
    @Override
    public WatchMatcher getWatchMatcher() {
        return matcher;
    }
    
    @Override
    public void starting() {
        getWatch().subscribe(this);
    }
    
    @Override
    public void stopping(Service.State from) {
        getWatch().unsubscribe(this);
    }
}