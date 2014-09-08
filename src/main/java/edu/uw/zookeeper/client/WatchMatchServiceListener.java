package edu.uw.zookeeper.client;

import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.ProtocolState;

public class WatchMatchServiceListener extends LoggingServiceListener<WatchMatchServiceListener> implements
        WatchMatchListener {

    public static WatchMatchServiceListener create(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher) {
        return new WatchMatchServiceListener(service, watch, matcher);
    }

    public static WatchMatchServiceListener create(
            Service service,
            WatchListeners watch,
            WatchMatchListener watcher) {
        return new WatchMatchServiceListener(service, watch, watcher);
    }
    
    protected final Service service;
    protected final WatchListeners watch;
    protected final WatchMatchListener watcher;

    protected WatchMatchServiceListener(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher) {
        super();
        this.service = service;
        this.watch = watch;
        this.watcher = LoggingWatchMatchListener.create(matcher, logger);
    }
    
    protected WatchMatchServiceListener(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher,
            Logger logger) {
        this(service, watch, LoggingWatchMatchListener.create(matcher, logger), logger);
    }

    protected WatchMatchServiceListener(
            Service service,
            WatchListeners watch,
            WatchMatchListener watcher) {
        super();
        this.service = service;
        this.watch = watch;
        this.watcher = watcher;
    }
    
    protected WatchMatchServiceListener(
            Service service,
            WatchListeners watch,
            WatchMatchListener watcher,
            Logger logger) {
        super(logger);
        this.service = service;
        this.watch = watch;
        this.watcher = watcher;
    }
    
    public boolean isRunning() {
        return service.isRunning();
    }
    
    public Service.State state() {
        return service.state();
    }
    
    public Service getService() {
        return service;
    }

    public WatchListeners getWatch() {
        return watch;
    }
    
    public WatchMatchListener getWatcher() {
        return watcher;
    }

    public Logger logger() {
        return logger;
    }
    
    public void listen() {
        Services.listen(this, getService());
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        if (isRunning()) {
            watcher.handleWatchEvent(event);
        }
    }

    @Override
    public void handleAutomatonTransition(
            Automaton.Transition<ProtocolState> transition) {
        if (isRunning()) {
            watcher.handleAutomatonTransition(transition);
        }
    }
    
    @Override
    public WatchMatcher getWatchMatcher() {
        return watcher.getWatchMatcher();
    }
    
    @Override
    public void starting() {
        super.starting();
        getWatch().subscribe(this);
    }

    @Override
    public void stopping(Service.State from) {
        super.stopping(from);
        getWatch().unsubscribe(this);
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        getWatch().unsubscribe(this);
    }
    
    @Override
    public String toString() {
        return toString(MoreObjects.toStringHelper(this)).toString();
    }
    
    protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
        return helper.addValue(watcher).addValue(service);
    }
}
