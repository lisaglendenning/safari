package edu.uw.zookeeper.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.ProtocolState;

public class LoggingWatchMatchListener implements WatchMatchListener {

    public static LoggingWatchMatchListener create(
            WatchMatcher matcher) {
        return new LoggingWatchMatchListener(matcher);
    }

    public static LoggingWatchMatchListener create(
            WatchMatcher matcher,
            Logger logger) {
        return new LoggingWatchMatchListener(matcher, logger);
    }
    
    protected final Logger logger;
    protected final WatchMatcher matcher;

    protected LoggingWatchMatchListener(
            WatchMatcher matcher) {
        super();
        this.matcher = matcher;
        this.logger = LogManager.getLogger(this);
    }
    
    protected LoggingWatchMatchListener(
            WatchMatcher matcher,
            Logger logger) {
        this.matcher = matcher;
        this.logger = logger;
    }
    
    public Logger logger() {
        return logger;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        logger.debug("WATCH {} ({})", event, this);
    }

    @Override
    public void handleAutomatonTransition(
            Automaton.Transition<ProtocolState> transition) {
    }
    
    @Override
    public WatchMatcher getWatchMatcher() {
        return matcher;
    }
    
    @Override
    public String toString() {
        return toString(MoreObjects.toStringHelper(this)).toString();
    }
    
    protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
        return helper.addValue(matcher);
    }
}