package edu.uw.zookeeper.safari.data;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.common.SameThreadExecutor;


public class PathToQueryWatcher<I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> extends AbstractWatchListener {

    public static <I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> PathToQueryWatcher<I,O> newInstance(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher,
            PathToQuery<I,O> query) {
        PathToQueryWatcher<I,O> instance = new PathToQueryWatcher<I,O>(service, watch, matcher, query);
        service.addListener(instance, SameThreadExecutor.getInstance());
        if (service.isRunning()) {
            instance.starting();
        }
        return instance;
    }
    
    protected final PathToQuery<I,O> query;

    public PathToQueryWatcher(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher,
            PathToQuery<I,O> query) {
        super(service, watch, matcher);
        this.query = query;
    }
    
    public PathToQuery<I,O> query() {
        return query;
    }
    
    @Override
    public void handleWatchEvent(WatchEvent event) {
        if (service.isRunning()) {
            query.apply(event.getPath()).call();
        }
    }
}