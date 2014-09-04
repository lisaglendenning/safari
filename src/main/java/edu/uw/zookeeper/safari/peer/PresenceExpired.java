package edu.uw.zookeeper.safari.peer;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;

public final class PresenceExpired extends LoggingWatchMatchListener implements Runnable {

    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Identifier> create(
            final Identifier peer, 
            final ClientExecutor<? super Records.Request, O, ?> client,
            final WatchListeners notifications,
            final Logger logger) {
        final Promise<Identifier> promise = SettableFuturePromise.create();
        final ZNodePath path = ControlSchema.Safari.Peers.Peer.Presence.pathOf(peer);
        final Runnable set = new Runnable() {
            @Override
            public void run() {
                if (!promise.isDone()) {
                    promise.set(peer);
                }
            }
        };
        @SuppressWarnings("unchecked")
        final Watchers.FixedQueryRunnable<O,?> query = Watchers.FixedQueryRunnable.create(
                FixedQuery.forIterable(
                        client, 
                        PathToRequests.forRequests(
                                Operations.Requests.sync(), 
                                Operations.Requests.exists().setWatch(true)).apply(path)), 
                        Watchers.MaybeErrorProcessor.maybeNoNode(),
                        new Watchers.SetExceptionCallback<Optional<Operation.Error>, Identifier>(promise) {
                            @Override
                            public void onSuccess(Optional<Operation.Error> result) {
                                if (result.isPresent()) {
                                    set.run();
                                }
                            }
                        });
        PresenceExpired listener = new PresenceExpired(
                set, 
                query, 
                WatchMatcher.exact(
                        path, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDeleted), 
                logger);
        Watchers.UnsubscribeWhenDone.subscribe(promise, listener, notifications);
        listener.run();
        return promise;
    }
    
    private final Runnable set;
    private final Runnable query;
    
    private PresenceExpired(
            Runnable set,
            Runnable query,
            WatchMatcher match,
            Logger logger) {
        super(match, logger);
        this.set = set;
        this.query = query;
    }

    @Override
    public void run() {
        query.run();
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        super.handleWatchEvent(event);
        if (event.getEventType() == Watcher.Event.EventType.NodeDeleted) {
            set.run();
        }
        run();
    }
}