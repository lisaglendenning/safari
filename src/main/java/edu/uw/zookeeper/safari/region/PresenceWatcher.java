package edu.uw.zookeeper.safari.region;

import java.util.EnumSet;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.ControlSchema;

public class PresenceWatcher extends PromiseTask<AbsoluteZNodePath, Identifier> implements WatchMatchListener, Runnable, Callable<Optional<Identifier>> {

    public static PresenceWatcher create(Identifier peer, ControlClientService control, Promise<Identifier> promise) {
        PresenceWatcher watcher = new PresenceWatcher(peer, control, promise);
        watcher.run();
        return watcher;
    }
    
    protected final ControlClientService control;
    protected final CallablePromiseTask<PresenceWatcher, Identifier> runnable;
    protected final WatchMatcher matcher;
    protected Optional<? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> request;
    
    protected PresenceWatcher(Identifier peer, ControlClientService control, Promise<Identifier> promise) {
        super(ControlSchema.Safari.Peers.Peer.Presence.pathOf(peer), promise);
        this.control = control;
        this.runnable = CallablePromiseTask.create(this, this);
        this.matcher = WatchMatcher.exact(task(), EnumSet.of(EventType.NodeDeleted));
        this.request = Optional.absent();
        
        addListener(this, SameThreadExecutor.getInstance());
    }
    
    public Identifier getPeer() {
        return Identifier.valueOf(((AbsoluteZNodePath) task().parent()).label().toString());
    }

    @Override
    public synchronized void run() {
        if (!isDone()) {
            runnable.run();
        } else {
            control.notifications().unsubscribe(this);
        }
    }
    
    @Override
    public synchronized Optional<Identifier> call() throws Exception {
        if (!request.isPresent()) {
            control.notifications().subscribe(this);
            control.materializer().submit(Operations.Requests.sync().setPath(task()).build());
            request = Optional.of(control.materializer().submit(Operations.Requests.exists().setPath(task()).setWatch(true).build()));
            request.get().addListener(this, SameThreadExecutor.getInstance());
        } else if (request.get().isDone()) {
            try {
                Optional<Operation.Error> error = Operations.maybeError(request.get().get().record(), KeeperException.Code.NONODE);
                if (error.isPresent()) {
                    return Optional.of(getPeer());
                }
            } catch (Exception e) {
                // TODO
                throw e;
            }
        }
        return Optional.absent();
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        switch (event.getEventType()) {
        case NodeDeleted:
        {
            set(getPeer());
            break;
        }
        default:
            throw new AssertionError(String.valueOf(event));
        }
    }

    @Override
    public WatchMatcher getWatchMatcher() {
        return matcher;
    }

    @Override
    public void handleAutomatonTransition(
            Automaton.Transition<ProtocolState> transition) {
        // TODO Auto-generated method stub
    }
}