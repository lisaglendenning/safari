package edu.uw.zookeeper.safari.storage.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode.SessionZNode.SessionIdHex;

/**
 * Commits local session snapshots when all dependants are committed.
 * 
 * Assumes that sessions are watched.
 */
public final class CommitSessionSnapshot<O extends Operation.ProtocolResponse<?>> extends Watchers.SimpleForwardingCallback<ZNodePath, FutureCallback<? super Optional<Operation.Error>>> {
    
    public static <O extends Operation.ProtocolResponse<?>> List<Watchers.CacheNodeCreatedListener<StorageZNode<?>>> create(
            AbsoluteZNodePath path,
            Predicate<Long> isLocal,
            Materializer<StorageZNode<?>,O> materializer,
            Service service,
            WatchListeners cacheEvents,
            Logger logger) {
        final FutureCallback<Object> callback = Watchers.StopServiceOnFailure.create(service);
        final SetMultimap<Long, ZNodeLabel> committed = HashMultimap.create();
        final CommitSessionSnapshot<O> instance = new CommitSessionSnapshot<O>(((AbsoluteZNodePath) ((AbsoluteZNodePath) path.parent()).parent()).parent(), committed, isLocal, materializer, callback, logger);
        final ImmutableList<? extends Pair<? extends FutureCallback<? super ZNodePath>, WatchMatcher>> watchers = ImmutableList.of(
                Pair.create(
                        instance,
                        WatchMatcher.exact(
                                path, 
                                Watcher.Event.EventType.NodeCreated, 
                                Watcher.Event.EventType.NodeDeleted, 
                                Watcher.Event.EventType.NodeChildrenChanged)), 
                Pair.create(
                        instance.new SessionCommitListener(),
                        WatchMatcher.exact(
                                ((AbsoluteZNodePath) path.parent()).parent().join(StorageZNode.CommitZNode.LABEL), 
                                Watcher.Event.EventType.NodeCreated)), 
                Pair.create(
                        instance.new SessionChangeListener(),
                        WatchMatcher.exact(
                                StorageSchema.Safari.Sessions.Session.PATH, 
                                Watcher.Event.EventType.NodeCreated, 
                                Watcher.Event.EventType.NodeDeleted, 
                                Watcher.Event.EventType.NodeDataChanged)));
        ImmutableList.Builder<Watchers.CacheNodeCreatedListener<StorageZNode<?>>> listeners = ImmutableList.builder();
        for (Pair<? extends FutureCallback<? super ZNodePath>, WatchMatcher> watcher: watchers) {
            listeners.add(
                    Watchers.CacheNodeCreatedListener.create(
                        materializer.cache(), 
                        service, 
                        cacheEvents, 
                        Watchers.FutureCallbackListener.create(
                                Watchers.EventToPathCallback.create(
                                        watcher.first()), 
                                watcher.second(), 
                                logger), 
                        logger));
        }
        return listeners.build();
    }

    private final Watchers.MaybeErrorProcessor processor;
    private final ZNodePath sessions;
    private final NameTrie<StorageZNode<?>> cache;
    private final Predicate<Long> isLocal;
    private final SetMultimap<Long, ZNodeLabel> committed;
    private final PathToQuery<?,O> commit;
    private final Logger logger;
    
    @SuppressWarnings("unchecked")
    protected CommitSessionSnapshot(
            ZNodePath sessions,
            SetMultimap<Long, ZNodeLabel> committed,
            Predicate<Long> isLocal,
            Materializer<StorageZNode<?>,O> materializer,
            FutureCallback<? super Optional<Operation.Error>> delegate,
            Logger logger) {
        super(delegate);
        this.logger = logger;
        this.processor = Watchers.MaybeErrorProcessor.maybeError(KeeperException.Code.NONODE, KeeperException.Code.NODEEXISTS);
        this.isLocal = isLocal;
        this.sessions = sessions;
        this.committed = committed;
        this.cache = materializer.cache().cache();
        try {
            this.commit = PathToQuery.forFunction(
                    materializer, 
                    Functions.compose(
                            PathToRequests.forRequests(Operations.Requests.create().setData(materializer.codec().toBytes(Boolean.TRUE))),
                            JoinToPath.forName(StorageZNode.CommitZNode.LABEL)));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Assumes cache is read locked.
     * Not thread-safe.
     */
    @Override
    public void onSuccess(ZNodePath result) {
        StorageZNode<?> node = cache.get(result);
        StorageZNode<?> values = (node != null) ? node.parent().get() : cache.get(((AbsoluteZNodePath) result).parent());
        StorageZNode<?> session = (values != null) ? values.parent().get() : cache.get(((AbsoluteZNodePath) ((AbsoluteZNodePath) result).parent()).parent());
        Long id = Long.valueOf(((session != null) ?
                    ((StorageZNode.SessionZNode<?>) session).name() :
                    SessionIdHex.valueOf(((AbsoluteZNodePath) ((AbsoluteZNodePath) result).parent()).parent().label().toString())).longValue());
        if (node != null) {
            if (!committed.containsKey(id) && (session.containsKey(StorageZNode.CommitZNode.LABEL) || !isLocal.apply(id))) {
                return;
            }
            if (node.containsKey(StorageZNode.CommitZNode.LABEL)) {
                final ZNodeLabel v = (ZNodeLabel) node.parent().name();
                if (!committed.put(id, v)) {
                    return;
                }
                logger.debug("snapshot {} committed for session {} in {}", v, Session.toString(id.longValue()), sessions);
            }
        } else {
            if (values == null) {
                committed.removeAll(id);
                return;
            }
        }
        Set<ZNodeLabel> committed = this.committed.get(id);
        if ((committed != null) && (values.size() == committed.size())) {
            assert (committed.equals(values.keySet()));
            if (!session.containsKey(StorageZNode.CommitZNode.LABEL)) {
                logger.debug("committing session snapshot {} in {}", Session.toString(id.longValue()), sessions);
                Watchers.Query.call(
                        processor,
                        delegate(),
                        commit.apply(session.path()));
            }
        }
    }
    
    @Override
    protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper toString) {
        return super.toString(toString.addValue(sessions));
    }
    
    protected final class SessionCommitListener extends Watchers.ForwardingCallback<ZNodePath, CommitSessionSnapshot<O>> {
        
        protected SessionCommitListener() {}

        /**
         * Assumes cache is read locked.
         * Not thread-safe.
         */
        @Override
        public void onSuccess(ZNodePath result) {
            StorageZNode<?> node = cache.get(result);
            if (node != null) {
                Long id = Long.valueOf(((StorageZNode.SessionZNode<?>) node.parent().get()).name().longValue());
                if (!committed.removeAll(id).isEmpty()) {
                    logger.debug("session snapshot {} committed in {}", Session.toString(id.longValue()), sessions);
                }
            }
        }

        @Override
        protected CommitSessionSnapshot<O> delegate() {
            return CommitSessionSnapshot.this;
        }
    }
    
    protected final class SessionChangeListener extends Watchers.ForwardingCallback<ZNodePath, CommitSessionSnapshot<O>> {
        
        private final PathToQuery<?,O> getValues;
        private final PathToQuery<?,O> getCommitted;
        
        @SuppressWarnings("unchecked")
        protected SessionChangeListener() {
            this.getValues = PathToQuery.forRequests(
                    commit.client(), 
                    Operations.Requests.sync(), 
                    Operations.Requests.getChildren());
            this.getCommitted = PathToQuery.forFunction(
                    commit.client(), 
                    Functions.compose(
                            PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.getData()),
                            JoinToPath.forName(StorageZNode.CommitZNode.LABEL)));
        }
        
        /**
         * Assumes cache is read locked.
         * Not thread-safe.
         */
        @Override
        public void onSuccess(ZNodePath result) {
            StorageZNode<?> node = cache.get(result);
            Long id = Long.valueOf(((node != null) ? 
                    ((StorageSchema.Safari.Sessions.Session) node).name() :
                    SessionIdHex.valueOf(result.label().toString())).longValue());
            if (node != null) {
                if (isLocal.apply(id)) {
                    if (!committed.containsKey(id)) {
                        result = sessions.join(node.parent().name());
                        node = cache.get(result);
                        if ((node != null) && !node.containsKey(StorageZNode.CommitZNode.LABEL)) {
                            // replay any uncommitted dependents
                            node = node.get(StorageZNode.ValuesZNode.LABEL);
                            if (node != null) {
                                result = node.path();
                                for (StorageZNode<?> value: node.values()) {
                                    if (value.containsKey(StorageZNode.CommitZNode.LABEL)) {
                                        delegate().onSuccess(value.path());
                                    } else {
                                        Watchers.Query.call(
                                                processor,
                                                delegate().delegate(),
                                                getCommitted.apply(value.path()));
                                    }
                                }
                            } else {
                                result = result.join(StorageZNode.ValuesZNode.LABEL);
                            }
                            Watchers.Query.call(
                                    processor,
                                    delegate().delegate(),
                                    getValues.apply(result));
                        }
                    }
                } else {
                    committed.removeAll(id);
                }
            } else {
                committed.removeAll(id);
            }
        }

        @Override
        protected CommitSessionSnapshot<O> delegate() {
            return CommitSessionSnapshot.this;
        }
    }
}
