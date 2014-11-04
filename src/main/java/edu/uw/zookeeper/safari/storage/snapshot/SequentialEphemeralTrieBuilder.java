package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.ListenableFutureActor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.AbstractNameTrie;
import edu.uw.zookeeper.data.DefaultsNode;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.Request;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class SequentialEphemeralTrieBuilder<O extends Operation.ProtocolResponse<?>> extends ListenableFutureActor<ZNodePath, List<O>, SubmittedRequests<? extends Records.Request, O>> implements Callable<ListenableFuture<Pair<SimpleLabelTrie<SequentialEphemeralTrieBuilder.SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>>> {

    public static <O extends Operation.ProtocolResponse<?>> Callable<ListenableFuture<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>>> create(
            ZNodePath snapshot,
            Materializer<StorageZNode<?>,O> materializer,
            Logger logger) {
        return new SequentialEphemeralTrieBuilder<O>(
                snapshot, 
                materializer,
                SettableFuturePromise.<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>>create(), 
                logger);
    }

    private final ZNodePath snapshot;
    private final SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> trie;
    private final ImmutableSet.Builder<AbsoluteZNodePath> leaves;
    private final Materializer<StorageZNode<?>,O> materializer;
    private final PathToRequests query;
    private final Promise<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>> promise;
    private boolean isReady;
    
    @SuppressWarnings("unchecked")
    protected SequentialEphemeralTrieBuilder(
            ZNodePath snapshot,
            Materializer<StorageZNode<?>,O> materializer,
            Promise<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>> promise,
            Logger logger) {
        super(Queues.<SubmittedRequests<? extends Request, O>>newConcurrentLinkedQueue(), logger);
        this.materializer = materializer;
        this.snapshot = snapshot;
        this.query = PathToRequests.forRequests(
                Operations.Requests.sync(), 
                Operations.Requests.getChildren());
        this.trie = SimpleLabelTrie.forRoot(SequentialNode.<AbsoluteZNodePath>sequentialRoot());
        this.leaves = ImmutableSet.builder();
        this.promise = promise;
        this.isReady = false;
        
        promise.addListener(this, MoreExecutors.directExecutor());
    }

    @Override
    public ListenableFuture<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>> call() {
        materializer.cache().lock().readLock().lock();
        try {
            synchronized (this) {
                if (!isReady) {
                    StorageZNode<?> node = materializer.cache().cache().get(snapshot);
                    for (ZNodeLabel label: ImmutableList.of(
                            StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL,
                            StorageZNode.SessionsZNode.LABEL)) {
                        if (node != null) {
                            node = node.get(label);
                        }
                    }
                    if (node != null) {
                        for (StorageZNode<?> session: node.values()) {
                            submit(session.path().join(StorageZNode.ValuesZNode.LABEL));
                        }
                    }
                    isReady = true;
                    run();
                }
            }
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
        return promise;
    }

    @Override
    public synchronized boolean isReady() {
        return isReady && super.isReady();
    }
    
    @Override
    public ListenableFuture<List<O>> submit(ZNodePath request) {
        SubmittedRequests<? extends Records.Request, O> task = SubmittedRequests.submit(
                materializer, 
                query.apply(request));
        if (! send(task)) {
            task.cancel(false);
            throw new RejectedExecutionException();
        }
        return task;
    }
    
    @Override
    public synchronized void run() {
        super.run();
        if (promise.isDone()) {
            stop();
        } else if (isReady && mailbox.isEmpty()) {
            promise.set(Pair.create(trie, leaves.build()));
        }
    }

    @Override
    protected void doRun() throws Exception {
        try {
            super.doRun();
        } catch (Exception e) {
            promise.setException(e);
        }
    }
    
    @Override
    protected boolean doApply(SubmittedRequests<? extends Request, O> input)
            throws Exception {
        final int last = input.getValue().size()-1;
        final Records.Response response = input.get().get(last).record();
        if (!Operations.maybeError(response, KeeperException.Code.NONODE).isPresent()) {
            final ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.getValue().get(last)).getPath());
            materializer.cache().lock().readLock().lock();
            try {
                final StorageZNode<?> values = materializer.cache().cache().get(path);
                assert (values.size() == ((Records.ChildrenGetter) response).getChildren().size());
                for (StorageZNode<?> child: values.values()) {
                    final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral ephemeral = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral) child;
                    final int index = ephemeral.name().lastIndexOf(ZNodeName.SLASH);
                    assert ((index < 0) || ((index < ephemeral.name().length() - 1) && (index > 0)));
                    final String label = (index < 0) ? ephemeral.name() : ephemeral.name().substring(index+1);
                    final Optional<? extends Sequential<String, ?>> sequential = Sequential.maybeFromString(label);
                    if (sequential.isPresent()) {
                        SequentialNode<AbsoluteZNodePath> node = SequentialNode.putIfAbsent(trie, (AbsoluteZNodePath) ZNodePath.root().join(ZNodeName.fromString(ephemeral.name())), (AbsoluteZNodePath) ephemeral.path());
                        leaves.add((AbsoluteZNodePath) node.path());
                    }
                }
            } finally {
                materializer.cache().lock().readLock().unlock();
            }
        }
        return true;
    }

    public static final class SequentialNode<V> extends DefaultsNode.AbstractDefaultsNode<SequentialNode<V>> {

        public static <V> SequentialNode<V> putIfAbsent(NameTrie<SequentialNode<V>> trie, AbsoluteZNodePath path, V value) {
            return putIfAbsent(trie, path.parent()).putIfAbsent(path.label(), value);
        }
        
        public static <V> SequentialNode<V> sequentialRoot() {
            return new SequentialNode<V>(
                    AbstractNameTrie.<SequentialNode<V>>rootPointer());
        }
        
        public static <V> SequentialNode<V> sequentialChild(ZNodeName label, SequentialNode<V> parent) {
            return new SequentialNode<V>(
                    AbstractNameTrie.<SequentialNode<V>>weakPointer(label, parent));
        }
        
        public static <V> SequentialNode<V> sequentialChild(ZNodeName label, SequentialNode<V> parent, V value) {
            return new SequentialNode<V>(
                    value,
                    AbstractNameTrie.<SequentialNode<V>>weakPointer(label, parent));
        }
        
        public static Comparator<ZNodeName> comparator() {
            return COMPARATOR;
        }

        protected static final Comparator<ZNodeName> COMPARATOR = new Comparator<ZNodeName>() {
            private final Comparator<String> delegate = Sequential.comparator();
            @Override
            public int compare(final ZNodeName a, final ZNodeName b) {
                return delegate.compare(a.toString(), b.toString());
            }
        };

        private final Optional<? extends Sequential<String,?>> sequential;
        private final Optional<V> value;

        protected SequentialNode(
                NameTrie.Pointer<? extends SequentialNode<V>> parent) {
            this(Optional.<Sequential<String,?>>absent(), Optional.<V>absent(), parent);
        }

        protected SequentialNode(
                V value,
                NameTrie.Pointer<? extends SequentialNode<V>> parent) {
            this(Optional.of(Sequential.fromString(parent.name().toString())), Optional.of(value), parent);
        }
        
        private SequentialNode(
                Optional<? extends Sequential<String,?>> sequential,
                Optional<V> value,
                NameTrie.Pointer<? extends SequentialNode<V>> parent) {
            super(parent, Maps.<ZNodeName, ZNodeName, SequentialNode<V>>newTreeMap(COMPARATOR));
            this.sequential = sequential;
            this.value = value;
        }
        
        public Sequential<String,?> getSequential() {
            return sequential.get();
        }
        
        public V getValue() {
            return value.get();
        }
        
        public SequentialNode<V> putIfAbsent(ZNodeName label, V value) {
            SequentialNode<V> child = get(label);
            if (child == null) {
                child = sequentialChild(label, this, value);
                put(label, child);
            }
            return child;
        }

        @Override
        protected SequentialNode<V> newChild(ZNodeName label) {
            return sequentialChild(label, this);
        }
    }
}
