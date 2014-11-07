package edu.uw.zookeeper.safari.storage.snapshot;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.Watcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ForwardingQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.SimpleNameTrie;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.protocol.FourLetterWords;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;
import edu.uw.zookeeper.safari.schema.PrefixCreator;
import edu.uw.zookeeper.safari.storage.schema.EscapedConverter;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode.SessionZNode.SessionIdHex;

@RunWith(JUnit4.class)
public class SnapshotWatchesTest extends AbstractMainTest {

    @ZNode(acl=Acls.Definition.ANYONE_ALL)
    public static final class SnapshotWatchesTestSchema extends StorageZNode<Void> {

        public SnapshotWatchesTestSchema(
                ValueNode<ZNodeSchema> schema,
                ByteCodec<Object> codec) {
            super(schema, codec, SimpleNameTrie.<StorageZNode<?>>rootPointer());
        }

        @ZNode
        public static class Watches extends StorageZNode<Void> {

            @Name
            public static final ZNodeLabel LABEL = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.LABEL;

            public Watches(ValueNode<ZNodeSchema> schema,
                    ByteCodec<Object> codec,
                    Pointer<StorageZNode<?>> parent) {
                super(schema, codec, parent);
            }

            @ZNode
            public static class Session extends StorageZNode.SessionZNode<Void> {

                public Session(ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<StorageZNode<?>> parent) {
                    super(schema, codec, parent);
                }

                @ZNode                
                public static class Values extends StorageZNode.ValuesZNode {

                    public Values(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<StorageZNode<?>> parent) {
                        super(schema, codec, parent);
                    }

                    @ZNode(dataType=Watcher.WatcherType.class)
                    public static class Watch extends StorageZNode.EscapedNamedZNode<Watcher.WatcherType> {
    
                        public Watch(ValueNode<ZNodeSchema> schema,
                                ByteCodec<Object> codec,
                                Pointer<StorageZNode<?>> parent) {
                            super(schema, codec, parent);
                        }
    
                        public Watch(
                                String name,
                                ValueNode<ZNodeSchema> schema,
                                ByteCodec<Object> codec,
                                Pointer<StorageZNode<?>> parent) {
                            super(name, schema, codec, parent);
                        }
                        
                        @ZNode(dataType=SessionIdHex.class)
                        public static class Ephemeral extends StorageZNode<SessionIdHex> {

                            @Name
                            public static final ZNodeLabel LABEL = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch.Ephemeral.LABEL;
                            
                            public Ephemeral(ValueNode<ZNodeSchema> schema,
                                    ByteCodec<Object> codec,
                                    Pointer<StorageZNode<?>> parent) {
                                super(schema, codec, parent);
                            }
                        }
                    }
                }
            }
        }
    }
    
    public static final class QueueSupplier<E> extends ForwardingQueue<E> implements Supplier<E> {

        public static <E> QueueSupplier<E> create() {
            return create(Queues.<E>newConcurrentLinkedQueue());
        }
        
        public static <E> QueueSupplier<E> create(
                Queue<E> delegate) {
            return new QueueSupplier<E>(delegate);
        }
        
        private final Queue<E> delegate;
        
        protected QueueSupplier(
                Queue<E> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public E get() {
            return remove();
        }

        @Override
        protected Queue<E> delegate() {
            return delegate;
        }
    }
    
    public static final class PromiseMap<T,V> extends ForwardingMap<T, PromiseTask<T,V>> implements AsyncFunction<T,V> {

        public static <T,V> PromiseMap<T,V> create() {
            return create(new MapMaker().<T, PromiseTask<T,V>>makeMap());
        }
        
        public static <T,V> PromiseMap<T,V> create(
                ConcurrentMap<T, PromiseTask<T,V>> delegate) {
            return new PromiseMap<T,V>(delegate);
        }
        
        private final ConcurrentMap<T, PromiseTask<T,V>> delegate;
        
        protected PromiseMap(
                ConcurrentMap<T, PromiseTask<T,V>> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public ListenableFuture<V> apply(T input) {
            PromiseTask<T,V> task = delegate().get(input);
            if (task == null) {
                task = PromiseTask.of(input, SettableFuturePromise.<V>create());
                if (delegate().putIfAbsent(input, task) != null) {
                    return apply(input);
                }
            }
            return task;
        }

        @Override
        protected ConcurrentMap<T, PromiseTask<T,V>> delegate() {
            return delegate;
        }
    }

    @Test(timeout=16000)
    public void test() throws Exception {
        final List<Component<?>> components = Modules.newServerAndClientComponents();
        final Injector injector = stopping(components, JacksonModule.create());
        final TimeValue timeOut = TimeValue.seconds(16L);
        final int nservers = 2;
        final int nsessions = 2;
        final int depth = 3;
        final ZNodePath prefix = ZNodePath.root();
        final Function<ZNodePath,ZNodeLabel> labelOf = new Function<ZNodePath,ZNodeLabel>() {
            @Override
            public ZNodeLabel apply(ZNodePath input) {
                return ZNodeLabel.fromString(EscapedConverter.getInstance().convert(input.suffix(prefix).toString()));
            }
        };
        final Function<Long, Long> toFrontend = new Function<Long, Long>() {
            @Override
            public Long apply(Long input) {
                return Long.valueOf(input.longValue() << 8);
            }
        };
        final List<Object> inputs = Lists.newArrayListWithCapacity(SnapshotWatches.Phase.values().length);
        final ImmutableMap.Builder<ZNodePath, Long> ephemerals = ImmutableMap.builder();
        Long session = Long.valueOf(1L);
        for (SnapshotWatches.Phase phase: SnapshotWatches.Phase.values()) {
            switch (phase) {
            case POST_PHASE:
            case PRE_PHASE:
                List<FourLetterWords.Wchc> wchcs = Lists.newArrayListWithCapacity(nservers);
                for (int i=0; i<nservers; ++i) {
                    ImmutableSetMultimap.Builder<Long,ZNodePath> builder = ImmutableSetMultimap.builder();
                    for (int j=0; j<nsessions; ++j) {
                        ZNodePath path = ZNodePath.root();
                        int d = 0;
                        do {
                            builder.put(session, path);
                            path = path.join(ZNodeLabel.fromString(session.toString()));
                            ++d;
                        } while (d < depth);
                        if (phase == SnapshotWatches.Phase.PRE_PHASE) {
                            // make some leaf paths ephemeral for the watcher
                            ephemerals.put(((AbsoluteZNodePath) path).parent(), toFrontend.apply(session));
                        }
                        session = Long.valueOf(session.longValue() + 1L);
                    }
                    wchcs.add(FourLetterWords.Wchc.fromMultimap(builder.build()));
                }
                inputs.add(wchcs);
                break;
            case SNAPSHOT:
                inputs.add(Pair.create(Long.valueOf(1L), ephemerals.build()));
                break;
            default:
                fail(String.valueOf(phase));
            }
        }
        
        final Callable<Void> callable = new Callable<Void>() {
            @SuppressWarnings("unchecked")
            @Override
            public Void call() throws Exception {
                final Serializers.ByteCodec<Object> codec = injector.getInstance(Key.get(new TypeLiteral<Serializers.ByteCodec<Object>>(){}));
                final Materializer<StorageZNode<?>,Message.ServerResponse<?>> client = Materializer.<StorageZNode<?>, Message.ServerResponse<?>>fromHierarchy(SnapshotWatchesTestSchema.class, codec, injector.getInstance(Key.get(Component.class, Names.named("client"))).injector().getInstance(ConnectionClientExecutorService.Builder.class).getConnectionClientExecutor());
                PrefixCreator.call(client).get();
                
                final List<QueueSupplier<Promise<FourLetterWords.Wchc>>> servers = Lists.newArrayListWithCapacity(nservers);
                for (int i=0; i<nservers; ++i) {
                    QueueSupplier<Promise<FourLetterWords.Wchc>> server = QueueSupplier.<Promise<FourLetterWords.Wchc>>create();
                    for (int phase=0; phase<2; ++phase) {
                        server.add(SettableFuturePromise.<FourLetterWords.Wchc>create());
                    }
                    servers.add(server);
                }
                
                final PromiseMap<Long,Long> sessions = PromiseMap.create();

                final ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> snapshot = SnapshotWatches.create(
                        client.schema().apply(SnapshotWatchesTestSchema.Watches.class).path(), 
                        labelOf, 
                        codec, 
                        client,
                        new Supplier<ListenableFuture<? extends AsyncFunction<Long,Long>>>() {
                            @Override
                            public ListenableFuture<? extends AsyncFunction<Long, Long>> get() {
                                return Futures.immediateFuture(sessions);
                            }
                        },
                        servers);

                for (SnapshotWatches.Phase phase: SnapshotWatches.Phase.values()) {
                    if (phase == SnapshotWatches.Phase.SNAPSHOT) {
                        ((ChainedFutures<ListenableFuture<?>,?,?>) snapshot.chain()).add(Futures.immediateFuture(inputs.get(phase.ordinal())));
                        continue;
                    }
                    final List<FourLetterWords.Wchc> wchcs = (List<FourLetterWords.Wchc>) inputs.get(phase.ordinal());
                    
                    List<Promise<FourLetterWords.Wchc>> futures = Lists.newArrayListWithCapacity(servers.size());
                    for (QueueSupplier<Promise<FourLetterWords.Wchc>> server: servers) {
                        futures.add(server.peek());
                    }
                    
                    assertFalse(snapshot.call().isPresent());
                    
                    for (int i=0; i<nservers; ++i) {
                        futures.get(i).set(wchcs.get(i));
                    }

                    ImmutableSetMultimap.Builder<Long,ZNodePath> builder = ImmutableSetMultimap.builder();
                    switch (phase) {
                    case PRE_PHASE:
                    {
                        assertTrue(sessions.isEmpty());
                        for (FourLetterWords.Wchc wchc: wchcs) {
                            builder.putAll(wchc.asMultimap());
                        }
                        break;
                    }
                    case POST_PHASE:
                    {
                        assertEquals(nsessions*nservers*2, sessions.size());
                        for (Map.Entry<Long, PromiseTask<Long,Long>> lookup: sessions.entrySet()) {
                            Long session = toFrontend.apply(lookup.getKey());
                            Collection<ZNodePath> values = null;
                            for (SnapshotWatches.Phase p: SnapshotWatches.Phase.values()) {
                                if (p == SnapshotWatches.Phase.SNAPSHOT) {
                                    continue;
                                }
                                for (FourLetterWords.Wchc wchc: (List<FourLetterWords.Wchc>) inputs.get(p.ordinal())) {
                                    values = wchc.getValues(lookup.getKey());
                                    if (!values.isEmpty()) {
                                        builder.putAll(session, values);
                                    }
                                }
                            }
                            lookup.getValue().set(session);
                        }
                        break;
                    }
                    default:
                        fail(String.valueOf(phase));
                    }
                    assertEquals(builder.build(), ((FourLetterWords.Wchc) snapshot.chain().getLast().get(timeOut.value(), timeOut.unit())).asMultimap());
                }
                
                final FourLetterWords.Wchc result = snapshot.call().get();
                assertSame(snapshot.chain().getLast().get(), result);
                
                client.cache().lock().readLock().lock();
                try {
                    ImmutableSetMultimap.Builder<Long,ZNodePath> builder = ImmutableSetMultimap.builder();
                    Function<ZNodePath, Long> getEphemeral = Functions.forMap(ephemerals.build(), null);
                    for (StorageZNode<?> node: client.cache().cache().get(client.schema().apply(SnapshotWatchesTestSchema.Watches.class).path()).values()) {
                        SnapshotWatchesTestSchema.Watches.Session session = (SnapshotWatchesTestSchema.Watches.Session) node;
                        for (StorageZNode<?> child: session.get(SnapshotWatchesTestSchema.Watches.Session.Values.LABEL).values()) {
                            SnapshotWatchesTestSchema.Watches.Session.Values.Watch watch = (SnapshotWatchesTestSchema.Watches.Session.Values.Watch) child;
                            assertEquals(Watcher.WatcherType.Data, watch.data().get());
                            ZNodePath path = prefix.join(ZNodeLabel.fromString(watch.name()));
                            builder.put(Long.valueOf(session.name().longValue()), path);
                            Long ephemeral = getEphemeral.apply(path);
                            if (ephemeral != null) {
                                assertEquals(SessionIdHex.valueOf(ephemeral.longValue()), watch.get(SnapshotWatchesTestSchema.Watches.Session.Values.Watch.Ephemeral.LABEL).data().get());
                            } else {
                                assertFalse(watch.containsKey(SnapshotWatchesTestSchema.Watches.Session.Values.Watch.Ephemeral.LABEL));
                            }
                        }
                    }
                    assertEquals(result.asMultimap(), builder.build());
                } finally {
                    client.cache().lock().readLock().unlock();
                }
                
                return null;
            }
        };
        
        callWithService(
                injector, 
                callable);
    }

}
