package edu.uw.zookeeper.safari.storage.snapshot;


import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.zookeeper.Watcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.SimpleNameTrie;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.snapshot.SequentialEphemeralSnapshotIterator.SequentialChildIterator;
import edu.uw.zookeeper.safari.storage.snapshot.SequentialEphemeralSnapshotTrieTest.SequentialEphemeralSnapshotTrieTestSchema;

@RunWith(JUnit4.class)
public class SequentialEphemeralSnapshotIteratorTest extends AbstractMainTest {

    @ZNode(acl=Acls.Definition.ANYONE_ALL)
    public static final class SequentialEphemeralSnapshotIteratorTestSchema extends StorageZNode<Void> {

        public static final ZNodePath PATH = ZNodePath.root();
       
        public SequentialEphemeralSnapshotIteratorTestSchema(
                ValueNode<ZNodeSchema> schema,
                ByteCodec<Object> codec) {
            super(schema, codec, SimpleNameTrie.<StorageZNode<?>>rootPointer());
        }

        @ZNode
        public static class Ephemerals extends StorageZNode<Void> {

            @Name
            public static final ZNodeLabel LABEL = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL;
            public static final AbsoluteZNodePath PATH = (AbsoluteZNodePath) SequentialEphemeralSnapshotIteratorTestSchema.PATH.join(LABEL);

            public Ephemerals(ValueNode<ZNodeSchema> schema,
                    ByteCodec<Object> codec,
                    Pointer<StorageZNode<?>> parent) {
                super(schema, codec, parent);
            }

            @ZNode                
            public static class Sessions extends StorageZNode.SessionsZNode {

                public static final AbsoluteZNodePath PATH = Ephemerals.PATH.join(LABEL);
                
                public Sessions(ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<StorageZNode<?>> parent) {
                    super(schema, codec, parent);
                }

                @ZNode
                public static class Session extends StorageZNode.SessionZNode<Void> {

                    public static final AbsoluteZNodePath PATH = Sessions.PATH.join(ZNodeLabel.fromString(LABEL));
                    
                    public static AbsoluteZNodePath pathOf(Long session) {
                        return Sessions.PATH.join(labelOf(session));
                    }
                    
                    public Session(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<StorageZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
    
                    @ZNode                
                    public static class Values extends StorageZNode.ValuesZNode {

                        public static final AbsoluteZNodePath PATH = Session.PATH.join(LABEL);
                        
                        public static AbsoluteZNodePath pathOf(Long session) {
                            return Session.pathOf(session).join(LABEL);
                        }
                        
                        public Values(ValueNode<ZNodeSchema> schema,
                                ByteCodec<Object> codec,
                                Pointer<StorageZNode<?>> parent) {
                            super(schema, codec, parent);
                        }
    
                        @ZNode
                        public static class Ephemeral extends StorageZNode.EscapedNamedZNode<Void> {

                            public static final AbsoluteZNodePath PATH = Values.PATH.join(ZNodeLabel.fromString(LABEL));
                            
                            public Ephemeral(ValueNode<ZNodeSchema> schema,
                                    ByteCodec<Object> codec,
                                    Pointer<StorageZNode<?>> parent) {
                                super(schema, codec, parent);
                            }
        
                            public Ephemeral(
                                    String name,
                                    ValueNode<ZNodeSchema> schema,
                                    ByteCodec<Object> codec,
                                    Pointer<StorageZNode<?>> parent) {
                                super(name, schema, codec, parent);
                            }
                            
                            @ZNode
                            public static class Commit extends StorageZNode.CommitZNode {

                                public static final AbsoluteZNodePath PATH = Ephemeral.PATH.join(LABEL);
                                
                                public Commit(
                                        ValueNode<ZNodeSchema> schema,
                                        ByteCodec<Object> codec,
                                        Pointer<? extends StorageZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    @Test//(timeout=10000)
    public void test() throws Exception {
        final SimpleNameTrie<ValueNode<Optional<Long>>> data = SequentialEphemeralSnapshotTrieTest.createSmallTestData();
        
        final List<Component<?>> components = Modules.newServerAndClientComponents();
        final Injector injector = stopping(components, JacksonModule.create());
        
        final TimeValue timeOut = TimeValue.seconds(4L);
        
        final Callable<Void> callable = new Callable<Void>() {
            @SuppressWarnings("unchecked")
            @Override
            public Void call() throws Exception {
                // create a frontend and backend materializer
                final Serializers.ByteCodec<Object> codec = injector.getInstance(Key.get(new TypeLiteral<Serializers.ByteCodec<Object>>(){}));
                final int FRONT = 0;
                final int BACK = 1;
                final List<Materializer<StorageZNode<?>,Message.ServerResponse<?>>> materializers = Lists.newArrayListWithCapacity(BACK+1);
                for (int i=0; i<BACK+1; ++i) {
                    materializers.add(Materializer.<StorageZNode<?>, Message.ServerResponse<?>>fromHierarchy(SequentialEphemeralSnapshotIteratorTestSchema.class, codec, injector.getInstance(Key.get(Component.class, Names.named("client"))).injector().getInstance(ConnectionClientExecutorService.Builder.class).getConnectionClientExecutor()));
                }
                
                final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> client = SchemaClientService.create(materializers.get(FRONT), ImmutableList.<Service.Listener>of());
                client.startAsync().awaitRunning();
                injector.getInstance(ServiceMonitor.class).add(client);
                
                // fetch on commit notifications
                Watchers.FutureCallbackServiceListener.listen(
                        Watchers.EventToPathCallback.create(
                            Watchers.PathToQueryCallback.create(
                                    PathToQuery.forRequests(
                                            client.materializer(), 
                                            Operations.Requests.sync(), Operations.Requests.getData()), 
                                    Watchers.MaybeErrorProcessor.maybeNoNode(), 
                                    Watchers.StopServiceOnFailure.create(client))), 
                        client, 
                        client.notifications(), 
                        WatchMatcher.exact(
                                SequentialEphemeralSnapshotIteratorTestSchema.Ephemerals.Sessions.Session.Values.Ephemeral.Commit.PATH, 
                                Watcher.Event.EventType.NodeCreated), 
                        client.logger());
                
                SequentialEphemeralSnapshotTrieTest.snapshot(data, materializers.get(BACK), materializers.get(FRONT));

                final Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>> trie = SequentialEphemeralSnapshotTrie.create(SequentialEphemeralSnapshotTrieTestSchema.PATH, materializers.get(FRONT), logger).call().get();
                final SequentialEphemeralSnapshotIterator iterator = SequentialEphemeralSnapshotIterator.listen(SequentialEphemeralSnapshotIteratorTestSchema.Ephemerals.PATH, trie.first(), trie.second(), client.materializer(), client.cacheEvents(), client, logger);
                for (AbsoluteZNodePath leaf: trie.second()) {
                    ZNodePath parent = leaf.parent();
                    assertTrue(String.valueOf(parent), iterator.iterators().containsKey(parent));
                }
                
                Set<ZNodePath> keys;
                synchronized (iterator.iterators()) {
                    keys = ImmutableSet.copyOf(iterator.iterators().keySet());
                    for (SequentialChildIterator<?> parent: iterator.iterators().values()) {
                        SequentialChildIterator.NextChild<?> next = parent.peek();
                        assertFalse(String.valueOf(next), next.getCommitted().isDone());
                        assertTrue(String.valueOf(next), trie.second().contains(next.getNode().path()));
                        boolean valid = false;
                        for (SequentialNode<?> child: parent.parent().values()) {
                            if (child.isEmpty()) {
                                valid = (child == next.getNode());
                                break;
                            }
                        }
                        assertTrue(String.valueOf(next), valid);
                    }
                }
                
                Operations.Requests.Create create = Operations.Requests.create().setData(codec.toBytes(Boolean.TRUE));
                for (ZNodePath k: keys) {
                    SequentialChildIterator<?> parent = iterator.iterators().get(k);
                    do {
                        SequentialChildIterator.NextChild<?> next = parent.next();
                        Operations.unlessProtocolError(materializers.get(BACK).submit(create.setPath(next.getNode().getValue().join(StorageZNode.CommitZNode.LABEL)).build()).get(timeOut.value(), timeOut.unit()));
                        assertEquals(next.getNode().getValue(), next.getCommitted().get(timeOut.value(), timeOut.unit()));
                    } while (parent.hasNext());
                }
                
                LockableZNodeCache<StorageZNode<?>,?,?> cache = materializers.get(FRONT).cache();
                cache.lock().readLock().lock();
                try {
                    for (AbsoluteZNodePath leaf: trie.second()) {
                        SequentialNode<AbsoluteZNodePath> node = trie.first().get(leaf);
                        assertTrue(String.valueOf(node), cache.cache().get(node.getValue()).containsKey(StorageZNode.CommitZNode.LABEL));
                    }
                } finally {
                    cache.lock().readLock().lock();
                }
                
                return null;
            }
        };
        
        callWithService(
                injector, 
                callable);
    }
}
