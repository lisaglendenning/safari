package edu.uw.zookeeper.safari.storage.snapshot;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.SimpleNameTrie;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;
import edu.uw.zookeeper.safari.schema.PrefixCreator;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

@RunWith(JUnit4.class)
public class SequentialEphemeralSnapshotTrieTest extends AbstractMainTest {

    @ZNode(acl=Acls.Definition.ANYONE_ALL)
    public static final class SequentialEphemeralSnapshotTrieTestSchema extends StorageZNode<Void> {

        public static final ZNodePath PATH = ZNodePath.root();
       
        public SequentialEphemeralSnapshotTrieTestSchema(
                ValueNode<ZNodeSchema> schema,
                ByteCodec<Object> codec) {
            super(schema, codec, SimpleNameTrie.<StorageZNode<?>>rootPointer());
        }

        @ZNode
        public static class Ephemerals extends StorageZNode<Void> {

            @Name
            public static final ZNodeLabel LABEL = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL;
            public static final AbsoluteZNodePath PATH = (AbsoluteZNodePath) SequentialEphemeralSnapshotTrieTestSchema.PATH.join(LABEL);

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
                        }
                    }
                }
            }
        }
    }

    // create a small data set with a mixture of sequentials and ephemerals
    // assume all leafs are ephemeral
    // the value stored at a node isPresent() if it is ephemeral and 
    // the value is the owner session
    public static SimpleNameTrie<ValueNode<Optional<Long>>> createSmallTestData() {
        final ImmutableMap<String,Long> owners = ImmutableMap.of("a", Long.valueOf(0L), "b", Long.valueOf(1L));
        final ImmutableList<Pair<String,Long>> values = ImmutableList.of(
                Pair.create("b", owners.get("b")),
                Pair.create("a", (Long) null),
                Pair.create("a/a", owners.get("a")),
                Pair.create("a/" + Sequential.fromInt("b", 0).toString(), owners.get("b")),
                Pair.create("a/" + Sequential.fromInt("a", 1).toString(), owners.get("a")),
                Pair.create(Sequential.fromInt("c", 0).toString(), (Long) null),
                Pair.create(Sequential.fromInt("c", 0).toString() + "/a", owners.get("a")),
                Pair.create(Sequential.fromInt("c", 0).toString() + "/" + Sequential.fromInt("b", 0).toString(), owners.get("b")),
                Pair.create(Sequential.fromInt("b", 1).toString(), owners.get("b")),
                Pair.create(Sequential.fromInt("a", 2).toString(), owners.get("a")));
        return createTestData(values);
    }
    
    public static SimpleNameTrie<ValueNode<Optional<Long>>> createTestData(
            ImmutableList<Pair<String,Long>> values) {
        final SimpleNameTrie<ValueNode<Optional<Long>>> data = SimpleNameTrie.forRoot(ValueNode.root(Optional.<Long>absent()));
        for (Pair<String,Long> v: values) {
            ZNodePath path = ZNodePath.root().join(ZNodeName.fromString(v.first()));
            ValueNode<Optional<Long>> parent = data.longestPrefix(path);
            ValueNode<Optional<Long>> child = ValueNode.child(Optional.fromNullable(v.second()), path.suffix(parent.path()), parent);
            data.put(child.path(), child);
        }
        for (ValueNode<Optional<Long>> node: data) {
            boolean isPresent = node.get().isPresent();
            if (node.isEmpty()) {
                assertTrue(isPresent);
            } else {
                assertFalse(isPresent);
            }
        }
        assertEquals(values.size()+1, data.size());
        return data;
    }
    
    public static void snapshot(final SimpleNameTrie<ValueNode<Optional<Long>>> data, final Materializer<StorageZNode<?>,Message.ServerResponse<?>> backend, final Materializer<StorageZNode<?>,Message.ServerResponse<?>> frontend) throws Exception {
        // first create sessions in frontend client
        PrefixCreator.call(frontend).get();
        Set<Long> sessions = Sets.newHashSet();
        for (ValueNode<Optional<Long>> node: data) {
            if (node.get().isPresent()) {
                Long session = node.get().get();
                if (sessions.add(session)) {
                    Operations.unlessProtocolError(frontend.create(SequentialEphemeralSnapshotTrieTestSchema.Ephemerals.Sessions.Session.pathOf(session)).call().get());
                }
            }
        }
        // second, snapshot values in backend client
        for (Long session: sessions) {
            Operations.unlessProtocolError(backend.create(SequentialEphemeralSnapshotTrieTestSchema.Ephemerals.Sessions.Session.Values.pathOf(session)).call().get());
        }
        for (ValueNode<Optional<Long>> node: data) {
            if (node.get().isPresent()) {
                Long session = node.get().get();
                ZNodeLabel label = SequentialEphemeralSnapshotTrieTestSchema.Ephemerals.Sessions.Session.Values.Ephemeral.labelOf(node.path().suffix(ZNodePath.root()));
                Operations.unlessProtocolError(backend.create(SequentialEphemeralSnapshotTrieTestSchema.Ephemerals.Sessions.Session.Values.pathOf(session).join(label)).call().get());
            }
        }
    }

    public static void validate(final SimpleNameTrie<ValueNode<Optional<Long>>> data, final LockableZNodeCache<StorageZNode<?>,?,?> cache, final Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>> result) {
        // check that computed set of leaves equals the actual trie leaves
        ImmutableSet.Builder<AbsoluteZNodePath> leaves = ImmutableSet.builder();
        for (SequentialNode<AbsoluteZNodePath> node: result.first()) {
            if (node.isEmpty() && !node.path().isRoot()) {
                leaves.add((AbsoluteZNodePath) node.path());
            }
        }
        assertEquals(leaves.build(), result.second());
        
        // check that the leaves are equal to the sequential data leaves
        leaves = ImmutableSet.builder();
        for (ValueNode<Optional<Long>> node: data) {
            if (node.isEmpty() && !node.path().isRoot() && Sequential.maybeFromString(node.path().label().toString()).isPresent()) {
                leaves.add((AbsoluteZNodePath) node.path());
            }
        }
        assertEquals(leaves.build(), result.second());
        
        // check that the value of leaves points to the correct snapshot path
        cache.lock().readLock().lock();
        try {
            for (AbsoluteZNodePath path: result.second()) {
                SequentialNode<AbsoluteZNodePath> leaf = result.first().get(path);
                SequentialEphemeralSnapshotTrieTestSchema.Ephemerals.Sessions.Session.Values.Ephemeral ephemeral = (SequentialEphemeralSnapshotTrieTestSchema.Ephemerals.Sessions.Session.Values.Ephemeral) cache.cache().get(leaf.getValue());
                assertEquals(leaf.path(), ZNodePath.root().join(ZNodeName.fromString(ephemeral.name())));
            }
        } finally {
            cache.lock().readLock().unlock();
        }
    }

    @Test(timeout=10000)
    public void test() throws Exception {
        final SimpleNameTrie<ValueNode<Optional<Long>>> data = createSmallTestData();
        
        final List<Component<?>> components = Modules.newServerAndClientComponents();
        final Injector injector = stopping(components, JacksonModule.create());
        
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                // create a frontend and backend materializer
                final Serializers.ByteCodec<Object> codec = injector.getInstance(Key.get(new TypeLiteral<Serializers.ByteCodec<Object>>(){}));
                final int FRONT = 0;
                final int BACK = 1;
                final List<Materializer<StorageZNode<?>,Message.ServerResponse<?>>> materializers = Lists.newArrayListWithCapacity(BACK+1);
                for (int i=0; i<BACK+1; ++i) {
                    materializers.add(Materializer.<StorageZNode<?>, Message.ServerResponse<?>>fromHierarchy(SequentialEphemeralSnapshotTrieTestSchema.class, codec, injector.getInstance(Key.get(Component.class, Names.named("client"))).injector().getInstance(ConnectionClientExecutorService.Builder.class).getConnectionClientExecutor()));
                }
                
                snapshot(data, materializers.get(BACK), materializers.get(FRONT));
                
                final Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>> result = SequentialEphemeralSnapshotTrie.create(SequentialEphemeralSnapshotTrieTestSchema.PATH, materializers.get(FRONT), logger).call().get();
                
                validate(data, materializers.get(FRONT).cache(), result);
                
                return null;
            }
        };
        
        callWithService(
                injector, 
                callable);
    }
}
