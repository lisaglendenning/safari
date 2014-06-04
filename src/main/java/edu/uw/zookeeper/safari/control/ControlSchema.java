package edu.uw.zookeeper.safari.control;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedLong;
import com.google.common.primitives.UnsignedLongs;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.data.NameType;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.data.VolumeState;
import edu.uw.zookeeper.safari.region.LeaderEpoch;

@ZNode(acl=Acls.Definition.ANYONE_ALL)
public class ControlSchema extends ControlZNode<Void> {

    public ControlSchema(ValueNode<ZNodeSchema> schema,
            ByteCodec<Object> codec) {
        super(schema, codec, SimpleLabelTrie.<ControlZNode<?>>rootPointer());
    }
    
    @ZNode
    public static class Safari extends ControlZNode<Void> {

        @Name
        public static final ZNodeLabel LABEL = ZNodeLabel.fromString("safari");
        
        public static final AbsoluteZNodePath PATH = (AbsoluteZNodePath) ZNodePath.root().join(LABEL);
       
        public Safari(ValueNode<ZNodeSchema> schema,
                ByteCodec<Object> codec,
                Pointer<ControlZNode<?>> parent) {
            super(schema, codec, parent);
        }

        @ZNode
        public static class Peers extends ControlEntityDirectoryZNode<ServerInetAddressView, Peers.Peer.PeerAddress, Peers.Peer> {
    
            @Name
            public static ZNodeLabel LABEL = ZNodeLabel.fromString("peers");

            public static final AbsoluteZNodePath PATH = Safari.PATH.join(LABEL);
            
            protected static final Function<ServerInetAddressView, Hash.Hashed> HASHER =
                new Function<ServerInetAddressView, Hash.Hashed>() {
                    @Override
                    public Hash.Hashed apply(ServerInetAddressView value) {
                        return Hash.default32().apply(value.toString());
                    }
                };
            
            public Peers(ValueNode<ZNodeSchema> schema,
                    ByteCodec<Object> codec,
                    Pointer<ControlZNode<?>> parent) {
                super(schema, codec, parent);
            }

            @Override
            public Class<Peer> entityType() {
                return Peer.class;
            }

            @Override
            public Class<Peer.PeerAddress> hashedType() {
                return Peer.PeerAddress.class;
            }
            
            @Override
            public Function<ServerInetAddressView, Hash.Hashed> hasher() {
                return HASHER;
            }
            
            @ZNode
            public static class Peer extends ControlZNode.IdentifierZNode {
                
                public static AbsoluteZNodePath pathOf(Identifier peer) {
                    return Peers.PATH.join(ZNodeLabel.fromString(peer.toString()));
                }
    
                public Peer(
                        ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<ControlZNode<?>> parent) {
                    this(Identifier.valueOf(parent.name().toString()), schema, codec, parent);
                }
                
                public Peer(
                        Identifier name,
                        ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<ControlZNode<?>> parent) {
                    super(name, schema, codec, parent);
                }
/*
                public static CachedFunction<Peers.Entity, Boolean> isPresent(
                        final Materializer<ControlZNode<?>, ?> materializer) {
                    Function<Peers.Entity, Boolean> cached = new Function<Peers.Entity, Boolean>() {
                        @Override
                        public Boolean apply(Peers.Entity input) {
                            return materializer.containsKey(input.presence().path());
                        }
                    };
                    AsyncFunction<Peers.Entity, Boolean> lookup = new AsyncFunction<Peers.Entity, Boolean>() {
                        @Override
                        public ListenableFuture<Boolean> apply(Peers.Entity input) {
                            return input.presence().exists(materializer);
                        }
                    };
                    return CachedFunction.create(cached, lookup);
                }
    
                public static AsyncFunction<ServerInetAddressView, Entity> lookup(
                        final Materializer<?> materializer) {
                    return new AsyncFunction<ServerInetAddressView, Entity>() {
                            @Override
                            public ListenableFuture<Entity> apply(
                                    final ServerInetAddressView value)
                                    throws Exception {
                                return Control.LookupEntityTask.call(
                                        value, schema(), materializer);
                            }
                    };
                }
    

*/
                @ZNode(createMode=CreateMode.EPHEMERAL)
                public static class Presence extends ControlZNode<Void> {

                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("presence");

                    public static AbsoluteZNodePath pathOf(Identifier peer) {
                        return Peer.pathOf(peer).join(LABEL);
                    }
        
                    public Presence(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
                }
                
                @ZNode(dataType=ServerInetAddressView.class)
                public static class ClientAddress extends ControlZNode<ServerInetAddressView> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("clientAddress");

                    public static AbsoluteZNodePath pathOf(Identifier peer) {
                        return Peer.pathOf(peer).join(LABEL);
                    }
        
                    public ClientAddress(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
                }
                
                @ZNode(dataType=ServerInetAddressView.class)
                public static class PeerAddress extends ControlZNode<ServerInetAddressView> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("peerAddress");

                    public static AbsoluteZNodePath pathOf(Identifier peer) {
                        return Peer.pathOf(peer).join(LABEL);
                    }
        
                    public PeerAddress(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
                }
                
                @ZNode(dataType=ServerInetAddressView.class)
                public static class StorageAddress extends ControlZNode<ServerInetAddressView> {

                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("storageAddress");

                    public static AbsoluteZNodePath pathOf(Identifier peer) {
                        return Peer.pathOf(peer).join(LABEL);
                    }
        
                    public StorageAddress(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
                }
            }
        }
    
        @ZNode
        public static class Regions extends ControlEntityDirectoryZNode<EnsembleView<ServerInetAddressView>, Regions.Region.Ensemble, Regions.Region> {

            @Name
            public static ZNodeLabel LABEL = ZNodeLabel.fromString("regions");

            public static final AbsoluteZNodePath PATH = Safari.PATH.join(LABEL);
            
            protected static final Function<EnsembleView<ServerInetAddressView>, Hash.Hashed> HASHER = new Function<EnsembleView<ServerInetAddressView>, Hash.Hashed>() {
                @Override
                public Hash.Hashed apply(EnsembleView<ServerInetAddressView> value) {
                    return Hash.default32().apply(EnsembleView.toString(value));
                }
            };

            public static Regions get(NameTrie<ControlZNode<?>> trie) {
                return (Regions) trie.get(PATH);
            }
            
            public Regions(ValueNode<ZNodeSchema> schema,
                    ByteCodec<Object> codec,
                    Pointer<ControlZNode<?>> parent) {
                super(schema, codec, parent);
            }

            @Override
            public Class<Region> entityType() {
                return Region.class;
            }

            @Override
            public Class<Region.Ensemble> hashedType() {
                return Region.Ensemble.class;
            }
            
            @Override
            public Function<EnsembleView<ServerInetAddressView>, Hash.Hashed> hasher() {
                return HASHER;
            }
            
            /*
            public static ListenableFuture<List<Regions.Entity>> getRegions(ClientExecutor<? super Records.Request, ?, ?> client) {
                client.submit(Operations.Requests.sync().setPath(path(Regions.class)).build());
                return Futures.transform(
                        client.submit(Operations.Requests.getChildren().setPath(path(Regions.class)).build()), 
                        new AsyncFunction<Operation.ProtocolResponse<?>, List<Regions.Entity>>() {
                            @Override
                            public ListenableFuture<List<Regions.Entity>> apply(Operation.ProtocolResponse<?> input) throws KeeperException {
                                Records.ChildrenGetter response = (Records.ChildrenGetter) Operations.unlessError(input.record());
                                List<Regions.Entity> result = Lists.newArrayListWithCapacity(response.getChildren().size());
                                for (String child: response.getChildren()) {
                                    result.add(Regions.Entity.of(Identifier.valueOf(child)));
                                }
                                return Futures.immediateFuture(result);
                            }
                        });
            }
            */
            @ZNode
            public static class Region extends ControlZNode.IdentifierZNode {

                public static final AbsoluteZNodePath PATH = Regions.PATH.join(LABEL);
                
                public static AbsoluteZNodePath pathOf(Identifier region) {
                    return Regions.PATH.join(ZNodeLabel.fromString(region.toString()));
                }
    
                public Region(
                        ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<ControlZNode<?>> parent) {
                    this(Identifier.valueOf(parent.name().toString()), schema, codec, parent);
                }
                
                public Region(
                        Identifier name,
                        ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<ControlZNode<?>> parent) {
                    super(name, schema, codec, parent);
                }
                
                public Members getMembers() {
                    return (Members) get(Members.LABEL);
                }
                
                public Ensemble getBackend() {
                    return (Ensemble) get(Ensemble.LABEL);
                }
                
/*
                public static AsyncFunction<EnsembleView<ServerInetAddressView>, Entity> lookup(
                        final Materializer<?> materializer) {
                    return new AsyncFunction<EnsembleView<ServerInetAddressView>, Entity>() {
                            @Override
                            public ListenableFuture<Entity> apply(
                                    final EnsembleView<ServerInetAddressView> value)
                                    throws Exception {
                                return Control.LookupEntityTask.call(
                                        value, schema(), materializer);
                            }
                    };
                }
*/

                @ZNode(dataType=EnsembleView.class)
                public static class Ensemble extends ControlZNode<EnsembleView<ServerInetAddressView>> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("ensemble");

                    public static AbsoluteZNodePath pathOf(Identifier region) {
                        return Region.pathOf(region).join(LABEL);
                    }
        
                    public Ensemble(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
                }
                
                @ZNode
                public static class Members extends ControlZNode<Void> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("members");

                    public static AbsoluteZNodePath pathOf(Identifier region) {
                        return Region.pathOf(region).join(LABEL);
                    }
        
                    public Members(
                            ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        super(schema, codec, parent);
                    }

                    @ZNode
                    public static class Member extends IdentifierZNode {
    
                        public Member(
                                ValueNode<ZNodeSchema> schema,
                                ByteCodec<Object> codec,
                                Pointer<ControlZNode<?>> parent) {
                            this(Identifier.valueOf(parent.name().toString()), schema, codec, parent);
                        }
                        
                        public Member(
                                Identifier name,
                                ValueNode<ZNodeSchema> schema,
                                ByteCodec<Object> codec,
                                Pointer<ControlZNode<?>> parent) {
                            super(name, schema, codec, parent);
                        }
                    }
                }
                
                @ZNode(dataType=Identifier.class)
                public static class Leader extends ControlZNode<Identifier> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("leader");

                    public static AbsoluteZNodePath pathOf(Identifier region) {
                        return Region.pathOf(region).join(LABEL);
                    }
                    
                    public static Leader get(NameTrie<ControlZNode<?>> trie, Identifier region) {
                        return (Leader) trie.get(pathOf(region));
                    }
        
                    public Leader(
                            ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
                    
                    public Optional<LeaderEpoch> getLeaderEpoch() {
                        StampedValue<?> nodeData = data();
                        StampedValue<Records.ZNodeStatGetter> nodeStat = stat();
                        Identifier leader = (Identifier) nodeData.get();
                        Records.ZNodeStatGetter stat = nodeStat.get();
                        if ((leader != null) && (stat != null)) {
                            if (nodeData.stamp() >= stat.getMzxid()) {
                                return Optional.of(LeaderEpoch.of(stat.getVersion(), leader));
                            }
                        }
                        return Optional.absent();
                    }
                }
            }
        }
    
        @ZNode
        public static class Volumes extends ControlEntityDirectoryZNode<ZNodePath, Volumes.Volume.Path, Volumes.Volume> {
            
            @Name
            public static ZNodeLabel LABEL = ZNodeLabel.fromString("volumes");

            public static final AbsoluteZNodePath PATH = Safari.PATH.join(LABEL);
            
            protected static final Function<ZNodePath, Hash.Hashed> HASHER = new Function<ZNodePath, Hash.Hashed>() {
                @Override
                public Hash.Hashed apply(ZNodePath value) {
                    return Hash.default32().apply(value.toString());
                }
            };

            public static Volumes get(NameTrie<ControlZNode<?>> trie) {
                return (Volumes) trie.get(PATH);
            }
            
            public Volumes(ValueNode<ZNodeSchema> schema,
                    ByteCodec<Object> codec,
                    Pointer<ControlZNode<?>> parent) {
                super(schema, codec, parent);
            }

            @Override
            public Class<Volume> entityType() {
                return Volume.class;
            }

            @Override
            public Class<Volume.Path> hashedType() {
                return Volume.Path.class;
            }
            
            @Override
            public Function<ZNodePath, Hash.Hashed> hasher() {
                return HASHER;
            }
            
            @ZNode
            public static class Volume extends IdentifierZNode {

                public static final AbsoluteZNodePath PATH = Volumes.PATH.join(LABEL);
                
                public static AbsoluteZNodePath pathOf(Identifier id) {
                    return Volumes.PATH.join(ZNodeLabel.fromString(id.toString()));
                }
    
                public Volume(
                        ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<ControlZNode<?>> parent) {
                    this(Identifier.valueOf(parent.name().toString()), schema, codec, parent);
                }
                
                public Volume(
                        Identifier name,
                        ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<ControlZNode<?>> parent) {
                    super(name, schema, codec, parent);
                }
                
                public Path getPath() {
                    return (Path) get(Path.LABEL);
                }

                public Log getLog() {
                    return (Log) get(Log.LABEL);
                }

                @ZNode(dataType=ZNodePath.class)
                public static class Path extends ControlZNode<ZNodePath> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("path");

                    public static final AbsoluteZNodePath PATH = Volume.PATH.join(LABEL);
                    
                    public static AbsoluteZNodePath pathOf(Identifier id) {
                        return Volume.pathOf(id).join(LABEL);
                    }
                    
                    public static AsyncFunction<Identifier, ZNodePath> byId(
                            final Materializer<ControlZNode<?>, ?> materializer) {
                        return new AsyncFunction<Identifier, ZNodePath>(){
                            @Override
                            public ListenableFuture<ZNodePath> apply(final Identifier id)
                                    throws Exception {
                                final AbsoluteZNodePath path = ControlSchema.Safari.Volumes.Volume.Path.pathOf(id);
                                materializer.sync(path).call();
                                return Futures.transform(
                                        materializer.getData(path).call(), 
                                        new AsyncFunction<Operation.ProtocolResponse<?>, ZNodePath>() {
                                            @Override
                                            public ListenableFuture<ZNodePath> apply(Operation.ProtocolResponse<?> response) throws KeeperException {
                                                Operations.unlessError(response.record());
                                                materializer.cache().lock().readLock().lock();
                                                try {
                                                    ControlZNode<?> node = materializer.cache().cache().get(path);
                                                    return Futures.immediateFuture((ZNodePath) node.data().get());
                                                } finally {
                                                    materializer.cache().lock().readLock().unlock();
                                                }
                                            }
                                        });
                            }
                        };
                    }
                    
                    public Path(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
                    
                    public Volume volume() {
                        return (Volume) parent().get();
                    }
                }
    
                @ZNode
                public static class Log extends ControlZNode<Void> implements NavigableMap<ZNodeName,ControlZNode<?>> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("log");

                    public static final AbsoluteZNodePath PATH = Volume.PATH.join(LABEL);

                    public static AbsoluteZNodePath pathOf(Identifier id) {
                        return Volume.pathOf(id).join(LABEL);
                    }
                    
                    public Log(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<ControlZNode<?>> parent) {
                        this(schema, codec, null, -1L, parent);
                    }

                    public Log(
                            ValueNode<ZNodeSchema> schema,
                            Serializers.ByteCodec<Object> codec,
                            Records.ZNodeStatGetter stat,
                            long stamp,
                            NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                        super(schema, codec, null, stat, stamp, parent, Maps.<ZNodeName,ZNodeName,ControlZNode<?>>newTreeMap(new VersionComparator()));
                    }

                    public Volume volume() {
                        return (Volume) parent().get();
                    }

                    public Latest latest() {
                        return (Latest) get(Latest.LABEL);
                    }
                    
                    public Version version(UnsignedLong version) {
                        return (Version) get(ZNodeLabel.fromString(version.toString()));
                    }

                    @Override
                    public Comparator<? super ZNodeName> comparator() {
                        return delegate().comparator();
                    }

                    @Override
                    public ZNodeName firstKey() {
                        return delegate().firstKey();
                    }

                    @Override
                    public ZNodeName lastKey() {
                        return delegate().lastKey();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, ControlZNode<?>> ceilingEntry(
                            ZNodeName key) {
                        return delegate().ceilingEntry(key);
                    }

                    @Override
                    public ZNodeName ceilingKey(ZNodeName key) {
                        return delegate().ceilingKey(key);
                    }

                    @Override
                    public NavigableSet<ZNodeName> descendingKeySet() {
                        return delegate().descendingKeySet();
                    }

                    @Override
                    public NavigableMap<ZNodeName, ControlZNode<?>> descendingMap() {
                        return delegate().descendingMap();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, ControlZNode<?>> firstEntry() {
                        return delegate().firstEntry();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, ControlZNode<?>> floorEntry(
                            ZNodeName key) {
                        return delegate().floorEntry(key);
                    }

                    @Override
                    public ZNodeName floorKey(ZNodeName key) {
                        return delegate().floorKey(key);
                    }

                    @Override
                    public SortedMap<ZNodeName, ControlZNode<?>> headMap(
                            ZNodeName toKey) {
                        return delegate().headMap(toKey);
                    }

                    @Override
                    public NavigableMap<ZNodeName, ControlZNode<?>> headMap(
                            ZNodeName toKey, boolean inclusive) {
                        return delegate().headMap(toKey, inclusive);
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, ControlZNode<?>> higherEntry(
                            ZNodeName key) {
                        return delegate().higherEntry(key);
                    }

                    @Override
                    public ZNodeName higherKey(ZNodeName key) {
                        return delegate().higherKey(key);
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, ControlZNode<?>> lastEntry() {
                        return delegate().lastEntry();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, ControlZNode<?>> lowerEntry(
                            ZNodeName key) {
                        return delegate().lowerEntry(key);
                    }

                    @Override
                    public ZNodeName lowerKey(ZNodeName key) {
                        return delegate().lowerKey(key);
                    }

                    @Override
                    public NavigableSet<ZNodeName> navigableKeySet() {
                        return delegate().navigableKeySet();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, ControlZNode<?>> pollFirstEntry() {
                        return delegate().pollFirstEntry();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, ControlZNode<?>> pollLastEntry() {
                        return delegate().pollLastEntry();
                    }

                    @Override
                    public SortedMap<ZNodeName, ControlZNode<?>> subMap(
                            ZNodeName fromKey, ZNodeName toKey) {
                        return delegate().subMap(fromKey, toKey);
                    }

                    @Override
                    public NavigableMap<ZNodeName, ControlZNode<?>> subMap(
                            ZNodeName fromKey, boolean fromInclusive,
                            ZNodeName toKey, boolean toInclusive) {
                        return delegate().subMap(fromKey, fromInclusive, toKey, toInclusive);
                    }

                    @Override
                    public SortedMap<ZNodeName, ControlZNode<?>> tailMap(
                            ZNodeName fromKey) {
                        return delegate().tailMap(fromKey);
                    }

                    @Override
                    public NavigableMap<ZNodeName, ControlZNode<?>> tailMap(
                            ZNodeName fromKey, boolean inclusive) {
                        return delegate().tailMap(fromKey, inclusive);
                    }
                    
                    @Override
                    protected NavigableMap<ZNodeName, ControlZNode<?>> delegate() {
                        return (NavigableMap<ZNodeName, ControlZNode<?>>) super.delegate();
                    }
                    
                    public static class VersionComparator implements Comparator<ZNodeName> {

                        public VersionComparator() {}
                        
                        @Override
                        public int compare(final ZNodeName a, final ZNodeName b) {
                            final UnsignedLong avalue = a.equals(Latest.LABEL) ? UnsignedLong.ZERO : UnsignedLong.valueOf(a.toString());
                            final UnsignedLong bvalue = b.equals(Latest.LABEL) ? UnsignedLong.ZERO : UnsignedLong.valueOf(b.toString());
                            return avalue.compareTo(bvalue);
                        }
                    }
                    
                    @ZNode(dataType=UnsignedLong.class)
                    public static class Latest extends ControlZNode<UnsignedLong> {
        
                        @Name
                        public static ZNodeLabel LABEL = ZNodeLabel.fromString("latest");

                        public static final AbsoluteZNodePath PATH = Log.PATH.join(LABEL);

                        public static AbsoluteZNodePath pathOf(Identifier id) {
                            return Log.pathOf(id).join(LABEL);
                        }
                        
                        public Latest(ValueNode<ZNodeSchema> schema,
                                ByteCodec<Object> codec,
                                Pointer<ControlZNode<?>> parent) {
                            super(schema, codec, parent);
                        }

                        public Log log() {
                            return (Log) parent().get();
                        }
                    }
                    
                    @ZNode
                    public static class Version extends ControlNamedZNode<Void,UnsignedLong> {

                        @Name(type=NameType.PATTERN)
                        public static final ZNodeLabel LABEL = ZNodeLabel.fromString("[0-9]+");

                        public static final AbsoluteZNodePath PATH = Log.PATH.join(LABEL);

                        public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                            return Log.pathOf(id).join(ZNodeLabel.fromString(version.toString()));
                        }
                        
                        public static UnsignedLong valueOfLabel(Object label) {
                            return UnsignedLong.valueOf(label.toString());
                        }
    
                        public static String labelOfValue(long value) {
                            return UnsignedLongs.toString(value);
                        }

                        public Version(
                                ValueNode<ZNodeSchema> schema,
                                Serializers.ByteCodec<Object> codec,
                                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                            this(UnsignedLong.valueOf(parent.name().toString()), schema, codec, parent);
                        }
                        
                        public Version(
                                UnsignedLong name,
                                ValueNode<ZNodeSchema> schema,
                                Serializers.ByteCodec<Object> codec,
                                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                            this(name, schema, codec, null, -1L, parent);
                        }

                        public Version(
                                UnsignedLong name,
                                ValueNode<ZNodeSchema> schema,
                                Serializers.ByteCodec<Object> codec,
                                Records.ZNodeStatGetter stat,
                                long stamp,
                                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                            super(name, schema, codec, null, stat, stamp, parent);
                        }

                        public Log log() {
                            return (Log) parent().get();
                        }
                        
                        public Lease lease() {
                            return (Lease) get(Lease.LABEL);
                        }

                        public State state() {
                            return (State) get(State.LABEL);
                        }

                        public Xomega xomega() {
                            return (Xomega) get(Xomega.LABEL);
                        }
                        
                        @ZNode(dataType=UnsignedLong.class)
                        public static class Lease extends ControlZNode<UnsignedLong> {
    
                            @Name
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("lease");

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }
                            
                            public Lease(
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                super(schema, codec, parent);
                            }

                            public Version version() {
                                return (Version) parent().get();
                            }
                        }
    
                        @ZNode(dataType=VolumeState.class)
                        public static class State extends ControlZNode<VolumeState> {
    
                            @Name
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("state");

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }
                            
                            public State(
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                super(schema, codec, parent);
                            }

                            public Version version() {
                                return (Version) parent().get();
                            }
                        }

                        @ZNode(dataType=Long.class)
                        public static class Xomega extends ControlZNode<Long> {
                            
                            @Name
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("xomega");

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }
    
                            public Xomega(
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                super(schema, codec, parent);
                            }

                            public Version version() {
                                return (Version) parent().get();
                            }
                        }
                        
                        // FIXME: real dataType?
                        @ZNode(createMode=CreateMode.PERSISTENT_SEQUENTIAL, dataType=String.class)
                        public static class Entry extends ControlNamedZNode<String,Sequential<?,?>> {
    
                            @Name(type=NameType.PATTERN)
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("entry" + Sequential.SequentialConverter.SUFFIX_PATTERN);
    
                            public Entry(
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                this(Sequential.fromString(parent.name().toString()), schema, codec, parent);
                            }
                            
                            public Entry(
                                    Sequential<?,?> name,
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                this(name, schema, codec, null, -1L, parent);
                            }

                            public Entry(
                                    Sequential<?,?> name,
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    Records.ZNodeStatGetter stat,
                                    long stamp,
                                    NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                super(name, schema, codec, null, stat, stamp, parent);
                            }

                            public Version version() {
                                return (Version) parent().get();
                            }
                            
                            @ZNode(dataType=Boolean.class)
                            public static class Vote extends ControlZNode<Boolean> {
                                
                                @Name
                                public static ZNodeLabel LABEL = ZNodeLabel.fromString("vote");
        
                                public Vote(
                                        ValueNode<ZNodeSchema> schema,
                                        Serializers.ByteCodec<Object> codec,
                                        NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                            }
                            
                            @ZNode
                            public static class Committed extends ControlZNode<Void> {
                                
                                @Name
                                public static ZNodeLabel LABEL = ZNodeLabel.fromString("committed");
        
                                public Committed(
                                        ValueNode<ZNodeSchema> schema,
                                        Serializers.ByteCodec<Object> codec,
                                        NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                            }
                            
                            @ZNode
                            public static class Aborted extends ControlZNode<Void> {
                                
                                @Name
                                public static ZNodeLabel LABEL = ZNodeLabel.fromString("aborted");
        
                                public Aborted(
                                        ValueNode<ZNodeSchema> schema,
                                        Serializers.ByteCodec<Object> codec,
                                        NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                            }
                        }
                    }
                }
            }            
        }
    }
}
