package edu.uw.zookeeper.safari.control.schema;

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
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.region.LeaderEpoch;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;

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
        public static class Peers extends EntityDirectoryZNode<ServerInetAddressView, Peers.Peer.PeerAddress, Peers.Peer> {
    
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
            public static class Peer extends ControlZNode.IdentifierControlZNode {
                
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
        public static class Regions extends EntityDirectoryZNode<EnsembleView<ServerInetAddressView>, Regions.Region.Ensemble, Regions.Region> {

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
            
            @ZNode
            public static class Region extends ControlZNode.IdentifierControlZNode {

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
                    public static class Member extends IdentifierControlZNode {
    
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
        public static class Volumes extends EntityDirectoryZNode<ZNodePath, Volumes.Volume.Path, Volumes.Volume> {
            
            @Name
            public static ZNodeLabel LABEL = ZNodeLabel.fromString("volumes");

            public static final AbsoluteZNodePath PATH = Safari.PATH.join(LABEL);
            
            protected static final Function<ZNodePath, Hash.Hashed> HASHER = new Function<ZNodePath, Hash.Hashed>() {
                @Override
                public Hash.Hashed apply(ZNodePath value) {
                    return Hash.default32().apply(value.toString());
                }
            };

            public static Volumes fromTrie(NameTrie<ControlZNode<?>> trie) {
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
            public static class Volume extends IdentifierControlZNode {

                public static final AbsoluteZNodePath PATH = Volumes.PATH.join(LABEL);
                
                public static AbsoluteZNodePath pathOf(Identifier id) {
                    return Volumes.PATH.join(ZNodeLabel.fromString(id.toString()));
                }

                public static Volume fromTrie(NameTrie<ControlZNode<?>> trie, Identifier id) {
                    return (Volume) trie.get(pathOf(id));
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
                    
                    @SuppressWarnings("unchecked")
                    public NavigableMap<ZNodeName,Version> versions() {
                        return (NavigableMap<ZNodeName,Version>) (NavigableMap<?,?>) tailMap(Latest.LABEL, false);
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
                            if (a.equals(Latest.LABEL)) {
                                if (b.equals(Latest.LABEL)) {
                                    return 0;
                                } else {
                                    return -1;
                                }
                            } else {
                                if (b.equals(Latest.LABEL)) {
                                    return 1;
                                } else {
                                    return UnsignedLong.valueOf(a.toString()).compareTo(UnsignedLong.valueOf(b.toString()));
                                }
                            }
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
                    public static class Version extends NamedControlZNode<Void,UnsignedLong> implements NavigableMap<ZNodeName,ControlZNode<?>> {

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
                        
                        public static Version fromTrie(NameTrie<ControlZNode<?>> trie, Identifier id, UnsignedLong version) {
                            return (Version) trie.get(pathOf(id, version));
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
                            super(name, schema, codec, null, stat, stamp, parent, Maps.<ZNodeName,ZNodeName,ControlZNode<?>>newTreeMap(new EntryComparator()));
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
                        
                        public VersionedId id() {
                            return VersionedId.valueOf(
                                    name(), 
                                    log().volume().name());
                        }
                        
                        @SuppressWarnings("unchecked")
                        public NavigableMap<ZNodeName,Entry> entries() {
                            return (NavigableMap<ZNodeName,Entry>) (NavigableMap<?,?>) tailMap(Xomega.LABEL, false);
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

                        public static class EntryComparator implements Comparator<ZNodeName> {

                            public EntryComparator() {}
                            
                            @Override
                            public int compare(final ZNodeName a, final ZNodeName b) {
                                if (a.toString().startsWith(Entry.PREFIX.toString())) {
                                    if (b.toString().startsWith(Entry.PREFIX.toString())) {
                                        return Sequential.fromString(a.toString()).compareTo(Sequential.fromString(b.toString()));
                                    } else {
                                        return 1;
                                    }
                                } else {
                                    if (b.toString().startsWith(Entry.PREFIX.toString())) {
                                        return -1;
                                    } else {
                                        return a.toString().compareTo(b.toString());
                                    }
                                }
                            }
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
    
                        @ZNode(dataType=RegionAndLeaves.class)
                        public static class State extends ControlZNode<RegionAndLeaves> {
    
                            @Name
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("state");

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }
                            
                            public static State fromTrie(Identifier id, UnsignedLong version, NameTrie<ControlZNode<?>> trie) {
                                return (State) trie.get(pathOf(id, version));
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
                        
                        @ZNode(createMode=CreateMode.PERSISTENT_SEQUENTIAL, dataType=VolumeLogEntry.class)
                        public static class Entry extends NamedControlZNode<VolumeLogEntry<?>,Sequential<String,?>> {

                            public static ZNodeLabel PREFIX = ZNodeLabel.fromString("entry");

                            @Name(type=NameType.PATTERN)
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString(PREFIX.toString() + Sequential.SUFFIX_PATTERN);

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version, ZNodeLabel entry) {
                                return Version.pathOf(id, version).join(entry);
                            }
                            
                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version, Sequential<?,?> entry) {
                                return pathOf(id, version, ZNodeLabel.fromString(entry.toString()));
                            }
    
                            public Entry(
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                this(Sequential.fromString(parent.name().toString()), schema, codec, parent);
                            }
                            
                            public Entry(
                                    Sequential<String,?> name,
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                super(name, schema, codec, parent);
                            }

                            public Entry(
                                    Sequential<String,?> name,
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
                            
                            public Vote vote() {
                                return (Vote) get(Vote.LABEL);
                            }
                            
                            public Commit commit() {
                                return (Commit) get(Commit.LABEL);
                            }
                            
                            public VolumeLogEntryPath entryPath() {
                                return VolumeLogEntryPath.valueOf(
                                        version().id(), 
                                        name());
                            }
                            
                            @ZNode(dataType=Boolean.class)
                            public static class Vote extends ControlZNode<Boolean> {

                                @Name
                                public static ZNodeLabel LABEL = ZNodeLabel.fromString("vote");

                                public static final AbsoluteZNodePath PATH = Version.Entry.PATH.join(LABEL);

                                public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version, ZNodeLabel entry) {
                                    return Version.Entry.pathOf(id, version, entry).join(LABEL);
                                }

                                public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                    return Version.Entry.pathOf(id, version).join(LABEL);
                                }
                                
                                public Vote(
                                        ValueNode<ZNodeSchema> schema,
                                        Serializers.ByteCodec<Object> codec,
                                        NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                                
                                public Version.Entry entry() {
                                    return (Version.Entry) parent().get();
                                }
                            }
                            
                            @ZNode(dataType=Boolean.class)
                            public static class Commit extends ControlZNode<Boolean> {
                                
                                @Name
                                public static ZNodeLabel LABEL = ZNodeLabel.fromString("commit");

                                public static final AbsoluteZNodePath PATH = Version.Entry.PATH.join(LABEL);

                                public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version, ZNodeLabel entry) {
                                    return Version.Entry.pathOf(id, version, entry).join(LABEL);
                                }

                                public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                    return Version.Entry.pathOf(id, version).join(LABEL);
                                }
                                
                                public Commit(
                                        ValueNode<ZNodeSchema> schema,
                                        Serializers.ByteCodec<Object> codec,
                                        NameTrie.Pointer<? extends ControlZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                                
                                public Version.Entry entry() {
                                    return (Version.Entry) parent().get();
                                }
                            }
                        }
                    }
                }
            }            
        }
    }
}
