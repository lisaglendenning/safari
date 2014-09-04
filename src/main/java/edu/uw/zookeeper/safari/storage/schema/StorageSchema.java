package edu.uw.zookeeper.safari.storage.schema;


import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedLong;
import com.google.common.primitives.UnsignedLongs;

import edu.uw.zookeeper.WatchType;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NameType;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.SimpleNameTrie;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;


@ZNode(acl=Acls.Definition.ANYONE_ALL)
public class StorageSchema extends StorageZNode<Void> {

    public static final ZNodePath PATH = ZNodePath.root();
   
    public StorageSchema(ValueNode<ZNodeSchema> schema,
            ByteCodec<Object> codec) {
        super(schema, codec, SimpleNameTrie.<StorageZNode<?>>rootPointer());
    }

    @ZNode
    public static class Safari extends StorageZNode<Void> {

        @Name
        public static final ZNodeLabel LABEL = ZNodeLabel.fromString("safari");

        public static final AbsoluteZNodePath PATH = (AbsoluteZNodePath) StorageSchema.PATH.join(LABEL);
       
        public Safari(ValueNode<ZNodeSchema> schema,
                ByteCodec<Object> codec,
                Pointer<StorageZNode<?>> parent) {
            super(schema, codec, parent);
        }
        
        @ZNode
        public static class Volumes extends StorageZNode<Void> {
    
            @Name
            public static ZNodeLabel LABEL = ZNodeLabel.fromString("volumes");

            public static final AbsoluteZNodePath PATH = Safari.PATH.join(LABEL);
            
            public static Identifier shardOfPath(ZNodePath path) {
                assert (path.startsWith(PATH));
                assert (path.length() > PATH.length());
                final int start = PATH.length() + 1;
                final int end = path.toString().indexOf(ZNodePath.SLASH, start);
                return Identifier.valueOf((end == -1) ? path.toString().substring(start) : path.toString().substring(start, end));
            }

            public static Volumes fromTrie(NameTrie<StorageZNode<?>> trie) {
                return (Volumes) trie.get(PATH);
            }
           
            public Volumes(ValueNode<ZNodeSchema> schema,
                    ByteCodec<Object> codec,
                    Pointer<StorageZNode<?>> parent) {
                super(schema, codec, parent);
            }
            
            @ZNode
            public static class Volume extends StorageZNode.IdentifierStorageZNode {

                public static final AbsoluteZNodePath PATH = Volumes.PATH.join(ZNodeLabel.fromString(LABEL));

                public static AbsoluteZNodePath pathOf(Identifier volume) {
                    return Volumes.PATH.join(ZNodeLabel.fromString(volume.toString()));
                }
    
                public Volume(
                        ValueNode<ZNodeSchema> schema,
                        Serializers.ByteCodec<Object> codec,
                        NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                    super(schema, codec, parent);
                }
                
                public Volume(
                        Identifier name,
                        ValueNode<ZNodeSchema> schema,
                        Serializers.ByteCodec<Object> codec,
                        NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                    super(name, schema, codec, parent);
                }

                public Volume(
                        Identifier name,
                        ValueNode<ZNodeSchema> schema,
                        Serializers.ByteCodec<Object> codec,
                        Records.ZNodeStatGetter stat,
                        long stamp,
                        NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                    super(name, schema, codec, stat, stamp, parent);
                }
                
                public Root getRoot() {
                    return (Root) get(Root.LABEL);
                }
                
                public Log getLog() {
                    return (Log) get(Log.LABEL);
                }
                
                @ZNode
                public static class Root extends StorageZNode<byte[]> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("root");

                    public static AbsoluteZNodePath pathOf(Identifier volume) {
                        return Volume.pathOf(volume).join(LABEL);
                    }
        
                    public Root(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<StorageZNode<?>> parent) {
                        super(schema, codec, parent);
                    }
                }
                
                @ZNode
                public static class Log extends StorageZNode<Void> implements NavigableMap<ZNodeName,StorageZNode<?>> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("log");

                    public static final AbsoluteZNodePath PATH = Volume.PATH.join(LABEL);

                    public static AbsoluteZNodePath pathOf(Identifier id) {
                        return Volume.pathOf(id).join(LABEL);
                    }
                    
                    public Log(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<StorageZNode<?>> parent) {
                        this(schema, codec, null, -1L, parent);
                    }

                    public Log(
                            ValueNode<ZNodeSchema> schema,
                            Serializers.ByteCodec<Object> codec,
                            Records.ZNodeStatGetter stat,
                            long stamp,
                            NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                        super(schema, codec, null, stat, stamp, parent, Maps.<ZNodeName,ZNodeName,StorageZNode<?>>newTreeMap(new VersionComparator()));
                    }

                    public Volume volume() {
                        return (Volume) parent().get();
                    }
                    
                    public Version version(UnsignedLong version) {
                        return (Version) get(ZNodeLabel.fromString(version.toString()));
                    }
                    
                    @SuppressWarnings("unchecked")
                    public NavigableMap<ZNodeName,Version> versions() {
                        return (NavigableMap<ZNodeName,Version>) (NavigableMap<?,?>) delegate();
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
                    public java.util.Map.Entry<ZNodeName, StorageZNode<?>> ceilingEntry(
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
                    public NavigableMap<ZNodeName, StorageZNode<?>> descendingMap() {
                        return delegate().descendingMap();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, StorageZNode<?>> firstEntry() {
                        return delegate().firstEntry();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, StorageZNode<?>> floorEntry(
                            ZNodeName key) {
                        return delegate().floorEntry(key);
                    }

                    @Override
                    public ZNodeName floorKey(ZNodeName key) {
                        return delegate().floorKey(key);
                    }

                    @Override
                    public SortedMap<ZNodeName, StorageZNode<?>> headMap(
                            ZNodeName toKey) {
                        return delegate().headMap(toKey);
                    }

                    @Override
                    public NavigableMap<ZNodeName, StorageZNode<?>> headMap(
                            ZNodeName toKey, boolean inclusive) {
                        return delegate().headMap(toKey, inclusive);
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, StorageZNode<?>> higherEntry(
                            ZNodeName key) {
                        return delegate().higherEntry(key);
                    }

                    @Override
                    public ZNodeName higherKey(ZNodeName key) {
                        return delegate().higherKey(key);
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, StorageZNode<?>> lastEntry() {
                        return delegate().lastEntry();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, StorageZNode<?>> lowerEntry(
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
                    public java.util.Map.Entry<ZNodeName, StorageZNode<?>> pollFirstEntry() {
                        return delegate().pollFirstEntry();
                    }

                    @Override
                    public java.util.Map.Entry<ZNodeName, StorageZNode<?>> pollLastEntry() {
                        return delegate().pollLastEntry();
                    }

                    @Override
                    public SortedMap<ZNodeName, StorageZNode<?>> subMap(
                            ZNodeName fromKey, ZNodeName toKey) {
                        return delegate().subMap(fromKey, toKey);
                    }

                    @Override
                    public NavigableMap<ZNodeName, StorageZNode<?>> subMap(
                            ZNodeName fromKey, boolean fromInclusive,
                            ZNodeName toKey, boolean toInclusive) {
                        return delegate().subMap(fromKey, fromInclusive, toKey, toInclusive);
                    }

                    @Override
                    public SortedMap<ZNodeName, StorageZNode<?>> tailMap(
                            ZNodeName fromKey) {
                        return delegate().tailMap(fromKey);
                    }

                    @Override
                    public NavigableMap<ZNodeName, StorageZNode<?>> tailMap(
                            ZNodeName fromKey, boolean inclusive) {
                        return delegate().tailMap(fromKey, inclusive);
                    }
                    
                    @Override
                    protected NavigableMap<ZNodeName, StorageZNode<?>> delegate() {
                        return (NavigableMap<ZNodeName, StorageZNode<?>>) super.delegate();
                    }
                    
                    public static class VersionComparator implements Comparator<ZNodeName> {

                        public VersionComparator() {}
                        
                        @Override
                        public int compare(final ZNodeName a, final ZNodeName b) {
                            UnsignedLong aLong;
                            try {
                                aLong = UnsignedLong.valueOf(a.toString());
                            } catch (NumberFormatException e) {
                                aLong = null;
                            }
                            UnsignedLong bLong;
                            try {
                                bLong = UnsignedLong.valueOf(b.toString());
                            } catch (NumberFormatException e) {
                                bLong = null;
                            }
                            if (aLong != null) {
                                if (bLong != null) {
                                    return aLong.compareTo(bLong);
                                } else {
                                    return 1;
                                }
                            } else {
                                if (bLong != null) {
                                    return -1;
                                } else {
                                    return a.toString().compareTo(b.toString());
                                }
                            }
                        }
                    }

                    @ZNode
                    public static class Version extends StorageZNode.NamedStorageNode<Void,UnsignedLong> {

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
                        
                        public static Version fromTrie(NameTrie<StorageZNode<?>> trie, Identifier id, UnsignedLong version) {
                            return (Version) trie.get(pathOf(id, version));
                        }

                        public Version(
                                ValueNode<ZNodeSchema> schema,
                                Serializers.ByteCodec<Object> codec,
                                NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                            super(UnsignedLong.valueOf(parent.name().toString()), schema, codec, parent);
                        }

                        public Log log() {
                            return (Log) parent().get();
                        }
                        
                        public Lease lease() {
                            return (Lease) get(Lease.LABEL);
                        }
                        
                        public Snapshot snapshot() {
                            return (Snapshot) get(Snapshot.LABEL);
                        }

                        public Xalpha xalpha() {
                            return (Xalpha) get(Xalpha.LABEL);
                        }

                        public Xomega xomega() {
                            return (Xomega) get(Xomega.LABEL);
                        }
                        
                        public VersionedId id() {
                            return VersionedId.valueOf(
                                    name(), 
                                    log().volume().name());
                        }
                        
                        @ZNode(dataType=UnsignedLong.class)
                        public static class Lease extends StorageZNode<UnsignedLong> {
    
                            @Name
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("lease");

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }
                            
                            public Lease(
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                                super(schema, codec, parent);
                            }

                            public Version version() {
                                return (Version) parent().get();
                            }
                        }

                        @ZNode(dataType=Long.class)
                        public static class Xalpha extends StorageZNode<Long> {
                            
                            @Name
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("xalpha");

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }
    
                            public Xalpha(
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                                super(schema, codec, parent);
                            }

                            public Version version() {
                                return (Version) parent().get();
                            }
                        }

                        @ZNode(dataType=Long.class)
                        public static class Xomega extends StorageZNode<Long> {
                            
                            @Name
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("xomega");

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }
    
                            public Xomega(
                                    ValueNode<ZNodeSchema> schema,
                                    Serializers.ByteCodec<Object> codec,
                                    NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                                super(schema, codec, parent);
                            }

                            public Version version() {
                                return (Version) parent().get();
                            }
                        }
                        
                        @ZNode
                        public static class Snapshot extends StorageZNode<Void> {

                            @Name
                            public static ZNodeLabel LABEL = ZNodeLabel.fromString("snapshot");

                            public static final AbsoluteZNodePath PATH = Version.PATH.join(LABEL);

                            public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                return Version.pathOf(id, version).join(LABEL);
                            }
                
                            public Snapshot(ValueNode<ZNodeSchema> schema,
                                    ByteCodec<Object> codec,
                                    Pointer<StorageZNode<?>> parent) {
                                super(schema, codec, parent);
                            }
                            
                            public Version version() {
                                return (Version) parent().get();
                            }
                            
                            public Commit commit() {
                                return (Commit) get(Commit.LABEL);
                            }
                            
                            public Ephemerals ephemerals() {
                                return (Ephemerals) get(Ephemerals.LABEL);
                            }
                            
                            public Watches watches() {
                                return (Watches) get(Watches.LABEL);
                            }

                            @ZNode(dataType=Boolean.class)
                            public static class Commit extends StorageZNode<Boolean> {
                                
                                @Name
                                public static ZNodeLabel LABEL = ZNodeLabel.fromString("commit");

                                public static final AbsoluteZNodePath PATH = Snapshot.PATH.join(LABEL);

                                public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                    return Snapshot.pathOf(id, version).join(LABEL);
                                }

                                public Commit(
                                        ValueNode<ZNodeSchema> schema,
                                        Serializers.ByteCodec<Object> codec,
                                        NameTrie.Pointer<? extends StorageZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                                
                                public Snapshot snapshot() {
                                    return (Snapshot) parent().get();
                                }
                            }
                            
                            @ZNode
                            public static class Ephemerals extends StorageZNode<Void> {

                                @Name
                                public static ZNodeLabel LABEL = ZNodeLabel.fromString("ephemerals");

                                public static final AbsoluteZNodePath PATH = Snapshot.PATH.join(LABEL);

                                public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                    return Snapshot.pathOf(id, version).join(LABEL);
                                }
                    
                                public Ephemerals(ValueNode<ZNodeSchema> schema,
                                        ByteCodec<Object> codec,
                                        Pointer<StorageZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                                
                                public Snapshot snapshot() {
                                    return (Snapshot) parent().get();
                                }
                                
                                @ZNode
                                public static class Session extends StorageZNode.SessionZNode<Void> {

                                    public static final AbsoluteZNodePath PATH = Ephemerals.PATH.join(ZNodeLabel.fromString(LABEL));

                                    public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version, long session) {
                                        return Ephemerals.pathOf(id, version).join(labelOf(session));
                                    }

                                    public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version, SessionIdHex session) {
                                        return Ephemerals.pathOf(id, version).join(labelOf(session));
                                    }
                                    
                                    public Session(ValueNode<ZNodeSchema> schema,
                                            ByteCodec<Object> codec,
                                            Pointer<StorageZNode<?>> parent) {
                                        super(schema, codec, parent);
                                    }
                                    
                                    @ZNode
                                    public static class Ephemeral extends StorageZNode.EscapedNamedZNode<byte[]> {

                                        public static final AbsoluteZNodePath PATH = Session.PATH.join(ZNodeLabel.fromString(LABEL));

                                        public Ephemeral(ValueNode<ZNodeSchema> schema,
                                                ByteCodec<Object> codec,
                                                Pointer<StorageZNode<?>> parent) {
                                            super(schema, codec, parent);
                                        }

                                        public Ephemeral(
                                                ZNodeName name,
                                                ValueNode<ZNodeSchema> schema,
                                                ByteCodec<Object> codec,
                                                Pointer<StorageZNode<?>> parent) {
                                            super(name, schema, codec, parent);
                                        }
                                        
                                        public Session session() {
                                            return (Session) parent().get();
                                        }
                                    }
                                }
                            }
                            
                            @ZNode
                            public static class Watches extends StorageZNode<Void> {

                                @Name
                                public static ZNodeLabel LABEL = ZNodeLabel.fromString("watches");

                                public static final AbsoluteZNodePath PATH = Snapshot.PATH.join(LABEL);

                                public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version) {
                                    return Snapshot.pathOf(id, version).join(LABEL);
                                }
                                
                                public Watches(ValueNode<ZNodeSchema> schema,
                                        ByteCodec<Object> codec,
                                        Pointer<StorageZNode<?>> parent) {
                                    super(schema, codec, parent);
                                }
                                
                                public Snapshot snapshot() {
                                    return (Snapshot) parent().get();
                                }
                                    
                                @ZNode
                                public static class Session extends StorageZNode.SessionZNode<Void> {

                                    public static final AbsoluteZNodePath PATH = Ephemerals.PATH.join(ZNodeLabel.fromString(LABEL));

                                    public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version, long session) {
                                        return Watches.pathOf(id, version).join(labelOf(session));
                                    }

                                    public static AbsoluteZNodePath pathOf(Identifier id, UnsignedLong version, SessionIdHex session) {
                                        return Watches.pathOf(id, version).join(labelOf(session));
                                    }
                                    
                                    public Session(ValueNode<ZNodeSchema> schema,
                                            ByteCodec<Object> codec,
                                            Pointer<StorageZNode<?>> parent) {
                                        super(schema, codec, parent);
                                    }

                                    @ZNode
                                    public static class Watch extends StorageZNode.EscapedNamedZNode<WatchType> {

                                        public static final AbsoluteZNodePath PATH = Session.PATH.join(ZNodeLabel.fromString(LABEL));
                                        
                                        public Watch(ValueNode<ZNodeSchema> schema,
                                                ByteCodec<Object> codec,
                                                Pointer<StorageZNode<?>> parent) {
                                            super(schema, codec, parent);
                                        }

                                        public Watch(
                                                ZNodeName name,
                                                ValueNode<ZNodeSchema> schema,
                                                ByteCodec<Object> codec,
                                                Pointer<StorageZNode<?>> parent) {
                                            super(name, schema, codec, parent);
                                        }
                                        
                                        public Session session() {
                                            return (Session) parent().get();
                                        }
                                    }
                                }
                            }
                        }      
                    }
                }
            }
        }

        @ZNode
        public static class Sessions extends StorageZNode<Void> {
    
            @Name
            public static ZNodeLabel LABEL = ZNodeLabel.fromString("sessions");

            public static final AbsoluteZNodePath PATH = Safari.PATH.join(LABEL);

            public static Sessions fromTrie(NameTrie<StorageZNode<?>> trie) {
                return (Sessions) trie.get(PATH);
            }
            
            public Sessions(ValueNode<ZNodeSchema> schema,
                    ByteCodec<Object> codec,
                    Pointer<StorageZNode<?>> parent) {
                super(schema, codec, parent);
            }
            
            @ZNode(createMode=CreateMode.EPHEMERAL, dataType=Session.Data.class)
            public static class Session extends StorageZNode.SessionZNode<Session.Data> {

                public static final AbsoluteZNodePath PATH = Sessions.PATH.join(ZNodeLabel.fromString(LABEL));

                public static AbsoluteZNodePath pathOf(long session) {
                    return Sessions.PATH.join(ZNodeLabel.fromString(SessionIdHex.toString(session)));
                }

                public static AbsoluteZNodePath pathOf(SessionIdHex session) {
                    return Sessions.PATH.join(ZNodeLabel.fromString(session.toString()));
                }
                
                public Session(ValueNode<ZNodeSchema> schema,
                        ByteCodec<Object> codec,
                        Pointer<StorageZNode<?>> parent) {
                    super(schema, codec, parent);
                }
                
                public static class Data extends AbstractPair<StorageZNode.SessionZNode.SessionIdHex, byte[]> {

                    public static Data valueOf(long sessionId, byte[] password) {
                        return new Data(StorageZNode.SessionZNode.SessionIdHex.valueOf(sessionId), password);
                    }
                    
                    @JsonCreator
                    public Data(
                            @JsonProperty("sessionId") SessionIdHex sessionId, 
                            @JsonProperty("password") byte[] password) {
                        super(sessionId, password);
                    }
                    
                    public SessionIdHex getSessionId() {
                        return first;
                    }
                    
                    public byte[] getPassword() {
                        return second;
                    }
                }
            }
        }
    }
}
