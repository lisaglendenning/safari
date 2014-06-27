package edu.uw.zookeeper.safari.storage.schema;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.WatchType;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.NameTrie;
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
                
                public Root root() {
                    return (Root) get(Root.LABEL);
                }
                
                public Snapshot snapshot() {
                    return (Snapshot) get(Snapshot.LABEL);
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
                public static class Snapshot extends StorageZNode<Void> {

                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("snapshot");

                    public static final AbsoluteZNodePath PATH = Volume.PATH.join(LABEL);

                    public static AbsoluteZNodePath pathOf(Identifier volume) {
                        return Volume.pathOf(volume).join(LABEL);
                    }
        
                    public Snapshot(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<StorageZNode<?>> parent) {
                        super(schema, codec, parent);
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

                        public static AbsoluteZNodePath pathOf(Identifier volume) {
                            return Snapshot.pathOf(volume).join(LABEL);
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

                        public static AbsoluteZNodePath pathOf(Identifier volume) {
                            return Snapshot.pathOf(volume).join(LABEL);
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

                            public static AbsoluteZNodePath pathOf(Identifier volume, long session) {
                                return Ephemerals.pathOf(volume).join(labelOf(session));
                            }

                            public static AbsoluteZNodePath pathOf(Identifier volume, SessionIdHex session) {
                                return Ephemerals.pathOf(volume).join(labelOf(session));
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

                        public static AbsoluteZNodePath pathOf(Identifier volume) {
                            return Snapshot.pathOf(volume).join(LABEL);
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

                            public static AbsoluteZNodePath pathOf(Identifier volume, long session) {
                                return Watches.pathOf(volume).join(labelOf(session));
                            }

                            public static AbsoluteZNodePath pathOf(Identifier volume, SessionIdHex session) {
                                return Watches.pathOf(volume).join(labelOf(session));
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

        @ZNode
        public static class Sessions extends StorageZNode<Void> {
    
            @Name
            public static ZNodeLabel LABEL = ZNodeLabel.fromString("sessions");

            public static final AbsoluteZNodePath PATH = Safari.PATH.join(LABEL);
           
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
