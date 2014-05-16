package edu.uw.zookeeper.safari.storage;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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

    public StorageSchema(ValueNode<ZNodeSchema> schema,
            ByteCodec<Object> codec) {
        super(schema, codec, SimpleNameTrie.<StorageZNode<?>>rootPointer());
    }

    @ZNode
    public static class Safari extends StorageZNode<Void> {

        @Name
        public static final ZNodeLabel LABEL = ZNodeLabel.fromString("safari");

        public static final AbsoluteZNodePath PATH = (AbsoluteZNodePath) ZNodePath.root().join(LABEL);
       
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
            public static class Volume extends StorageZNode.IdentifierZNode {

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
                
                @ZNode
                public static class Root extends StorageZNode<byte[]> {
    
                    @Name
                    public static ZNodeLabel LABEL = ZNodeLabel.fromString("root");

                    public static AbsoluteZNodePath pathOf(Identifier region) {
                        return Volume.pathOf(region).join(LABEL);
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

                    public static AbsoluteZNodePath pathOf(Identifier volume) {
                        return Volume.pathOf(volume).join(LABEL);
                    }
        
                    public Snapshot(ValueNode<ZNodeSchema> schema,
                            ByteCodec<Object> codec,
                            Pointer<StorageZNode<?>> parent) {
                        super(schema, codec, parent);
                    }

                    @ZNode
                    public static class Ephemerals extends StorageZNode<Void> {

                        @Name
                        public static ZNodeLabel LABEL = ZNodeLabel.fromString("ephemerals");

                        public Ephemerals(ValueNode<ZNodeSchema> schema,
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
                            public static class Ephemeral extends StorageZNode.EscapedNamedZNode<byte[]> {

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
                            }
                        }
                    }
                    
                    @ZNode
                    public static class Watches extends StorageZNode<Void> {

                        @Name
                        public static ZNodeLabel LABEL = ZNodeLabel.fromString("watches");

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
                            public static class Watch extends StorageZNode.EscapedNamedZNode<byte[]> {

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
