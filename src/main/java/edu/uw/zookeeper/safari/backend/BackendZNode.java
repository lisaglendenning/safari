package edu.uw.zookeeper.safari.backend;

import java.util.Map;

import com.google.common.base.Converter;

import edu.uw.zookeeper.common.Hex;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NameType;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.Serializes;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.data.SafariZNode;


public abstract class BackendZNode<V> extends SafariZNode<BackendZNode<?>,V> {

    protected BackendZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            NameTrie.Pointer<? extends BackendZNode<?>> parent) {
        this(schema, codec, null, null, -1L, parent);
    }

    protected BackendZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            V data,
            Records.ZNodeStatGetter stat,
            long stamp,
            NameTrie.Pointer<? extends BackendZNode<?>> parent) {
        super(schema, codec, data, stat, stamp, parent);
    }

    protected BackendZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            V data,
            Records.ZNodeStatGetter stat,
            long stamp,
            NameTrie.Pointer<? extends BackendZNode<?>> parent,
            Map<ZNodeName, BackendZNode<?>> children) {
        super(schema, codec, data, stat, stamp, parent, children);
    }

    public static abstract class BackendNamedZNode<V,T> extends BackendZNode<V> {

        protected final T name;

        protected BackendNamedZNode(
                T name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            this(name, schema, codec, null, null, -1L, parent);
        }

        protected BackendNamedZNode(
                T name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                V data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            super(schema, codec, data, stat, stamp, parent);
            this.name = name;
        }

        public T name() {
            return name;
        }
    }
    
    public static abstract class IdentifierZNode extends BackendNamedZNode<Void,Identifier> {

        @Name(type=NameType.PATTERN)
        public static final String LABEL = Identifier.PATTERN;
        
        protected IdentifierZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            this(Identifier.valueOf(parent.name().toString()), schema, codec, parent);
        }
        
        protected IdentifierZNode(
                Identifier name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            this(name, schema, codec, null, -1L, parent);
        }

        protected IdentifierZNode(
                Identifier name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            super(name, schema, codec, null, stat, stamp, parent);
        }
    }

    public static abstract class SessionZNode<V> extends BackendNamedZNode<V,SessionZNode.SessionIdHex> {
        
        public static final class SessionIdHex {

            @Serializes(from=String.class, to=SessionIdHex.class)
            public static SessionIdHex valueOf(String string) {
                return valueOf(Hex.parseLong(string));
            }
            
            public static SessionIdHex valueOf(long value) {
                return new SessionIdHex(value);
            }
            
            public static String toString(long value) {
                return Hex.toPaddedHexString(value);
            }

            private final long value;
            
            protected SessionIdHex(long value) {
                this.value = value;
            }
            
            public long longValue() {
                return value;
            }
            
            @Serializes(from=SessionIdHex.class, to=String.class)
            @Override
            public String toString() {
                return toString(longValue());
            }
        }

        @Name(type=NameType.PATTERN)
        public static final String LABEL = "[0-9a-f]+";
        
        protected SessionZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            this(SessionIdHex.valueOf(parent.name().toString()), schema, codec, parent);
        }
        
        protected SessionZNode(
                SessionIdHex name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            this(name, schema, codec, null, -1L, parent);
        }

        protected SessionZNode(
                SessionIdHex name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            super(name, schema, codec, null, stat, stamp, parent);
        }
    }
    
    public static abstract class EscapedNamedZNode<V> extends BackendNamedZNode<V,ZNodeName> {

        @Name(type=NameType.PATTERN)
        public static final String LABEL = ".+";
        
        public static class EscapedConverter extends Converter<ZNodeName, ZNodeName> {

            @Override
            protected ZNodeName doForward(ZNodeName input) {
                StringBuilder sb = new StringBuilder(input.length()+1);
                sb.append('\\');
                for (int i=0; i<input.length(); ++i) {
                    char c = input.charAt(i);
                    switch (c) {
                    case '/':
                        sb.append('\\');
                        break;
                    case '\\':
                        sb.append('\\').append('\\');
                        break;
                    default:
                        sb.append(c);
                        break;
                    }
                }
                if (sb.length() == 0) {
                    // special case to avoid empty label
                    sb.append('\\');
                }
                return ZNodeName.fromString(sb.toString());
            }

            @Override
            protected ZNodeName doBackward(ZNodeName input) {
                StringBuilder sb = new StringBuilder(input.length());
                for (int i=0; i<input.length(); ++i) {
                    char c = input.charAt(i);
                    switch (c) {
                    case '\\':
                        if (i+1 < input.length()) {
                            if (input.charAt(i+1) == '\\') {
                                sb.append('\\');
                                ++i;
                            } else {
                                sb.append('/');
                            }
                        } else {
                            assert (i == 0);
                            // special case for root path
                        }
                        break;
                    default:
                        sb.append(c);
                        break;
                    }
                }
                return ZNodeName.fromString(sb.toString());
            }
        }

        protected static final EscapedConverter CONVERTER = new EscapedConverter();
        
        public static EscapedConverter converter() {
            return CONVERTER;
        }
        
        protected EscapedNamedZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            this(CONVERTER.reverse().convert(parent.name()), schema, codec, parent);
        }
        
        protected EscapedNamedZNode(
                ZNodeName name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            this(name, schema, codec, null, -1L, parent);
        }

        protected EscapedNamedZNode(
                ZNodeName name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends BackendZNode<?>> parent) {
            super(name, schema, codec, null, stat, stamp, parent);
        }
    }
}
