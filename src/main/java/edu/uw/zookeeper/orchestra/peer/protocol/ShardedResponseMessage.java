package edu.uw.zookeeper.orchestra.peer.protocol;

import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

@JsonIgnoreProperties({"response", "xid", "zxid", "record"})
public class ShardedResponseMessage<V extends Records.Response> extends EncodableMessage<Message.ServerResponse<V>> implements Message.ServerResponse<V>, ShardedOperation.Response<Message.ServerResponse<V>> {

    public static <V extends Records.Response> ShardedResponseMessage<V> of(
            Identifier identifier, Message.ServerResponse<V> response) {
        return new ShardedResponseMessage<V>(identifier, response);
    }

    @SuppressWarnings("unchecked")
    @JsonCreator
    public ShardedResponseMessage(
            @JsonProperty("identifier") Identifier identifier,
            @JsonProperty("opCode") int opCode,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(identifier, (Message.ServerResponse<V>) ProtocolResponseMessage.decode(OpCode.of(opCode), Unpooled.wrappedBuffer(payload)));
    }

    private final Identifier identifier;
    
    public ShardedResponseMessage(Identifier identifier, Message.ServerResponse<V> response) {
        super(response);
        this.identifier = identifier;
    }

    @Override
    public Identifier getIdentifier() {
        return identifier;
    }

    @Override
    public Message.ServerResponse<V> getResponse() {
        return delegate();
    }
    
    @Override
    public int getXid() {
        return getResponse().getXid();
    }

    @Override
    public long getZxid() {
        return getResponse().getZxid();
    }

    @Override
    public V getRecord() {
        return getResponse().getRecord();
    }

    public int getOpCode() {
        return getRecord().getOpcode().intValue();
    }
}
