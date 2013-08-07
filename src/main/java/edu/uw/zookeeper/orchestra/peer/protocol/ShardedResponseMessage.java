package edu.uw.zookeeper.orchestra.peer.protocol;

import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

@JsonIgnoreProperties({"value", "response", "xid", "zxid", "record"})
public class ShardedResponseMessage<V extends Records.Response> extends ShardedMessage<Message.ServerResponse<V>> implements Message.ServerResponse<V>, ShardedOperation.Response<Message.ServerResponse<V>> {

    public static <V extends Records.Response> ShardedResponseMessage<V> of(
            Identifier identifier, Message.ServerResponse<V> message) {
        return new ShardedResponseMessage<V>(identifier, message);
    }

    @SuppressWarnings("unchecked")
    @JsonCreator
    public ShardedResponseMessage(
            @JsonProperty("identifier") Identifier identifier,
            @JsonProperty("opCode") int opCode,
            @JsonProperty("payload") byte[] payload) throws IOException {
        this(identifier, (Message.ServerResponse<V>) ProtocolResponseMessage.decode(OpCode.of(opCode), Unpooled.wrappedBuffer(payload)));
    }

    public ShardedResponseMessage(Identifier identifier, Message.ServerResponse<V> message) {
        super(identifier, message);
    }

    @Override
    public Message.ServerResponse<V> getResponse() {
        return getValue();
    }
    
    @Override
    public int getXid() {
        return getValue().getXid();
    }

    @Override
    public long getZxid() {
        return getValue().getZxid();
    }

    @Override
    public V getRecord() {
        return getValue().getRecord();
    }

    public int getOpCode() {
        return getRecord().getOpcode().intValue();
    }
}
