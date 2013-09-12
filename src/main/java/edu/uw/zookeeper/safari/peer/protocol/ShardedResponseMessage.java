package edu.uw.zookeeper.safari.peer.protocol;

import io.netty.buffer.Unpooled;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;

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
    public int xid() {
        return getValue().xid();
    }

    @Override
    public long zxid() {
        return getValue().zxid();
    }

    @Override
    public V record() {
        return getValue().record();
    }

    public int getOpCode() {
        return record().opcode().intValue();
    }
}