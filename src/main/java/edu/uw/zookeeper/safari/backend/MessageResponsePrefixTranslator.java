package edu.uw.zookeeper.safari.backend;

import com.google.common.base.Function;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.Records;

public final class MessageResponsePrefixTranslator implements Function<Message.ServerResponse<?>,Message.ServerResponse<?>> {

    public static MessageResponsePrefixTranslator forPrefix(
            ZNodePath fromPrefix, ZNodePath toPrefix) {
        return new MessageResponsePrefixTranslator(
                RecordResponsePrefixTranslator.forPrefix(fromPrefix, toPrefix));
    }

    private final RecordResponsePrefixTranslator delegate;
        
    public MessageResponsePrefixTranslator(RecordResponsePrefixTranslator delegate) {
        this.delegate = delegate;
    }

    public PrefixTranslator getPrefix() {
        return delegate.getPrefix();
    }

    @Override
    public Message.ServerResponse<?> apply(Message.ServerResponse<?> input) {
        final Records.Response sharded = input.record();
        final Records.Response unsharded = delegate.apply(sharded);
        final Message.ServerResponse<?> output = (sharded == unsharded) ? input :
            ProtocolResponseMessage.of(input.xid(), input.zxid(), unsharded);
        return output;
    }
}
