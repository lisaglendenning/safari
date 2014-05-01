package edu.uw.zookeeper.safari.backend;

import com.google.common.base.Function;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.Records;

public final class MessageRequestPrefixTranslator implements Function<Message.ClientRequest<?>,Message.ClientRequest<?>> {

    public static MessageRequestPrefixTranslator forPrefix(
            ZNodePath fromPrefix, ZNodePath toPrefix) {
        return new MessageRequestPrefixTranslator(
                RecordRequestPrefixTranslator.forPrefix(fromPrefix, toPrefix));
    }

    private final RecordRequestPrefixTranslator delegate;
        
    public MessageRequestPrefixTranslator(RecordRequestPrefixTranslator delegate) {
        this.delegate = delegate;
    }
    
    public PrefixTranslator getPrefix() {
        return delegate.getPrefix();
    }

    @Override
    public Message.ClientRequest<?> apply(Message.ClientRequest<?> input) {
        final Records.Request unsharded = input.record();
        final Records.Request sharded = delegate.apply(unsharded);
        final Message.ClientRequest<?> output = (unsharded == sharded) ? input : 
            ProtocolRequestMessage.of(input.xid(), sharded);
        return output;
    }
}
