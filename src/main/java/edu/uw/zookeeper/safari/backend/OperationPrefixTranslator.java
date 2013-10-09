package edu.uw.zookeeper.safari.backend;

import java.util.List;

import com.google.common.collect.Lists;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;

public class OperationPrefixTranslator extends AbstractPair<RecordPrefixTranslator<Records.Request>, RecordPrefixTranslator<Records.Response>> {

    public static OperationPrefixTranslator create(
            ZNodeLabel.Path fromPrefix, ZNodeLabel.Path toPrefix) {
        return create(
                RecordPrefixTranslator.<Records.Request>of(fromPrefix, toPrefix),
                RecordPrefixTranslator.<Records.Response>of(toPrefix, fromPrefix));
    }

    public static OperationPrefixTranslator create(
            RecordPrefixTranslator<Records.Request> forward, 
            RecordPrefixTranslator<Records.Response> reverse) {
        return new OperationPrefixTranslator(forward, reverse);
    }
        
    public OperationPrefixTranslator(
            RecordPrefixTranslator<Records.Request> forward, 
            RecordPrefixTranslator<Records.Response> reverse) {
        super(forward, reverse);
    }
    
    public RecordPrefixTranslator<Records.Request> forward() {
        return first;
    }
    
    public RecordPrefixTranslator<Records.Response> reverse() {
        return second;
    }

    public Message.ClientRequest<?> apply(Message.ClientRequest<?> unsharded) {
        Records.Request record = unsharded.record();
        Records.Request translated = apply(record);
        Message.ClientRequest<?> sharded;
        if (translated == record) {
            sharded = unsharded;
        } else {
            sharded = ProtocolRequestMessage.of(
                    unsharded.xid(), translated);
        }
        return sharded;
    }

    public Records.Request apply(Records.Request unsharded) {
        Records.Request output = unsharded;
        if (unsharded instanceof IMultiRequest) {
            List<Records.MultiOpRequest> ops = Lists.newArrayListWithExpectedSize(((IMultiRequest) unsharded).size());
            for (Records.MultiOpRequest e: (IMultiRequest) unsharded) {
                ops.add((Records.MultiOpRequest) forward().apply(e));
            }
            output = new IMultiRequest(ops);
        } else {
            output = forward().apply(unsharded);
        }
        return output;
    }
    
    public Message.ServerResponse<?> apply(Message.ServerResponse<?> sharded) {
        Records.Response record = sharded.record();
        Records.Response translated = apply(record);
        Message.ServerResponse<?> unsharded;
        if (translated == record) {
            unsharded = sharded;
        } else {
            unsharded = ProtocolResponseMessage.of(
                    sharded.xid(), sharded.zxid(), translated);
        }
        return unsharded;
    }
    
    public Records.Response apply(Records.Response sharded) {
        Records.Response output = sharded;
        if (sharded instanceof IMultiResponse) {
            List<Records.MultiOpResponse> ops = Lists.newArrayListWithExpectedSize(((IMultiResponse) sharded).size());
            for (Records.MultiOpResponse e: (IMultiResponse) sharded) {
                ops.add((Records.MultiOpResponse) reverse().apply(e));
            }
            output = new IMultiResponse(ops);
        } else {
            output = reverse().apply(sharded);
        }
        return output;
    }
}
