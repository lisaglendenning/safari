package edu.uw.zookeeper.orchestra.backend;

import java.util.List;

import com.google.common.collect.Lists;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractPair;

public class OperationPrefixTranslator extends AbstractPair<RecordPrefixTranslator<Records.Request>, RecordPrefixTranslator<Records.Response>> {

    public static OperationPrefixTranslator of(ZNodeLabel.Path fromPrefix, ZNodeLabel.Path toPrefix) {
        return new OperationPrefixTranslator(fromPrefix, toPrefix);
    }
    
    public OperationPrefixTranslator(ZNodeLabel.Path fromPrefix, ZNodeLabel.Path toPrefix) {
        super(RecordPrefixTranslator.<Records.Request>of(fromPrefix, toPrefix),
                RecordPrefixTranslator.<Records.Response>of(toPrefix, fromPrefix));
    }
    
    public RecordPrefixTranslator<Records.Request> forward() {
        return first;
    }
    
    public RecordPrefixTranslator<Records.Response> reverse() {
        return second;
    }

    public Records.Request apply(Records.Request input) {
        Records.Request output = input;
        if (input instanceof IMultiRequest) {
            List<Records.MultiOpRequest> ops = Lists.newArrayListWithExpectedSize(((IMultiRequest) input).size());
            for (Records.MultiOpRequest e: (IMultiRequest) input) {
                ops.add((Records.MultiOpRequest) forward().apply(e));
            }
            output = new IMultiRequest(ops);
        } else {
            output = forward().apply(input);
        }
        return output;
    }
    
    public Records.Response apply(Records.Response input) {
        Records.Response output = input;
        if (input instanceof IMultiResponse) {
            List<Records.MultiOpResponse> ops = Lists.newArrayListWithExpectedSize(((IMultiResponse) input).size());
            for (Records.MultiOpResponse e: (IMultiResponse) input) {
                ops.add((Records.MultiOpResponse) reverse().apply(e));
            }
            output = new IMultiResponse(ops);
        } else {
            output = reverse().apply(input);
        }
        return output;
    }
}
