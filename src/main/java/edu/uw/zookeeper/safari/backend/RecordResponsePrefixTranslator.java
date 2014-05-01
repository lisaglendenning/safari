package edu.uw.zookeeper.safari.backend;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;

public final class RecordResponsePrefixTranslator implements Function<Records.Response, Records.Response> {

    public static RecordResponsePrefixTranslator forPrefix(
            ZNodePath fromPrefix, ZNodePath toPrefix) {
        return new RecordResponsePrefixTranslator(
                RecordPrefixTranslator.<Records.Response>forPrefix(fromPrefix, toPrefix));
    }

    private final RecordPrefixTranslator<Records.Response> delegate;
        
    public RecordResponsePrefixTranslator( 
            RecordPrefixTranslator<Records.Response> delegate) {
        this.delegate = delegate;
    }
    
    public PrefixTranslator getPrefix() {
        return delegate.getPrefix();
    }

    @Override
    public Records.Response apply(Records.Response input) {
        final Records.Response output;
        if (input instanceof IMultiResponse) {
            IMultiResponse multi = (IMultiResponse) input;
            List<Records.MultiOpResponse> ops = Lists.newArrayListWithCapacity(multi.size());
            for (Records.MultiOpResponse e: multi) {
                ops.add((Records.MultiOpResponse) delegate.apply(e));
            }
            output = new IMultiResponse(ops);
        } else {
            output = delegate.apply(input);
        }
        return output;
    }
}
