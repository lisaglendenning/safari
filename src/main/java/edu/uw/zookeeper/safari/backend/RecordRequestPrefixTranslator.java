package edu.uw.zookeeper.safari.backend;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public final class RecordRequestPrefixTranslator implements Function<Records.Request, Records.Request> {

    public static RecordRequestPrefixTranslator forPrefix(
            ZNodePath fromPrefix, ZNodePath toPrefix) {
        return new RecordRequestPrefixTranslator(
                RecordPrefixTranslator.<Records.Request>forPrefix(fromPrefix, toPrefix));
    }

    private final RecordPrefixTranslator<Records.Request> delegate;
        
    public RecordRequestPrefixTranslator(
            RecordPrefixTranslator<Records.Request> delegate) {
        this.delegate = delegate;
    }
    
    public PrefixTranslator getPrefix() {
        return delegate.getPrefix();
    }

    @Override
    public Records.Request apply(Records.Request input) {
        final Records.Request output;
        if (input instanceof IMultiRequest) {
            IMultiRequest multi = (IMultiRequest) input;
            List<Records.MultiOpRequest> ops = Lists.newArrayListWithCapacity(multi.size());
            for (Records.MultiOpRequest e: multi) {
                ops.add((Records.MultiOpRequest) delegate.apply(e));
            }
            output = new IMultiRequest(ops);
        } else {
            output = delegate.apply(input);
        }
        return output;
    }
}
