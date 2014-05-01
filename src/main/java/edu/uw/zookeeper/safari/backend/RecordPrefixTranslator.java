package edu.uw.zookeeper.safari.backend;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;

public final class RecordPrefixTranslator<V extends Records.Coded> implements Function<V,V> {

    public static <V extends Records.Coded> RecordPrefixTranslator<V> forPrefix(ZNodePath fromPrefix, ZNodePath toPrefix) {
        return new RecordPrefixTranslator<V>(PrefixTranslator.forPrefix(fromPrefix, toPrefix));
    }
    
    private final PrefixTranslator delegate;
    
    public RecordPrefixTranslator(PrefixTranslator delegate) {
        this.delegate = delegate;
    }
    
    public final PrefixTranslator getPrefix() {
        return delegate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V apply(V input) {
        final V output;
        if (input instanceof Records.PathGetter) {
            Operations.PathBuilder<?,?> builder = (Operations.PathBuilder<?,?>) Operations.fromRecord(input);
            builder.setPath(delegate.apply(builder.getPath()));
            output = (V) builder.build();
        } else {
            output = input;
        }
        return output;
    }
}
