package edu.uw.zookeeper.orchestra.backend;

import com.google.common.base.Function;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.Records;

public class RecordPrefixTranslator<T extends Records.Coded> extends AbstractPair<ZNodeLabel.Path, ZNodeLabel.Path> implements Function<T,T> {

    public static <T extends Records.Coded> RecordPrefixTranslator<T> none() {
        return new NoPrefix<T>();
    }
    
    public static <T extends Records.Coded> RecordPrefixTranslator<T> of(ZNodeLabel.Path fromPrefix, ZNodeLabel.Path toPrefix) {
        return new RecordPrefixTranslator<T>(fromPrefix, toPrefix);
    }
    
    public RecordPrefixTranslator(ZNodeLabel.Path fromPrefix, ZNodeLabel.Path toPrefix) {
        super(fromPrefix, toPrefix);
    }
    
    public ZNodeLabel.Path getFromPrefix() {
        return first;
    }
    
    public ZNodeLabel.Path getToPrefix() {
        return second;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T apply(T input) {
        T output = input;
        if (input instanceof Records.PathGetter) {
            Operations.PathBuilder<?,?> builder = (Operations.PathBuilder<?,?>) Operations.fromRecord(input);
            ZNodeLabel.Path path = builder.getPath();
            if (getFromPrefix().prefixOf(path)) {
                int prefixLen = getFromPrefix().length();
                ZNodeLabel.Path transformed;
                if (path.length() == prefixLen) {
                    transformed = getToPrefix();
                } else {
                    String remaining = path.toString().substring(prefixLen);
                    transformed = ZNodeLabel.Path.joined(getToPrefix().toString(), remaining);
                }
                builder.setPath(transformed);
                output = (T) builder.build();
            } else {
                throw new IllegalArgumentException(String.format("%s not a prefix of %s", getFromPrefix(), path));
            }
        }
        return output;
    }
    
    public static class NoPrefix<T extends Records.Coded> extends RecordPrefixTranslator<T> {

        public NoPrefix() {
            super(ZNodeLabel.Path.root(), ZNodeLabel.Path.root());
        }

        @Override
        public T apply(T input) {
            return input;
        }
    }
}
