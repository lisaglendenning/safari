package edu.uw.zookeeper.safari.backend;

import com.google.common.base.Function;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;

public final class PrefixTranslator extends AbstractPair<ZNodePath, ZNodePath> implements Function<ZNodePath,ZNodePath> {

    public static PrefixTranslator forPrefix(ZNodePath fromPrefix, ZNodePath toPrefix) {
        return new PrefixTranslator(fromPrefix, toPrefix);
    }
    
    public PrefixTranslator(ZNodePath fromPrefix, ZNodePath toPrefix) {
        super(fromPrefix, toPrefix);
    }
    
    public ZNodePath getFromPrefix() {
        return first;
    }
    
    public ZNodePath getToPrefix() {
        return second;
    }

    @Override
    public ZNodePath apply(ZNodePath input) {
        assert (input.startsWith(getFromPrefix()));
        ZNodeName remaining = input.suffix(first.isRoot() ? 0 : first.length());
        return second.join(remaining);
    }
}
