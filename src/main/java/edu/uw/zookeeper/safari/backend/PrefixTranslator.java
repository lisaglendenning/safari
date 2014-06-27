package edu.uw.zookeeper.safari.backend;

import com.google.common.base.Function;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;

public class PrefixTranslator extends AbstractPair<ZNodePath, ZNodePath> implements Function<ZNodePath,ZNodePath> {

    public static PrefixTranslator forPrefix(ZNodePath fromPrefix, ZNodePath toPrefix) {
        return new PrefixTranslator(fromPrefix, toPrefix);
    }
    
    protected PrefixTranslator(ZNodePath fromPrefix, ZNodePath toPrefix) {
        super(fromPrefix, toPrefix);
    }
    
    public final ZNodePath from() {
        return first;
    }
    
    public final ZNodePath to() {
        return second;
    }

    @Override
    public ZNodePath apply(ZNodePath input) {
        return join(input.suffix(from()));
    }
    
    protected ZNodePath join(ZNodeName remaining) {
        return to().join(remaining);
    }
}
