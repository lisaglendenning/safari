package edu.uw.zookeeper.data;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;

public final class JoinToPath implements Function<ZNodePath, ZNodePath> {
    
    public static JoinToPath forName(ZNodeName name) {
        return new JoinToPath(name);
    }
    
    private final ZNodeName name;

    protected JoinToPath(ZNodeName name) {
        this.name = name;
    }
    
    @Override
    public ZNodePath apply(ZNodePath input) {
        return input.join(name);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(name).toString();
    }
}
