package edu.uw.zookeeper.data;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;

import edu.uw.zookeeper.data.ZNodePath;

public final class ParentOfPath implements Function<ZNodePath, ZNodePath> {
    
    public static ParentOfPath create() {
        return new ParentOfPath();
    }
    
    protected ParentOfPath() {
    }
    
    @Override
    public ZNodePath apply(ZNodePath input) {
        return ((AbsoluteZNodePath) input).parent();
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }
}
