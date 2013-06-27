package edu.uw.zookeeper.orchestra;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.util.AbstractPair;

public class ConductorAddressView extends AbstractPair<Identifier, ServerInetAddressView> {
    public static ConductorAddressView of(Identifier first, ServerInetAddressView second) {
        return new ConductorAddressView(first, second);
    }
    
    public ConductorAddressView(Identifier first, ServerInetAddressView second) {
        super(first, second);
    }
    
    public Identifier id() {
        return first;
    }
    
    public ServerInetAddressView address() {
        return second;
    }
}
