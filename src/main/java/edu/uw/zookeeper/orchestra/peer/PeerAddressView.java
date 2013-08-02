package edu.uw.zookeeper.orchestra.peer;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.orchestra.Identifier;

public class PeerAddressView extends AbstractPair<Identifier, ServerInetAddressView> {
    public static PeerAddressView of(Identifier first, ServerInetAddressView second) {
        return new PeerAddressView(first, second);
    }
    
    public PeerAddressView(Identifier first, ServerInetAddressView second) {
        super(first, second);
    }
    
    public Identifier id() {
        return first;
    }
    
    public ServerInetAddressView address() {
        return second;
    }
}
