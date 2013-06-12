package edu.uw.zookeeper.orchestra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.util.AbstractPair;

public class BackendView extends AbstractPair<ServerInetAddressView, EnsembleView<ServerInetAddressView>> {

    public static BackendView of(
            ServerInetAddressView clientAddress,
            EnsembleView<ServerInetAddressView> ensemble) {
        return new BackendView(clientAddress, ensemble);
    }
    
    @JsonCreator
    public BackendView(
            @JsonProperty("clientAddress") ServerInetAddressView clientAddress,
            @JsonProperty("ensemble") EnsembleView<ServerInetAddressView> ensemble) {
        super(clientAddress, ensemble);
    }
    
    public ServerInetAddressView getClientAddress() {
        return first;
    }

    public EnsembleView<ServerInetAddressView> getEnsemble() {
        return second;
    }
}
