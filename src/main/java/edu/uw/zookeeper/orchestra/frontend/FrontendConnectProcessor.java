package edu.uw.zookeeper.orchestra.frontend;

import java.util.Map;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.peer.EnsemblePeerService;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpen;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.server.ConnectListenerProcessor;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;

public class FrontendConnectProcessor extends ConnectListenerProcessor {

    public static FrontendConnectProcessor newInstance(
            EnsemblePeerService peers,
            Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate,
            Map<Long, Publisher> listeners) {
        return new FrontendConnectProcessor(peers, delegate, listeners);
    }
    
    protected final EnsemblePeerService peers;
    
    public FrontendConnectProcessor(
            EnsemblePeerService peers,
            Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate,
            Map<Long, Publisher> listeners) {
        super(delegate, listeners);
        this.peers = peers;
    }

    @Override
    protected ConnectMessage.Response apply(Pair<ConnectMessage.Request, Publisher> input, ConnectMessage.Response output) {
        if (output instanceof ConnectMessage.Response.Valid) {
            MessagePacket message = MessagePacket.of(MessageSessionOpen.of(output.getSessionId(), output.getTimeOut()));
            for (Identifier ensemble: peers) {
                try {
                    ClientPeerConnection connection = peers.getConnectionForEnsemble(ensemble);
                    connection.write(message);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    throw Throwables.propagate(e);
                }
            }
        }
        
        return super.apply(input, output);
    }

}
