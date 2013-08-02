package edu.uw.zookeeper.orchestra.backend;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.GetEvent;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.intravm.EndpointFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.VolumeDescriptor;
import edu.uw.zookeeper.orchestra.VolumeCache;
import edu.uw.zookeeper.orchestra.control.Hash;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class ShardedClientConnectionExecutorTest {
    
    @Test
    public void test() throws InterruptedException, ExecutionException {
        
        final VolumeCache volumes = VolumeCache.newInstance();
        Function<ZNodeLabel.Path, Identifier> lookup = new Function<ZNodeLabel.Path, Identifier>() {
            @Override
            public Identifier apply(ZNodeLabel.Path input) {
                return volumes.get(input).getId();
            }
        };
        VolumeShardedOperationTranslators translator = new VolumeShardedOperationTranslators(
                new Function<Identifier, Volume>() {
                    @Override
                    public Volume apply(Identifier input) {
                        return volumes.get(input);
                    }
                });
        
        SortedSet<ZNodeLabel> leaves = Sets.newTreeSet(ImmutableList.of(ZNodeLabel.of("v1"), ZNodeLabel.of("v2")));
        ZNodeLabel.Path path = ZNodeLabel.Path.root();
        Volume volume = Volume.of(Hash.default32().apply(path.toString()).asIdentifier(), VolumeDescriptor.of(path, leaves));
        volumes.put(volume);
        for (ZNodeLabel leaf: volume.getDescriptor().getLeaves()) {
            path = ZNodeLabel.Path.of(volume.getDescriptor().getRoot(), leaf);
            volumes.put(Volume.of(Hash.default32().apply(path.toString()).asIdentifier(), VolumeDescriptor.of(path)));
        }
        
        EndpointFactory<InetSocketAddress> endpointFactory = EndpointFactory.defaults();
        Pair<IntraVmConnection<InetSocketAddress>, IntraVmConnection<InetSocketAddress>> connections = IntraVmConnection.createPair(endpointFactory.get());
        Session session = Session.create(1, Session.Parameters.uninitialized());
        ShardedClientConnectionExecutor<IntraVmConnection<InetSocketAddress>> client = ShardedClientConnectionExecutor.newInstance(
                translator, 
                lookup, 
                Futures.immediateFuture(ConnectMessage.Response.Valid.newInstance(session)), 
                connections.first());
        
        GetEvent<Message.ClientRequest<?>> requestEvent = GetEvent.newInstance(connections.second());
        ListenableFuture<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> clientFuture = client.submit(Operations.Requests.getChildren().setPath(volume.getDescriptor().getRoot()).build());
        connections.second().read();
        assertEquals(VolumeShardedOperationTranslators.rootOf(volume.getId()).toString(), ((Records.PathGetter) requestEvent.get().getRecord()).getPath());

        Long zxid = 1L;
        Message.ServerResponse<Records.Response> response = ProtocolResponseMessage.of(requestEvent.get().getXid(), zxid, Operations.Responses.getChildren().build());
        ListenableFuture<Message.ServerResponse<Records.Response>> serverFuture = connections.second().write(response);
        connections.first().read();
        serverFuture.get();
        assertEquals(volume.getId(), ((ShardedRequestMessage<?>) clientFuture.get().first()).getIdentifier());
        assertEquals(volume.getId(), ((ShardedResponseMessage<?>) clientFuture.get().second()).getIdentifier());
        
        volume = volumes.get(ZNodeLabel.Path.of("/v1"));
        requestEvent = GetEvent.newInstance(connections.second());
        clientFuture = client.submit(Operations.Requests.create().setPath(volume.getDescriptor().getRoot()).build());
        connections.second().read();
        assertEquals(VolumeShardedOperationTranslators.rootOf(volume.getId()).toString(), ((Records.PathGetter) requestEvent.get().getRecord()).getPath());

        zxid += 1L;
        response = ProtocolResponseMessage.of(requestEvent.get().getXid(), zxid, Operations.Responses.create().setPath(ZNodeLabel.Path.of(((Records.PathGetter) requestEvent.get().getRecord()).getPath())).build());
        serverFuture = connections.second().write(response);
        connections.first().read();
        serverFuture.get();
        assertEquals(volume.getId(), ((ShardedRequestMessage<?>) clientFuture.get().first()).getIdentifier());
        assertEquals(volume.getId(), ((ShardedResponseMessage<?>) clientFuture.get().second()).getIdentifier());

    }
}
