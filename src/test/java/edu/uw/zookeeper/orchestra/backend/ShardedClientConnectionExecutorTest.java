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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.GetEvent;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.intravm.IntraVmEndpointFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.control.Hash;
import edu.uw.zookeeper.orchestra.data.Volume;
import edu.uw.zookeeper.orchestra.data.VolumeCache;
import edu.uw.zookeeper.orchestra.data.VolumeDescriptor;
import edu.uw.zookeeper.orchestra.net.SimpleClient;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.SimpleServer;

@RunWith(JUnit4.class)
public class ShardedClientConnectionExecutorTest {
    
    public static class Module extends SimpleClient {
        
        @Provides @Singleton
        public VolumeCache getVolumes() {
            return VolumeCache.newInstance();
        }
    }
    
    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = Guice.createInjector(new Module());
        VolumeCache volumes = injector.getInstance(VolumeCache.class);
        
        Function<ZNodeLabel.Path, Identifier> lookup = BackendRequestService.newVolumePathLookup(volumes);
        VolumeShardedOperationTranslators translator = new VolumeShardedOperationTranslators(
                BackendRequestService.newVolumeIdLookup(volumes));
        
        SortedSet<ZNodeLabel> leaves = Sets.newTreeSet(ImmutableList.of(ZNodeLabel.of("v1"), ZNodeLabel.of("v2")));
        ZNodeLabel.Path path = ZNodeLabel.Path.root();
        Volume volume = Volume.of(Hash.default32().apply(path.toString()).asIdentifier(), VolumeDescriptor.of(path, leaves));
        volumes.put(volume);
        for (ZNodeLabel leaf: volume.getDescriptor().getLeaves()) {
            path = ZNodeLabel.Path.of(volume.getDescriptor().getRoot(), leaf);
            volumes.put(Volume.of(Hash.default32().apply(path.toString()).asIdentifier(), VolumeDescriptor.of(path)));
        }
        
        injector.getInstance(SimpleServer.class).start().get();
        FixedClientConnectionFactory<? extends Connection<? super Message.ClientSession>> clients = injector.getInstance(Key.get(new TypeLiteral<FixedClientConnectionFactory<? extends Connection<? super Message.ClientSession>>>(){}));
        clients.second().start().get();

        GetEvent<Connection<?>> connectEvent = GetEvent.create(injector.getInstance(SimpleServer.class).getConnections().connections());
        
        Session session = Session.create(1, Session.Parameters.uninitialized());
        ShardedClientConnectionExecutor<?> client = ShardedClientConnectionExecutor.newInstance(
                translator, 
                lookup, 
                Futures.immediateFuture(ConnectMessage.Response.Valid.newInstance(session)), 
                clients.get().get());
        
        Connection<?> serverConnection = connectEvent.get();
        
        GetEvent<Message.ClientRequest<?>> requestEvent = GetEvent.create(serverConnection);
        ListenableFuture<Message.ServerResponse<?>> clientFuture = client.submit(Operations.Requests.getChildren().setPath(volume.getDescriptor().getRoot()).build());
        serverConnection.read();
        assertEquals(VolumeShardedOperationTranslators.rootOf(volume.getId()).toString(), ((Records.PathGetter) requestEvent.get().getRecord()).getPath());
/*
        Long zxid = 1L;
        Message.ServerResponse<Records.Response> response = ProtocolResponseMessage.of(requestEvent.get().getXid(), zxid, Operations.Responses.getChildren().build());
        ListenableFuture<Message.ServerResponse<Records.Response>> serverFuture = connections.second().write(response);
        connections.first().read();
        serverFuture.get();
        assertEquals(volume.getId(), ((ShardedResponseMessage<?>) clientFuture.get()).getIdentifier());
        
        volume = volumes.get(ZNodeLabel.Path.of("/v1"));
        requestEvent = GetEvent.create(connections.second());
        clientFuture = client.submit(Operations.Requests.create().setPath(volume.getDescriptor().getRoot()).build());
        connections.second().read();
        assertEquals(VolumeShardedOperationTranslators.rootOf(volume.getId()).toString(), ((Records.PathGetter) requestEvent.get().getRecord()).getPath());

        zxid += 1L;
        response = ProtocolResponseMessage.of(requestEvent.get().getXid(), zxid, Operations.Responses.create().setPath(ZNodeLabel.Path.of(((Records.PathGetter) requestEvent.get().getRecord()).getPath())).build());
        serverFuture = connections.second().write(response);
        connections.first().read();
        serverFuture.get();
        assertEquals(volume.getId(), ((ShardedResponseMessage<?>) clientFuture.get()).getIdentifier());
*/
    }
}
