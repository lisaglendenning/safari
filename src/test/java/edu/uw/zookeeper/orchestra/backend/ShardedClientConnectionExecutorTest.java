package edu.uw.zookeeper.orchestra.backend;

import static org.junit.Assert.*;

import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.GetEvent;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.control.Hash;
import edu.uw.zookeeper.orchestra.data.Volume;
import edu.uw.zookeeper.orchestra.data.VolumeCache;
import edu.uw.zookeeper.orchestra.data.VolumeDescriptor;
import edu.uw.zookeeper.orchestra.net.SimpleClient;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.SimpleServer;

@RunWith(JUnit4.class)
public class ShardedClientConnectionExecutorTest {
    
    public static class Module extends SimpleClient {
        
        public static Injector injector() {
            return Guice.createInjector(create());
        }
        
        public static Module create() {
            return new Module();
        }

        @Provides @Singleton
        public VolumeCache getVolumes() {
            return VolumeCache.newInstance();
        }
    }
    
    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = Module.injector();
        VolumeCache volumes = injector.getInstance(VolumeCache.class);
        
        Function<ZNodeLabel.Path, Identifier> lookup = BackendRequestService.newVolumePathLookup(volumes);
        VolumeShardedOperationTranslators translator = new VolumeShardedOperationTranslators(
                BackendRequestService.newVolumeIdLookup(volumes));
        
        SortedSet<ZNodeLabel> leaves = Sets.newTreeSet(ImmutableList.of(ZNodeLabel.of("v1"), ZNodeLabel.of("v2")));
        ZNodeLabel.Path path = ZNodeLabel.Path.root();
        Volume volume = Volume.of(Hash.default32().apply(path.toString()).asIdentifier(), VolumeDescriptor.of(path, leaves));
        volumes.put(volume);
        for (ZNodeLabel leaf: volume.getDescriptor().getLeaves()) {
            path = volume.getDescriptor().append(leaf);
            volumes.put(Volume.of(Hash.default32().apply(path.toString()).asIdentifier(), VolumeDescriptor.of(path)));
        }
        
        injector.getInstance(Module.SimpleClientService.class).start().get();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.start().get();

        GetEvent<Connection<?>> connectEvent = GetEvent.create(injector.getInstance(SimpleServer.class).getConnections().connections());
        
        ShardedClientConnectionExecutor<?> client = ShardedClientConnectionExecutor.newInstance(
                translator, 
                lookup, 
                ConnectMessage.Request.NewRequest.newInstance(),
                injector.getInstance(Key.get(new TypeLiteral<ListenableFuture<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>>>(){})).get(),
                injector.getInstance(ScheduledExecutorService.class));
        
        Connection<?> serverConnection = connectEvent.get();
        
        GetEvent<Message.ClientRequest<?>> requestEvent = GetEvent.create(serverConnection);
        ListenableFuture<Message.ServerResponse<?>> clientFuture = client.submit(Operations.Requests.getChildren().setPath(volume.getDescriptor().getRoot()).build());
        assertEquals(VolumeShardedOperationTranslators.rootOf(volume.getId()).toString(), ((Records.PathGetter) requestEvent.get().getRecord()).getPath());
        assertEquals(volume.getId(), ((ShardedResponseMessage<?>) clientFuture.get()).getIdentifier());
        
        volume = volumes.get(ZNodeLabel.Path.of("/v1"));
        requestEvent = GetEvent.create(serverConnection);
        clientFuture = client.submit(Operations.Requests.create().setPath(volume.getDescriptor().getRoot()).build());
        assertEquals(VolumeShardedOperationTranslators.rootOf(volume.getId()).toString(), ((Records.PathGetter) requestEvent.get().getRecord()).getPath());
        assertEquals(volume.getId(), ((ShardedResponseMessage<?>) clientFuture.get()).getIdentifier());


        client.submit(Records.Requests.getInstance().get(OpCode.CLOSE_SESSION)).get();
        
        monitor.stop().get();
    }
}
