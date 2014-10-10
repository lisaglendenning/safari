package edu.uw.zookeeper.safari.storage.snapshot;

import static org.junit.Assert.*;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.Hex;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelVector;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.client.AbstractConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.StorageModules;
import edu.uw.zookeeper.safari.storage.schema.EscapedConverter;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode.SessionZNode.SessionIdHex;
import edu.uw.zookeeper.safari.storage.volumes.CreateVolume;

@RunWith(JUnit4.class)
public class ExecuteSnapshotTest extends AbstractMainTest {
    
    public static enum Index {
        FROM, TO;
        
        public <T> T get(List<T> input) {
            return input.get(ordinal());
        }
    }
    
    @Test(timeout=20000)
    public void test() throws Exception {
        final Component<?> root = Modules.newRootComponent();
        final List<Component<?>> storage = StorageModules.newStorageEnsemble(root, Index.values().length);
        ImmutableList.Builder<Component<?>> builder = ImmutableList.builder();
        for (Index server: Index.values()) {
            Component<?> client = StorageModules.newStorageSingletonClient(
                    storage.get(server.ordinal()), 
                    ImmutableList.of(root), 
                    ImmutableList.<SafariModule>of(),
                    Names.named(String.format("client-%d", server.ordinal())));
            builder.add(client);
        }
        final List<Component<?>> clients = builder.build();
        builder = ImmutableList.builder();
        final ImmutableList<Component<?>> components = builder.add(root).addAll(storage).addAll(clients).build();
        final Injector injector = stopping(components);
        
        final Callable<Void> callable = new Callable<Void>(){

            @Override
            public Void call() throws Exception {
                final Identifier volume = Identifier.valueOf(1);
                final ImmutableList<UnsignedLong> versions = ImmutableList.of(UnsignedLong.valueOf(1L), UnsignedLong.valueOf(2L));
                for (Index index: Index.values()) {
                    assertTrue(CreateVolume.call(volume, index.get(versions), index.get(clients).injector().getInstance(Key.get(new TypeLiteral<Materializer<StorageZNode<?>,?>>(){}, Storage.class))).get().booleanValue());
                }
                
                // create volume znodes
                OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> client = Index.FROM.get(clients).injector().getInstance(Key.get(new TypeLiteral<ListenableFuture<? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>>(){}, Storage.class)).get();
                Serializers.ByteCodec<Object> serializer = Index.FROM.get(clients).injector().getInstance(Key.get(new TypeLiteral<Serializers.ByteCodec<Object>>(){}));
                ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
                Operations.Requests.Create create = Operations.Requests.create();
                ZNodePath prefix = StorageSchema.Safari.Volumes.Volume.Root.pathOf(volume).join(ZNodeName.fromString(Hex.toPaddedHexString(client.session().get().getSessionId())));
                requests.add(create.setPath(prefix).setData(serializer.toBytes(prefix.label().toString())).build());
                for (String label: ImmutableList.of("sequentials", "ephemerals")) {
                    requests.add(create.setPath(prefix.join(ZNodeLabel.fromString(label))).setData(serializer.toBytes(label)).build());
                }
                for (Operation.ProtocolResponse<?> response: SubmittedRequests.submit(client, requests.build()).get()) {
                    Operations.unlessError(response.record());
                }
                requests = ImmutableList.builder();
                ZNodeLabel label = ZNodeLabel.fromString("sequentials");
                ZNodePath path = prefix.join(label);
                label = ZNodeLabel.fromString("sequential");
                path = path.join(label);
                create.setMode(CreateMode.PERSISTENT_SEQUENTIAL).setPath(path);
                for (int i=1; i<=3; ++i) {
                    requests.add(create.setData(serializer.toBytes(Integer.valueOf(i))).build());
                }
                List<? extends Operation.ProtocolResponse<?>> responses = SubmittedRequests.submit(client, requests.build()).get();
                requests = ImmutableList.builder();
                Operations.unlessError(responses.get(0).record());
                requests.add(Operations.Requests.delete().setPath(ZNodePath.fromString(((Records.PathGetter) responses.get(1).record()).getPath())).build());
                requests.add(Operations.Requests.exists().setPath(ZNodePath.fromString(((Records.PathGetter) responses.get(2).record()).getPath())).setWatch(true).build());
                label = ZNodeLabel.fromString("ephemeral");
                requests.add(create.setMode(CreateMode.EPHEMERAL_SEQUENTIAL).setPath(ZNodePath.fromString(((Records.PathGetter) responses.get(2).record()).getPath()).join(label)).setData(serializer.toBytes(label.toString())).build());
                requests.add(create.setMode(CreateMode.EPHEMERAL).setPath(prefix.join(ZNodeLabel.fromString("ephemerals")).join(label)).build());
                for (Operation.ProtocolResponse<?> response: SubmittedRequests.submit(client, requests.build()).get()) {
                    Operations.unlessError(response.record());
                }
                long zxid = client.submit(Operations.Requests.sync().build()).get().zxid();
                
                // snapshot
                final Function<Long,Long> toFrontend = new Function<Long,Long>() {
                    @Override
                    public Long apply(Long input) {
                        return Long.valueOf(input.longValue()+1L);
                    }
                };
                List<OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> snapshotClients = Lists.newArrayListWithCapacity(Index.values().length);
                for (Index index: Index.values()) {
                    snapshotClients.add(index.get(clients).injector().getInstance(Key.get(new TypeLiteral<ListenableFuture<? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>>(){}, Storage.class)).get());
                }
                ServerInetAddressView fromAddress = Index.FROM.get(clients).injector().getInstance(Key.get(ServerInetAddressView.class, Storage.class));
                Injector snapshot = Index.FROM.get(clients).injector().createChildInjector(
                    SnapshotClientModule.create(
                            EnsembleView.copyOf(fromAddress), 
                            fromAddress, 
                            Index.FROM.get(snapshotClients), 
                            Index.TO.get(snapshotClients)),
                    SnapshotVolumeModule.create(
                            new SnapshotVolumeParameters(volume, prefix.label(), Index.FROM.get(versions)),
                            new SnapshotVolumeParameters(volume, ZNodeLabel.empty(), Index.TO.get(versions))),
                    new AbstractModule() {
                        @Override
                        protected void configure() {
                            bind(new TypeLiteral<AsyncFunction<Long, Long>>(){}).annotatedWith(Snapshot.class).toInstance(
                                    new AsyncFunction<Long, Long>(){
                                        @Override
                                        public ListenableFuture<Long> apply(
                                                Long input) throws Exception {
                                            return Futures.immediateFuture(toFrontend.apply(input));
                                        }
                                    });
                        }
                    },
                    ExecuteSnapshot.module());
                Long value = snapshot.getInstance(Key.get(new TypeLiteral<AsyncFunction<Boolean,Long>>(){}, Snapshot.class)).apply(Boolean.TRUE).get();
                assertEquals(zxid, value.longValue());
                
                // check result
                path = StorageSchema.Safari.Volumes.Volume.Root.pathOf(volume); 
                List<String> children = ((Records.ChildrenGetter) Index.FROM.get(snapshotClients).submit(Operations.Requests.getChildren().setPath(prefix).build()).get().record()).getChildren();
                assertEquals(children, ((Records.ChildrenGetter) Index.TO.get(snapshotClients).submit(Operations.Requests.getChildren().setPath(path).build()).get().record()).getChildren());
                for (String child: children) {
                    label = ZNodeLabel.fromString(child);
                    byte[] data = ((Records.DataGetter) Index.FROM.get(snapshotClients).submit(Operations.Requests.getData().setPath(prefix.join(label)).build()).get().record()).getData();
                    assertTrue(Arrays.equals(data, ((Records.DataGetter) Index.TO.get(snapshotClients).submit(Operations.Requests.getData().setPath(path.join(label)).build()).get().record()).getData()));
                }
                label = ZNodeLabel.fromString("ephemerals");
                assertEquals(ImmutableList.<String>of(), ((Records.ChildrenGetter) Index.TO.get(snapshotClients).submit(Operations.Requests.getChildren().setPath(path.join(label)).build()).get().record()).getChildren());
                label = ZNodeLabel.fromString("sequentials");
                List<List<Sequential<String,?>>> sequentials = Lists.newArrayListWithCapacity(Index.values().length);
                for (Index index: Index.values()) {
                    switch (index) {
                    case FROM:
                        path = prefix;
                        break;
                    case TO:
                        path = StorageSchema.Safari.Volumes.Volume.Root.pathOf(volume);
                        break;
                    default:
                        throw new AssertionError();
                    }
                    children = ((Records.ChildrenGetter) index.get(snapshotClients).submit(Operations.Requests.getChildren().setPath(path.join(label)).build()).get().record()).getChildren();
                    sequentials.add(Lists.<Sequential<String,?>>newArrayListWithCapacity(children.size()));
                    for (String child: children) {
                        index.get(sequentials).add(Sequential.fromString(child));
                    }
                    Collections.sort(index.get(sequentials));
                    if (sequentials.size() > 1) {
                        assertEquals(sequentials.get(sequentials.size()-1).size(), sequentials.get(sequentials.size()-2).size());
                    }
                }
                for (int i=0; i<sequentials.get(sequentials.size()-1).size(); ++i) {
                    byte[] data = null;
                    for (Index index: Index.values()) {
                        switch (index) {
                        case FROM:
                        {
                            data = ((Records.DataGetter) index.get(snapshotClients).submit(Operations.Requests.getData().setPath(prefix.join(label).join(ZNodeLabel.fromString(index.get(sequentials).get(i).toString()))).build()).get().record()).getData();
                            break;
                        }
                        case TO:
                        {
                            path = StorageSchema.Safari.Volumes.Volume.Root.pathOf(volume).join(label).join(ZNodeLabel.fromString(index.get(sequentials).get(i).toString()));
                            assertTrue(Arrays.equals(data, ((Records.DataGetter) index.get(snapshotClients).submit(Operations.Requests.getData().setPath(path).build()).get().record()).getData()));
                            assertEquals(ImmutableList.<String>of(), ((Records.ChildrenGetter) index.get(snapshotClients).submit(Operations.Requests.getChildren().setPath(path).build()).get().record()).getChildren());
                            break;
                        }
                        default:
                            throw new AssertionError();
                        }
                    }
                }
                
                // check ephemerals
                path = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.pathOf(volume, Index.TO.get(versions)).join(ZNodeLabel.fromString(SessionIdHex.valueOf(toFrontend.apply(client.session().get().getSessionId())).toString()));
                children = ((Records.ChildrenGetter) Index.TO.get(snapshotClients).submit(Operations.Requests.getChildren().setPath(path).build()).get().record()).getChildren();
                assertEquals(2, children.size());
                ZNodeName name = ZNodeName.fromString("ephemerals/ephemeral");
                byte[] data = ((Records.DataGetter) Index.FROM.get(snapshotClients).submit(Operations.Requests.getData().setPath(prefix.join(name)).build()).get().record()).getData();
                label = ZNodeLabel.fromString(EscapedConverter.getInstance().convert(name.toString()));
                assertTrue(String.valueOf(children), children.contains(label.toString()));
                assertEquals(
                        Operations.Requests.create().setMode(CreateMode.EPHEMERAL).setData(data).setPath(StorageSchema.Safari.Volumes.Volume.Root.pathOf(volume).join(name)).build(), 
                        Records.Requests.deserialize(OpCode.CREATE,
                                new ByteBufInputArchive(Unpooled.wrappedBuffer(
                                        ((Records.DataGetter) Index.TO.get(snapshotClients).submit(Operations.Requests.getData().setPath(path.join(label)).build()).get().record()).getData()))));
                name = ZNodeName.fromString(ZNodeLabelVector.join("sequentials", Index.FROM.get(sequentials).get(1).toString()));
                label = ZNodeLabel.fromString(Iterables.getOnlyElement(((Records.ChildrenGetter) Index.FROM.get(snapshotClients).submit(Operations.Requests.getChildren().setPath(prefix.join(name)).build()).get().record()).getChildren()));
                data = ((Records.DataGetter) Index.FROM.get(snapshotClients).submit(Operations.Requests.getData().setPath(prefix.join(name).join(label)).build()).get().record()).getData();
                name = ZNodeName.fromString(ZNodeLabelVector.join("sequentials", Index.TO.get(sequentials).get(1).toString(), label.toString()));
                label = ZNodeLabel.fromString(EscapedConverter.getInstance().convert(name.toString()));
                assertTrue(String.valueOf(children), children.contains(label.toString()));
                path = path.join(label);
                label = Iterables.getLast((ZNodeLabelVector) name);
                name = ((RelativeZNodePath) ((RelativeZNodePath) name).prefix(name.length() - label.length() - 1)).join(ZNodeLabel.fromString(Sequential.fromString(label.toString()).prefix()));
                assertEquals(
                        Operations.Requests.create().setMode(CreateMode.EPHEMERAL_SEQUENTIAL).setData(data).setPath(StorageSchema.Safari.Volumes.Volume.Root.pathOf(volume).join(name)).build(), 
                        Records.Requests.deserialize(OpCode.CREATE,
                                new ByteBufInputArchive(Unpooled.wrappedBuffer(
                                        ((Records.DataGetter) Index.TO.get(snapshotClients).submit(Operations.Requests.getData().setPath(path).build()).get().record()).getData()))));
                
                // check watches
                path = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.pathOf(volume, Index.TO.get(versions)).join(ZNodeLabel.fromString(SessionIdHex.valueOf(toFrontend.apply(client.session().get().getSessionId())).toString()));
                children = ((Records.ChildrenGetter) Index.TO.get(snapshotClients).submit(Operations.Requests.getChildren().setPath(path).build()).get().record()).getChildren();
                name = ZNodeName.fromString(ZNodeLabelVector.join("sequentials", Index.TO.get(sequentials).get(1).toString()));
                assertEquals(ImmutableList.of(EscapedConverter.getInstance().convert(name.toString())), children);
                
                AbstractConnectionClientExecutor.disconnect(client);
                for (Index index: Index.values()) {
                    AbstractConnectionClientExecutor.disconnect(index.get(snapshotClients));
                }
                return null;
            }
            
        };
        
        callWithService(
                injector, 
                callable);
    }
}
