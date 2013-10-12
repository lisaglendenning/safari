package edu.uw.zookeeper.safari.control;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.UnknownHostException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Label;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.backend.BackendView;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.control.Control.CreateEntityTask;
import edu.uw.zookeeper.safari.data.VolumeDescriptor;

public abstract class ControlSchema extends Control.ControlZNode {
    
    public static SchemaInstance getInstance() {
        return Holder.getInstance().get();
    }

    @Label
    public static final ZNodeLabel.Path ROOT = ZNodeLabel.Path.of("/safari");
    
    @ZNode
    public static abstract class Peers extends Control.ControlZNode {

        @Label
        public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("peers");
        
        @ZNode
        public static class Entity extends Control.IdentifierZNode {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;
            
            public static Control.EntityValue<ServerInetAddressView, Entity, PeerAddress> schema() {
                return EntityHolder.ENTITY_SCHEMA.get();
            }
            
            protected static enum EntityHolder implements Reference<Control.EntityValue<ServerInetAddressView, Entity, PeerAddress>> {
                ENTITY_SCHEMA;

                private final Control.EntityValue<ServerInetAddressView, Entity, PeerAddress> instance;
                
                private EntityHolder() {
                    this.instance = Control.EntityValue.create(
                            Entity.class, PeerAddress.class,
                            new Function<ServerInetAddressView, Hash.Hashed>() {
                                @Override
                                public Hash.Hashed apply(ServerInetAddressView value) {
                                    return Hash.default32().apply(ServerInetAddressView.toString(value));
                                }
                            });
                }
                
                @Override
                public Control.EntityValue<ServerInetAddressView, Entity, PeerAddress> get() {
                    return instance;
                }
            }

            public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Peers.Entity> create(
                    final ServerInetAddressView value, 
                    final Materializer<O> materializer) {
                Control.LookupHashedTask<Peers.Entity> task = Control.LookupHashedTask.newInstance(
                        hashOf(value),
                        CreateEntityTask.newInstance(value, schema(), materializer));
                task.run();
                return task;
            }
            
            public static CachedFunction<Peers.Entity, Boolean> isPresent(
                    final Materializer<?> materializer) {
                Function<Peers.Entity, Boolean> cached = new Function<Peers.Entity, Boolean>() {
                    @Override
                    public Boolean apply(Peers.Entity input) {
                        return materializer.contains(input.presence().path());
                    }
                };
                AsyncFunction<Peers.Entity, Boolean> lookup = new AsyncFunction<Peers.Entity, Boolean>() {
                    @Override
                    public ListenableFuture<Boolean> apply(Peers.Entity input) {
                        return input.presence().exists(materializer);
                    }
                };
                return CachedFunction.create(cached, lookup);
            }

            public static AsyncFunction<ServerInetAddressView, Entity> lookup(
                    final Materializer<?> materializer) {
                return new AsyncFunction<ServerInetAddressView, Entity>() {
                        @Override
                        public ListenableFuture<Entity> apply(
                                final ServerInetAddressView value)
                                throws Exception {
                            return Control.LookupEntityTask.call(
                                    value, schema(), materializer);
                        }
                };
            }

            public static AsyncFunction<Identifier, Regions.Entity> lookupEnsemble(
                    final Materializer<?> materializer) {
                final AsyncFunction<EnsembleView<ServerInetAddressView>, Regions.Entity> ensembleOfBackend = Regions.Entity.lookup(materializer);
                return new AsyncFunction<Identifier, Regions.Entity>() {
                    @Override
                    public ListenableFuture<Regions.Entity> apply(
                            final Identifier peer)
                            throws Exception {
                        return Futures.transform(
                                Entity.of(peer).backend(materializer), 
                                new AsyncFunction<Peers.Entity.Backend, Regions.Entity>() {
                                    @Override
                                    public ListenableFuture<Regions.Entity> apply(Peers.Entity.Backend backend)
                                            throws Exception {
                                        return ensembleOfBackend.apply(backend.get().getEnsemble());
                                    }
                                });
                    }
                };
            }

            public static Peers.Entity valueOf(String label) {
                return of(Identifier.valueOf(label));
            }
            
            public static Hash.Hashed hashOf(ServerInetAddressView value) {
                return schema().getHasher().apply(value);
            }
            
            public static Peers.Entity of(Identifier identifier) {
                return new Entity(identifier);
            }
            
            public Entity(Identifier identifier) {
                super(identifier);
            }
            
            @Override
            public String toString() {
                return get().toString();
            }
            
            public Entity.Presence presence() {
                return Entity.Presence.of(this);
            }

            public ListenableFuture<Backend> backend(Materializer<?> materializer) {
                return Backend.get(this, materializer);
            }
            
            @ZNode(createMode=CreateMode.EPHEMERAL)
            public static class Presence extends Control.ControlZNode {

                protected static enum Exists implements Function<Operation.ProtocolResponse<?>, Boolean> {
                    EXISTS;

                    @Override
                    public Boolean apply(Operation.ProtocolResponse<?> input) {
                        return ! (input.record() instanceof Operation.Error);
                    }
                }
                
                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("presence");

                public static Presence of(Peers.Entity parent) {
                    return new Presence(parent);
                }
                
                public Presence(Peers.Entity parent) {
                    super(parent);
                }

                public <V extends Operation.ProtocolResponse<?>> 
                ListenableFuture<V> create(Materializer<V> materializer) {
                    return materializer.operator().create(path()).submit();
                }
                
                public ListenableFuture<Boolean> exists(ClientExecutor<? super Records.Request, ?> client) {
                    return Futures.transform(
                            client.submit(Operations.Requests.exists().setPath(path()).build()), 
                            Exists.EXISTS);
                }
            }
            
            @ZNode(type=ServerInetAddressView.class)
            public static class ClientAddress extends Control.ValueZNode<ServerInetAddressView> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("clientAddress");
                
                public static ListenableFuture<ClientAddress> get(Peers.Entity entity, Materializer<?> materializer) {
                    return getValue(ClientAddress.class, entity, materializer);
                }
                
                public static ListenableFuture<ClientAddress> create(ServerInetAddressView value, Peers.Entity entity, Materializer<?> materializer) {
                    return create(ClientAddress.class, value, entity, materializer);
                }
                
                public static ClientAddress valueOf(String label, Peers.Entity parent) throws UnknownHostException {
                    return of(ServerInetAddressView.fromString(label), parent);
                }
                
                public static ClientAddress of(ServerInetAddressView address, Peers.Entity parent) {
                    return new ClientAddress(address, parent);
                }
                
                public ClientAddress(ServerInetAddressView address, Peers.Entity parent) {
                    super(address, parent);
                }
            }
            
            @ZNode(type=ServerInetAddressView.class)
            public static class PeerAddress extends Control.ValueZNode<ServerInetAddressView> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("peerAddress");
                
                public static ZNodeLabel.Path pathOf(Peers.Entity entity) {
                    return (ZNodeLabel.Path) ZNodeLabel.joined(entity.path(), LABEL);
                }
                
                public static CachedFunction<Identifier, PeerAddress> lookup(
                        final Materializer<?> materializer) {
                    Function<Identifier, PeerAddress> cached = new Function<Identifier, PeerAddress>() {
                        @Override
                        public @Nullable PeerAddress apply(Identifier peer) {
                            Entity entity = Entity.of(peer);
                            ServerInetAddressView address = null;
                            ZNodeLabel.Path path = pathOf(entity);
                            Materializer.MaterializedNode node = materializer.get(path);
                            if (node != null) {
                                address = (ServerInetAddressView) node.get().get();
                            }                    
                            if (address == null) {
                                return null;
                            } else {
                                return of(address, entity);
                            }
                        }
                    };
                    AsyncFunction<Identifier, PeerAddress> lookup = new AsyncFunction<Identifier, PeerAddress>() {
                        @Override
                        public ListenableFuture<PeerAddress> apply(final Identifier peer) {
                            final Entity entity = Entity.of(peer);
                            final ZNodeLabel.Path path = pathOf(entity);
                            return Futures.transform(materializer.operator().getData(path).submit(),
                                    new Function<Operation.ProtocolResponse<?>, PeerAddress>() {
                                        @Override
                                        public @Nullable PeerAddress apply(Operation.ProtocolResponse<?> input) {
                                            try {
                                                Operations.maybeError(input.record(), KeeperException.Code.NONODE);
                                            } catch (KeeperException e) {
                                                return null;
                                            }
                                            ServerInetAddressView address = null;
                                            Materializer.MaterializedNode node = materializer.get(path);
                                            if (node != null) {
                                                address = (ServerInetAddressView) node.get().get();
                                            }
                                            if (address == null) {
                                                return null;
                                            } else {
                                                return of(address, entity);
                                            }
                                        }
                                    });
                        }
                    };
                    return CachedFunction.create(cached, lookup);
                }
                
                public static ListenableFuture<PeerAddress> get(Peers.Entity entity, Materializer<?> materializer) {
                    return getValue(PeerAddress.class, entity, materializer);
                }
                
                public static ListenableFuture<PeerAddress> create(ServerInetAddressView value, Peers.Entity entity, Materializer<?> materializer) {
                    return create(PeerAddress.class, value, entity, materializer);
                }
                
                public static PeerAddress valueOf(String label, Peers.Entity parent) throws UnknownHostException {
                    return of(ServerInetAddressView.fromString(label), parent);
                }
                
                public static PeerAddress of(ServerInetAddressView address, Peers.Entity parent) {
                    return new PeerAddress(address, parent);
                }

                public PeerAddress(ServerInetAddressView address, Peers.Entity parent) {
                    super(checkNotNull(address), parent);
                }
            }
            
            @ZNode(label="backend", type=BackendView.class)
            public static class Backend extends Control.ValueZNode<BackendView> {
                
                public static ListenableFuture<Entity.Backend> get(Peers.Entity entity, Materializer<?> materializer) {
                    return getValue(Entity.Backend.class, entity, materializer);
                }
                
                public static ListenableFuture<Entity.Backend> create(BackendView value, Peers.Entity entity, Materializer<?> materializer) {
                    return create(Entity.Backend.class, value, entity, materializer);
                }
                
                public static Entity.Backend of(BackendView value, Peers.Entity parent) {
                    return new Backend(value, parent);
                }

                public Backend(BackendView value, Peers.Entity parent) {
                    super(value, parent);
                }
            }
        }
    }

    @ZNode(label="regions")
    public static abstract class Regions extends Control.ControlZNode {
        
        public static ListenableFuture<List<Regions.Entity>> getRegions(ClientExecutor<? super Records.Request, ?> client) {
            client.submit(Operations.Requests.sync().setPath(path(Regions.class)).build());
            return Futures.transform(
                    client.submit(Operations.Requests.getChildren().setPath(path(Regions.class)).build()), 
                    new AsyncFunction<Operation.ProtocolResponse<?>, List<Regions.Entity>>() {
                        @Override
                        public ListenableFuture<List<Regions.Entity>> apply(Operation.ProtocolResponse<?> input) throws KeeperException {
                            Records.ChildrenGetter response = (Records.ChildrenGetter) Operations.unlessError(input.record());
                            List<Regions.Entity> result = Lists.newArrayListWithCapacity(response.getChildren().size());
                            for (String child: response.getChildren()) {
                                result.add(Regions.Entity.of(Identifier.valueOf(child)));
                            }
                            return Futures.immediateFuture(result);
                        }
                    });
        }
        
        @ZNode
        public static class Entity extends Control.IdentifierZNode {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;

            public static Control.EntityValue<EnsembleView<ServerInetAddressView>, Entity, Backend> schema() {
                return EntityHolder.ENTITY_SCHEMA.get();
            }
            
            protected static enum EntityHolder implements Reference<Control.EntityValue<EnsembleView<ServerInetAddressView>, Entity, Backend>> {
                ENTITY_SCHEMA;

                private final Control.EntityValue<EnsembleView<ServerInetAddressView>, Entity, Backend> instance;
                
                private EntityHolder() {
                    this.instance = Control.EntityValue.create(
                            Entity.class, Backend.class,
                            new Function<EnsembleView<ServerInetAddressView>, Hash.Hashed>() {
                                @Override
                                public Hash.Hashed apply(EnsembleView<ServerInetAddressView> value) {
                                    return Hash.default32().apply(EnsembleView.toString(value));
                                }
                            });
                }
                
                @Override
                public Control.EntityValue<EnsembleView<ServerInetAddressView>, Entity, Backend> get() {
                    return instance;
                }
            }

            public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Regions.Entity> create(
                    final EnsembleView<ServerInetAddressView> value, 
                    final Materializer<O> materializer) {
                Control.LookupHashedTask<Entity> task = Control.LookupHashedTask.newInstance(
                        hashOf(value),
                        CreateEntityTask.newInstance(value, schema(), materializer));
                task.run();
                return task;
            }
            
            public static AsyncFunction<EnsembleView<ServerInetAddressView>, Entity> lookup(
                    final Materializer<?> materializer) {
                return new AsyncFunction<EnsembleView<ServerInetAddressView>, Entity>() {
                        @Override
                        public ListenableFuture<Entity> apply(
                                final EnsembleView<ServerInetAddressView> value)
                                throws Exception {
                            return Control.LookupEntityTask.call(
                                    value, schema(), materializer);
                        }
                };
            }

            public static Hash.Hashed hashOf(EnsembleView<ServerInetAddressView> value) {
                return schema().getHasher().apply(value);
            }
            
            public static Regions.Entity valueOf(String label) {
                return of(Identifier.valueOf(label));
            }
            
            public static Regions.Entity of(Identifier identifier) {
                return new Entity(identifier);
            }
            
            public Entity(Identifier identifier) {
                super(identifier);
            }
            
            @Override
            public String toString() {
                return get().toString();
            }
            
            public ListenableFuture<Backend> backend(Materializer<?> materializer) {
                return Backend.get(this, materializer);
            }

            @ZNode(type=EnsembleView.class)
            public static class Backend extends Control.ValueZNode<EnsembleView<ServerInetAddressView>> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("backend");

                public static ZNodeLabel.Path pathOf(Regions.Entity entity) {
                    return (ZNodeLabel.Path) ZNodeLabel.joined(entity.path(), LABEL);
                }
                
                public static ListenableFuture<Entity.Backend> get(Regions.Entity entity, Materializer<?> materializer) {
                    return getValue(Entity.Backend.class, entity, materializer);
                }
                
                public static ListenableFuture<Entity.Backend> create(EnsembleView<ServerInetAddressView> value, Regions.Entity entity, Materializer<?> materializer) {
                    return create(Regions.Entity.Backend.class, value, entity, materializer);
                }
                
                public static Entity.Backend of(EnsembleView<ServerInetAddressView> value, Regions.Entity parent) {
                    return new Backend(value, parent);
                }

                public Backend(EnsembleView<ServerInetAddressView> value, Regions.Entity parent) {
                    super(value, parent);
                }
            }
            
            @ZNode
            public static class Members extends Control.ControlZNode {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("members");
                
                public static Entity.Members of(Regions.Entity parent) {
                    return new Members(parent);
                }

                public static CachedFunction<Identifier, List<Member>> getMembers(
                        final Materializer<?> materializer) {
                    Function<Identifier, List<Member>> cached = new Function<Identifier, List<Members.Member>>() {
                        @Override
                        @Nullable
                        public
                        List<Member> apply(Identifier region) {
                            Members members = Members.of(Entity.of(region));
                            return members.get(materializer);
                        }
                    };
                    AsyncFunction<Identifier, List<Member>> lookup = new AsyncFunction<Identifier, List<Members.Member>>() {
                        @Override
                        public ListenableFuture<List<Member>> apply(Identifier region) {
                            final Members members = Members.of(Entity.of(region));
                            materializer.operator().sync(members.path()).submit();
                            return Futures.transform(
                                    materializer.operator().getChildren(members.path()).submit(),
                                    new AsyncFunction<Operation.ProtocolResponse<?>, List<Member>>() {
                                        @Override
                                        @Nullable
                                        public ListenableFuture<List<Member>> apply(Operation.ProtocolResponse<?> input) throws KeeperException {
                                            Operations.unlessError(input.record());
                                            return Futures.immediateFuture(members.get(materializer));
                                        }
                                    });
                        }
                    };
                    return CachedFunction.create(cached, lookup);
                }
                
                public Members(Regions.Entity parent) {
                    super(parent);
                }

                public List<Member> get(Materializer<?> materializer) {
                    Materializer.MaterializedNode parent = materializer.get(path());
                    if (parent != null) {
                        ImmutableList.Builder<Member> members = ImmutableList.builder();
                        for (ZNodeLabel.Component e: parent.keySet()) {
                            members.add(Member.valueOf(e.toString(), this));
                        }
                        return members.build();
                    } else {
                        return null;
                    }
                }
                
                @ZNode
                public static class Member extends Control.IdentifierZNode {

                    @Label(type=LabelType.PATTERN)
                    public static final String LABEL_PATTERN = Identifier.PATTERN;

                    public static Members.Member valueOf(String label, Entity.Members parent) {
                        return of(Identifier.valueOf(label), parent);
                    }
                    
                    public static Members.Member of(Identifier identifier, Entity.Members parent) {
                        return new Member(identifier, parent);
                    }
                    
                    public Member(Identifier identifier, Entity.Members parent) {
                        super(identifier, parent);
                    }
                    
                    @Override
                    public String toString() {
                        return get().toString();
                    }
                }
            }
            
            @ZNode(type=Identifier.class, createMode=CreateMode.EPHEMERAL)
            public static class Leader extends Control.ValueZNode<Identifier> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("leader");
                
                public static ListenableFuture<Entity.Leader> get(Regions.Entity parent, Materializer<?> materializer) {
                    return getValue(Entity.Leader.class, parent, materializer);
                }
                
                public static ListenableFuture<Entity.Leader> create(Identifier value, Regions.Entity parent, Materializer<?> materializer) {
                    return create(Entity.Leader.class, value, parent, materializer);
                }
                
                public static class Proposal<O extends Operation.ProtocolResponse<?>> extends RunnablePromiseTask<Entity.Leader, Entity.Leader> {

                    public static <O extends Operation.ProtocolResponse<?>>
                    Proposal<O> of(
                            Entity.Leader task, 
                            Materializer<O> materializer) {
                        Promise<Entity.Leader> promise = SettableFuturePromise.create();
                        Proposal<O> proposal = new Proposal<O>(task, materializer, promise);
                        proposal.run();
                        return proposal;
                    }
                    
                    protected final Materializer<O> materializer;
                    protected ListenableFuture<O> future;
                    
                    public Proposal(
                            Entity.Leader task, 
                            Materializer<O> materializer,
                            Promise<Entity.Leader> delegate) {
                        super(task, delegate);
                        this.materializer = materializer;
                        this.future = null;
                    }
                    
                    @Override
                    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
                        boolean cancel = super.cancel(mayInterruptIfRunning);
                        if (cancel) {
                            if (future != null) {
                                future.cancel(mayInterruptIfRunning);
                            }
                        }
                        return cancel;
                    }
                    
                    @Override
                    public synchronized Optional<Entity.Leader> call() throws Exception {
                        if (future == null) {
                            materializer.operator().create(task().path(), task().get()).submit();
                            materializer.operator().sync(task().path()).submit();
                            future = materializer.operator().getData(task().path(), true).submit();
                            future.addListener(this, MoreExecutors.sameThreadExecutor());
                            return Optional.absent();
                        }
                        if (future.isDone()) {
                            O result = future.get();
                            Optional<Operation.Error> error = Operations.maybeError(result.record(), KeeperException.Code.NONODE, result.toString());
                            if (! error.isPresent()) {
                                Materializer.MaterializedNode node = materializer.get(task().path());
                                if (node != null) {
                                    Identifier id = (Identifier) node.get().get();
                                    if (id != null) {
                                        return Optional.of(Entity.Leader.of(id, task().parent()));
                                    }
                                }
                            }
                            
                            // try again
                            future = null;
                            run();
                        }
                        return Optional.absent();
                    }
                }
                
                public static class Proposer<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<Entity.Leader, Entity.Leader> {

                    public static <O extends Operation.ProtocolResponse<?>> Proposer<O> of(
                            Materializer<O> materializer) {
                        return new Proposer<O>(materializer);
                    }
                    
                    protected final Materializer<O> materializer;
                    
                    public Proposer(
                            Materializer<O> materializer) {
                        this.materializer = materializer;
                    }
                    
                    @Override
                    public ListenableFuture<Entity.Leader> apply(Entity.Leader input) {
                        return Proposal.of(input, materializer);
                    }
                }
                
                public static Entity.Leader of(Identifier value, Regions.Entity parent) {
                    return new Leader(value, parent);
                }

                public Leader(Identifier value, Regions.Entity parent) {
                    super(value, parent);
                }
                
                public Regions.Entity parent() {
                    return (Regions.Entity) super.parent();
                }
            }
        }
    }

    @ZNode(label="volumes")
    public static abstract class Volumes extends Control.ControlZNode {
        
        @ZNode
        public static class Entity extends Control.IdentifierZNode {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;

            public static Control.EntityValue<VolumeDescriptor, Entity, Entity.Volume> schema() {
                return EntityHolder.ENTITY_SCHEMA.get();
            }
            
            protected static enum EntityHolder implements Reference<Control.EntityValue<VolumeDescriptor, Entity, Entity.Volume>> {
                ENTITY_SCHEMA;

                private final Control.EntityValue<VolumeDescriptor, Entity, Entity.Volume> instance;
                
                private EntityHolder() {
                    this.instance = Control.EntityValue.create(
                            Entity.class, Entity.Volume.class,
                            new Function<VolumeDescriptor, Hash.Hashed>() {
                                @Override
                                public Hash.Hashed apply(VolumeDescriptor value) {
                                    return Hash.default32().apply(value.getRoot().toString());
                                }
                            });
                }
                
                @Override
                public Control.EntityValue<VolumeDescriptor, Entity, Entity.Volume> get() {
                    return instance;
                }
            }

            public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Volumes.Entity> create(
                    final VolumeDescriptor value, 
                    final Materializer<O> materializer) {
                Control.LookupHashedTask<Entity> task = Control.LookupHashedTask.newInstance(
                        hashOf(value),
                        CreateEntityTask.newInstance(value, schema(), materializer));
                task.run();
                return task;
            }

            public static AsyncFunction<VolumeDescriptor, Entity> lookup(
                    final Materializer<?> materializer) {
                return new AsyncFunction<VolumeDescriptor, Entity>() {
                        @Override
                        public ListenableFuture<Entity> apply(
                                final VolumeDescriptor value)
                                throws Exception {
                            return Control.LookupEntityTask.call(
                                    value, schema(), materializer);
                        }
                };
            }

            public static Hash.Hashed hashOf(VolumeDescriptor value) {
                return schema().getHasher().apply(value);
            }
            
            public static Volumes.Entity valueOf(String label) {
                return of(Identifier.valueOf(label));
            }
            
            public static Volumes.Entity of(Identifier identifier) {
                return new Entity(identifier);
            }
            
            public Entity(Identifier identifier) {
                super(identifier);
            }
            
            @Override
            public String toString() {
                return get().toString();
            }
            
            @ZNode(type=VolumeDescriptor.class)
            public static class Volume extends Control.ValueZNode<VolumeDescriptor> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("volume");

                public static ZNodeLabel.Path pathOf(Volumes.Entity entity) {
                    return (ZNodeLabel.Path) ZNodeLabel.joined(entity.path(), LABEL);
                }
                
                public static ListenableFuture<Entity.Volume> get(Volumes.Entity entity, Materializer<?> materializer) {
                    return getValue(Entity.Volume.class, entity, materializer);
                }
                
                public static ListenableFuture<Entity.Volume> create(VolumeDescriptor value, Volumes.Entity entity, Materializer<?> materializer) {
                    return create(Entity.Volume.class, value, entity, materializer);
                }

                public static ListenableFuture<Entity.Volume> set(VolumeDescriptor value, Volumes.Entity entity, Materializer<?> materializer) {
                    return setValue(Entity.Volume.class, value, entity, materializer);
                }
                
                public static Entity.Volume of(VolumeDescriptor value, Volumes.Entity parent) {
                    return new Volume(value, parent);
                }

                public Volume(VolumeDescriptor value, Volumes.Entity parent) {
                    super(value, parent);
                }
            }

            @ZNode(type=Identifier.class)
            public static class Region extends Control.ValueZNode<Identifier> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("region");
                
                public static ListenableFuture<Entity.Region> get(Volumes.Entity entity, Materializer<?> materializer) {
                    return getValue(Entity.Region.class, entity, materializer);
                }
                
                public static ListenableFuture<Entity.Region> create(Identifier value, Volumes.Entity entity, Materializer<?> materializer) {
                    return create(Entity.Region.class, value, entity, materializer);
                }

                public static ListenableFuture<Entity.Region> set(Identifier value, Volumes.Entity entity, Materializer<?> materializer) {
                    return setValue(Entity.Region.class, value, entity, materializer);
                }
                
                public static Entity.Region of(Identifier value, Volumes.Entity parent) {
                    return new Region(value, parent);
                }

                public Region(Identifier value, Volumes.Entity parent) {
                    super(value, parent);
                }
            }
        }            
    }
    
    protected static enum Holder implements Reference<SchemaInstance> {
        SCHEMA(ControlSchema.class);
        
        public static Holder getInstance() {
            return SCHEMA;
        }
        
        private final SchemaInstance instance;
        
        private Holder(Object root) {
            this.instance = SchemaInstance.newInstance(root);
        }
    
        @Override
        public SchemaInstance get() {
            return instance;
        }
    }}