package edu.uw.zookeeper.orchestra.control;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.Label;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.VolumeDescriptor;
import edu.uw.zookeeper.orchestra.backend.BackendView;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Pair;

public abstract class Orchestra extends Control.ControlZNode {
    
    @Label
    public static final ZNodeLabel.Path ROOT = ZNodeLabel.Path.of("/orchestra");
    
    @ZNode(label="conductors")
    public static abstract class Conductors extends Control.ControlZNode {

        @ZNode
        public static class Entity extends Control.TypedLabelZNode<Identifier> {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;

            public static Entity create(ServerInetAddressView value, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                Materializer.Operator<?,?> operator = materializer.operator();
                Hash.Hashed hashed = Orchestra.Conductors.Entity.hashOf(value);
                Orchestra.Conductors.Entity entity = null;
                while (entity == null) {
                    Identifier id = hashed.asIdentifier();
                    entity = Orchestra.Conductors.Entity.of(id);
                    Pair<? extends Operation.SessionRequest, ? extends Operation.SessionResponse> result = operator.create(entity.path()).submit().get();
                    Operations.maybeError(result.second().response(), KeeperException.Code.NODEEXISTS, result.toString());
                    
                    Orchestra.Conductors.Entity.ConductorAddress address = Orchestra.Conductors.Entity.ConductorAddress.create(value, entity, materializer);
                    if (! value.equals(address.get())) {
                        entity = null;
                        hashed = hashed.rehash();
                        continue;
                    }
                }
                return entity;
            }
            
            public static Conductors.Entity valueOf(String label) {
                return of(Identifier.valueOf(label));
            }
            
            public static Hash.Hashed hashOf(ServerInetAddressView value) {
                return Hash.default32().apply(ServerInetAddressView.toString(value));
            }
            
            public static Conductors.Entity of(Identifier identifier) {
                return new Entity(identifier);
            }
            
            public Entity(Identifier identifier) {
                super(identifier);
            }
            
            @Override
            public String toString() {
                return get().toString();
            }
            
            @ZNode(createMode=CreateMode.EPHEMERAL)
            public static class Presence extends Control.ControlZNode {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("presence");

                public static Presence of(Conductors.Entity parent) {
                    return new Presence(parent);
                }
                
                public Presence(Conductors.Entity parent) {
                    super(parent);
                }
                
                public boolean exists(ClientExecutor<Operation.Request,?,?> client) throws InterruptedException, ExecutionException {
                    Pair<? extends Operation.SessionRequest, ? extends Operation.SessionResponse> result = client.submit(Operations.Requests.exists().setPath(path()).build()).get();
                    return ! (result.second().response() instanceof Operation.Error);
                }
            }
            
            @ZNode(type=ServerInetAddressView.class)
            public static class ClientAddress extends Control.TypedValueZNode<ServerInetAddressView> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("clientAddress");
                
                public static ClientAddress get(Conductors.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(ClientAddress.class, entity, materializer);
                }
                
                public static ClientAddress create(ServerInetAddressView value, Conductors.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(ClientAddress.class, value, entity, materializer);
                }
                
                public static ClientAddress valueOf(String label, Conductors.Entity parent) {
                    return of(ServerInetAddressView.fromString(label), parent);
                }
                
                public static ClientAddress of(ServerInetAddressView address, Conductors.Entity parent) {
                    return new ClientAddress(address, parent);
                }
                
                public ClientAddress(ServerInetAddressView address, Conductors.Entity parent) {
                    super(address, parent);
                }
            }
            
            @ZNode(type=ServerInetAddressView.class)
            public static class ConductorAddress extends Control.TypedValueZNode<ServerInetAddressView> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("conductorAddress");
                
                public static ConductorAddress lookup(Conductors.Entity entity, Materializer<?,?> materializer) throws KeeperException, InterruptedException, ExecutionException {
                    Materializer.MaterializedNode parent = materializer.get(entity.path());
                    if (parent == null) {
                        Operations.maybeError(materializer.operator().getChildren(entity.path()).submit().get().second().response(), KeeperException.Code.NONODE);
                        parent = materializer.get(entity.path());
                        if (parent == null) {
                            return null;
                        }
                    }
                    Materializer.MaterializedNode child = parent.get(LABEL);
                    if (child == null) {
                        Operations.maybeError(materializer.operator().getData(ZNodeLabel.Path.of(entity.path(), LABEL)).submit().get().second().response(), KeeperException.Code.NONODE);
                        child = parent.get(LABEL);
                        if (child == null) {
                            return null;
                        }            
                    }
                    return of((ServerInetAddressView) child.get().get(), entity);
                }
                
                public static ConductorAddress get(Conductors.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(ConductorAddress.class, entity, materializer);
                }
                
                public static ConductorAddress create(ServerInetAddressView value, Conductors.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(ConductorAddress.class, value, entity, materializer);
                }
                
                public static ConductorAddress valueOf(String label, Conductors.Entity parent) {
                    return of(ServerInetAddressView.fromString(label), parent);
                }
                
                public static ConductorAddress of(ServerInetAddressView address, Conductors.Entity parent) {
                    return new ConductorAddress(address, parent);
                }
                
                public ConductorAddress(ServerInetAddressView address, Conductors.Entity parent) {
                    super(address, parent);
                }
            }
            
            @ZNode(label="backend", type=BackendView.class)
            public static class Backend extends Control.TypedValueZNode<BackendView> {
                
                public static Entity.Backend get(Conductors.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Backend.class, entity, materializer);
                }
                
                public static Entity.Backend create(BackendView value, Conductors.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Entity.Backend.class, value, entity, materializer);
                }
                
                public static Entity.Backend of(BackendView value, Conductors.Entity parent) {
                    return new Backend(value, parent);
                }

                public Backend(BackendView value, Conductors.Entity parent) {
                    super(value, parent);
                }
            }
        }
    }

    @ZNode(label="ensembles")
    public static abstract class Ensembles extends Control.ControlZNode {
        
        @ZNode
        public static class Entity extends Control.TypedLabelZNode<Identifier> {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;

            public static Entity create(EnsembleView<ServerInetAddressView> value, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                Materializer.Operator<?,?> operator = materializer.operator();
                Hash.Hashed hashed = Orchestra.Ensembles.Entity.hashOf(value);
                Orchestra.Ensembles.Entity entity = null;
                while (entity == null) {
                    Identifier id = hashed.asIdentifier();
                    entity = Orchestra.Ensembles.Entity.of(id);
                    Pair<? extends Operation.SessionRequest, ? extends Operation.SessionResponse> result = operator.create(entity.path()).submit().get();
                    Operations.maybeError(result.second().response(), KeeperException.Code.NODEEXISTS, result.toString());
                    
                    // If this identifier doesn't correspond to my ensemble, keep hashing
                    Orchestra.Ensembles.Entity.Backend ensembleBackend = Orchestra.Ensembles.Entity.Backend.create(value, entity, materializer);
                    if (! value.equals(ensembleBackend.get())) {
                        entity = null;
                        hashed = hashed.rehash();
                        continue;
                    }
                }
                return entity;
            }
            
            public static Hash.Hashed hashOf(EnsembleView<ServerInetAddressView> value) {
                return Hash.default32().apply(EnsembleView.toString(value));
            }
            
            public static Ensembles.Entity valueOf(String label) {
                return of(Identifier.valueOf(label));
            }
            
            public static Ensembles.Entity of(Identifier identifier) {
                return new Entity(identifier);
            }
            
            public Entity(Identifier identifier) {
                super(identifier);
            }
            
            @Override
            public String toString() {
                return get().toString();
            }

            @ZNode(type=EnsembleView.class)
            public static class Backend extends Control.TypedValueZNode<EnsembleView<ServerInetAddressView>> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("backend");
                
                public static Entity.Backend get(Ensembles.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Backend.class, entity, materializer);
                }
                
                public static Entity.Backend create(EnsembleView<ServerInetAddressView> value, Ensembles.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Ensembles.Entity.Backend.class, value, entity, materializer);
                }
                
                public static Entity.Backend of(EnsembleView<ServerInetAddressView> value, Ensembles.Entity parent) {
                    return new Backend(value, parent);
                }

                public Backend(EnsembleView<ServerInetAddressView> value, Ensembles.Entity parent) {
                    super(value, parent);
                }
            }
            
            @ZNode
            public static class Conductors extends Control.ControlZNode {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("conductors");
                
                public static Entity.Conductors of(Ensembles.Entity parent) {
                    return new Conductors(parent);
                }
                
                public Conductors(Ensembles.Entity parent) {
                    super(parent);
                }
                
                public List<Member> lookup(Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    Materializer.MaterializedNode parent = materializer.get(path());
                    if (parent == null || parent.isEmpty()) {
                        return get(materializer);
                    } else {
                        ImmutableList.Builder<Member> members = ImmutableList.builder();
                        for (ZNodeLabel.Component e: parent.keySet()) {
                            members.add(Member.valueOf(e.toString(), this));
                        }
                        return members.build();
                    }
                }

                public List<Member> get(Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    ImmutableList.Builder<Member> members = ImmutableList.builder();
                    Operations.maybeError(materializer.operator().getChildren(path()).submit().get().second().response(), KeeperException.Code.NODEEXISTS);
                    Materializer.MaterializedNode parent = materializer.get(path());
                    if (parent != null) {
                        for (ZNodeLabel.Component e: parent.keySet()) {
                            members.add(Member.valueOf(e.toString(), this));
                        }
                    }
                    return members.build();
                }
                
                @ZNode
                public static class Member extends Control.TypedLabelZNode<Identifier> {

                    @Label(type=LabelType.PATTERN)
                    public static final String LABEL_PATTERN = Identifier.PATTERN;

                    public static Conductors.Member valueOf(String label, Entity.Conductors parent) {
                        return of(Identifier.valueOf(label), parent);
                    }
                    
                    public static Conductors.Member of(Identifier identifier, Entity.Conductors parent) {
                        return new Member(identifier, parent);
                    }
                    
                    public Member(Identifier identifier, Entity.Conductors parent) {
                        super(identifier, parent);
                    }
                    
                    @Override
                    public String toString() {
                        return get().toString();
                    }
                }
            }
            
            @ZNode(type=Identifier.class, createMode=CreateMode.EPHEMERAL)
            public static class Leader extends Control.TypedValueZNode<Identifier> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("leader");
                
                public static Entity.Leader get(Ensembles.Entity parent, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Leader.class, parent, materializer);
                }
                
                public static Entity.Leader create(Identifier value, Ensembles.Entity parent, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Entity.Leader.class, value, parent, materializer);
                }
                
                public static class Proposer implements Callable<Entity.Leader> {

                    protected final Identifier value;
                    protected final Ensembles.Entity parent;
                    protected final Materializer<?,?> materializer;
                    
                    public Proposer(Identifier value, Ensembles.Entity parent, Materializer<?,?> materializer) {
                        this.value = value;
                        this.parent = parent;
                        this.materializer = materializer;
                    }
                    
                    @Override
                    public Leader call() throws InterruptedException, ExecutionException, KeeperException {
                        Materializer.Operator<?,?> operator = materializer.operator();
                        Entity.Leader instance = of(value, parent);
                        Entity.Leader leader = null;
                        while (leader == null) {
                            operator.create(instance.path(), instance.get()).submit();
                            operator.sync(instance.path()).submit();
                            Pair<? extends Operation.SessionRequest, ? extends Operation.SessionResponse> result = operator.getData(instance.path(), true).submit().get();
                            Operation.Response reply = Operations.maybeError(result.second().response(), KeeperException.Code.NONODE, result.toString());
                            if (! (reply instanceof Operation.Error)) {
                                Materializer.MaterializedNode node = materializer.get(instance.path());
                                if (node != null) {
                                    leader = Entity.Leader.of((Identifier) node.get().get(), parent);
                                }
                            }
                        }
                        return leader;
                    }
                    
                }
                
                public static Entity.Leader of(Identifier value, Ensembles.Entity parent) {
                    return new Leader(value, parent);
                }

                public Leader(Identifier value, Ensembles.Entity parent) {
                    super(value, parent);
                }
            }
        }
    }

    @ZNode(label="volumes")
    public static abstract class Volumes extends Control.ControlZNode {
        
        @ZNode
        public static class Entity extends Control.TypedLabelZNode<Identifier> {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;

            public static Entity create(VolumeDescriptor value, Materializer<?,?> materializer) throws KeeperException, InterruptedException, ExecutionException {
                Materializer.Operator<?,?> operator = materializer.operator();
                Hash.Hashed hashed = hashOf(value);
                Entity entity = null;
                while (entity == null) {
                    Identifier id = hashed.asIdentifier();
                    entity = Entity.of(id);
                    Pair<? extends Operation.SessionRequest, ? extends Operation.SessionResponse> result = operator.create(entity.path()).submit().get();
                    Operations.maybeError(result.second().response(), KeeperException.Code.NODEEXISTS, result.toString());
                    
                    // If this identifier doesn't correspond to the value, keep hashing
                    Orchestra.Volumes.Entity.Volume volume = Orchestra.Volumes.Entity.Volume.create(value, entity, materializer);
                    if (! value.equals(volume.get())) {
                        entity = null;
                        hashed = hashed.rehash();
                        continue;
                    }
                }
                return entity;
            }
            
            public static Hash.Hashed hashOf(VolumeDescriptor value) {
                return Hash.default32().apply(value.getRoot().toString());
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
            public static class Volume extends Control.TypedValueZNode<VolumeDescriptor> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("volume");
                
                public static Entity.Volume get(Volumes.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Volume.class, entity, materializer);
                }
                
                public static Entity.Volume create(VolumeDescriptor value, Volumes.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Entity.Volume.class, value, entity, materializer);
                }
                
                public static Entity.Volume of(VolumeDescriptor value, Volumes.Entity parent) {
                    return new Volume(value, parent);
                }

                public Volume(VolumeDescriptor value, Volumes.Entity parent) {
                    super(value, parent);
                }
            }

            @ZNode(type=Identifier.class)
            public static class Ensemble extends Control.TypedValueZNode<Identifier> {

                @Label
                public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("ensemble");
                
                public static Entity.Ensemble get(Volumes.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Ensemble.class, entity, materializer);
                }
                
                public static Entity.Ensemble create(Identifier value, Volumes.Entity entity, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Entity.Ensemble.class, value, entity, materializer);
                }
                
                public static Entity.Ensemble of(Identifier value, Volumes.Entity parent) {
                    return new Ensemble(value, parent);
                }

                public Ensemble(Identifier value, Volumes.Entity parent) {
                    super(value, parent);
                }
            }
        }            
    }
}