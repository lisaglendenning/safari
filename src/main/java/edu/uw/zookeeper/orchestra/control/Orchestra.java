package edu.uw.zookeeper.orchestra.control;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.Label;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.orchestra.BackendView;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.VolumeDescriptor;
import edu.uw.zookeeper.protocol.Operation;

public abstract class Orchestra extends Control.ControlZNode {
    
    @Label
    public static final ZNodeLabel.Path ROOT = ZNodeLabel.Path.of("/orchestra");
    
    @ZNode(label="conductors")
    public static abstract class Conductors extends Control.ControlZNode {

        @ZNode
        public static class Entity extends Control.TypedLabelZNode<Identifier> {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;

            public static Entity create(ServerInetAddressView value, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                Materializer.Operator operator = materializer.operator();
                Hash.Hashed hashed = Orchestra.Conductors.Entity.hashOf(value);
                Orchestra.Conductors.Entity entity = null;
                while (entity == null) {
                    Identifier id = hashed.asIdentifier();
                    entity = Orchestra.Conductors.Entity.of(id);
                    Operation.SessionResult result = operator.create(entity.path()).submit().get();
                    Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());
                    
                    Orchestra.Conductors.Entity.Address address = Orchestra.Conductors.Entity.Address.create(value, entity, materializer);
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
            
            @ZNode(label="presence", createMode=CreateMode.EPHEMERAL)
            public static class Presence extends Control.ControlZNode {

                public static Entity.Presence of(Conductors.Entity parent) {
                    return new Presence(parent);
                }
                
                public Presence(Conductors.Entity parent) {
                    super(parent);
                }
            }
            
            @ZNode(label="address", type=ServerInetAddressView.class)
            public static class Address extends Control.TypedValueZNode<ServerInetAddressView> {
                
                public static Entity.Address get(Conductors.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Address.class, entity, materializer);
                }
                
                public static Entity.Address create(ServerInetAddressView value, Conductors.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Entity.Address.class, value, entity, materializer);
                }
                
                public static Entity.Address valueOf(String label, Conductors.Entity parent) {
                    return of(ServerInetAddressView.fromString(label), parent);
                }
                
                public static Entity.Address of(ServerInetAddressView address, Conductors.Entity parent) {
                    return new Address(address, parent);
                }
                
                public Address(ServerInetAddressView address, Conductors.Entity parent) {
                    super(address, parent);
                }
            }
            
            @ZNode(label="backend", type=BackendView.class)
            public static class Backend extends Control.TypedValueZNode<BackendView> {
                
                public static Entity.Backend get(Conductors.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Backend.class, entity, materializer);
                }
                
                public static Entity.Backend create(BackendView value, Conductors.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
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

            public static Entity create(EnsembleView<ServerInetAddressView> value, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                Materializer.Operator operator = materializer.operator();
                Hash.Hashed hashed = Orchestra.Ensembles.Entity.hashOf(value);
                Orchestra.Ensembles.Entity entity = null;
                while (entity == null) {
                    Identifier id = hashed.asIdentifier();
                    entity = Orchestra.Ensembles.Entity.of(id);
                    Operation.SessionResult result = operator.create(entity.path()).submit().get();
                    Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());
                    
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

            @ZNode(label="backend", type=EnsembleView.class)
            public static class Backend extends Control.TypedValueZNode<EnsembleView<ServerInetAddressView>> {
                
                public static Entity.Backend get(Ensembles.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Backend.class, entity, materializer);
                }
                
                public static Entity.Backend create(EnsembleView<ServerInetAddressView> value, Ensembles.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Ensembles.Entity.Backend.class, value, entity, materializer);
                }
                
                public static Entity.Backend of(EnsembleView<ServerInetAddressView> value, Ensembles.Entity parent) {
                    return new Backend(value, parent);
                }

                public Backend(EnsembleView<ServerInetAddressView> value, Ensembles.Entity parent) {
                    super(value, parent);
                }
            }
            
            @ZNode(label="conductors")
            public static class Conductors extends Control.ControlZNode {

                public static Entity.Conductors of(Ensembles.Entity parent) {
                    return new Conductors(parent);
                }
                
                public Conductors(Ensembles.Entity parent) {
                    super(parent);
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
            
            @ZNode(label="leader", type=Identifier.class, createMode=CreateMode.EPHEMERAL)
            public static class Leader extends Control.TypedValueZNode<Identifier> {

                public static Entity.Leader get(Ensembles.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Leader.class, entity, materializer);
                }
                
                public static Entity.Leader create(Identifier value, Ensembles.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Entity.Leader.class, value, entity, materializer);
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

            public static Entity create(VolumeDescriptor value, Materializer materializer) throws KeeperException, InterruptedException, ExecutionException {
                Materializer.Operator operator = materializer.operator();
                Hash.Hashed hashed = hashOf(value);
                Entity entity = null;
                while (entity == null) {
                    Identifier id = hashed.asIdentifier();
                    entity = Entity.of(id);
                    Operation.SessionResult result = operator.create(entity.path()).submit().get();
                    Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());
                    
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
            
            @ZNode(label="volume", type=VolumeDescriptor.class)
            public static class Volume extends Control.TypedValueZNode<VolumeDescriptor> {
                
                public static Entity.Volume get(Volumes.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Volume.class, entity, materializer);
                }
                
                public static Entity.Volume create(VolumeDescriptor value, Volumes.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return create(Entity.Volume.class, value, entity, materializer);
                }
                
                public static Entity.Volume of(VolumeDescriptor value, Volumes.Entity parent) {
                    return new Volume(value, parent);
                }

                public Volume(VolumeDescriptor value, Volumes.Entity parent) {
                    super(value, parent);
                }
            }

            @ZNode(label="ensemble", type=Identifier.class)
            public static class Ensemble extends Control.TypedValueZNode<Identifier> {

                public static Entity.Ensemble get(Volumes.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
                    return get(Entity.Ensemble.class, entity, materializer);
                }
                
                public static Entity.Ensemble create(Identifier value, Volumes.Entity entity, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
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