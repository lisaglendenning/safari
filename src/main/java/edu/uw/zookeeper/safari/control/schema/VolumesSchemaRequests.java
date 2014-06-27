package edu.uw.zookeeper.safari.control.schema;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.volume.BoundVolumeOperator;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;

public class VolumesSchemaRequests<O extends Operation.ProtocolResponse<?>> {

    public static <O extends Operation.ProtocolResponse<?>> VolumesSchemaRequests<O> create(
            Materializer<ControlZNode<?>,O> materializer) {
        return new VolumesSchemaRequests<O>(materializer);
    }
    
    protected final Materializer<ControlZNode<?>,O> materializer;
    
    protected VolumesSchemaRequests(
            Materializer<ControlZNode<?>,O> materializer) {
        this.materializer = materializer;
    }

    public AbsoluteZNodePath getPath() {
        return ControlSchema.Safari.Volumes.PATH;
    }
    
    public VolumeSchemaRequests volume(final Identifier volume) {
        return new VolumeSchemaRequests(volume);
    }
    
    public VolumeSchemaRequests.VolumeVersionSchemaRequests version(final VersionedId volume) {
        return volume(volume.getValue()).version(volume.getVersion());
    }
    
    public Materializer<ControlZNode<?>,O> getMaterializer() {
        return materializer;
    }

    public ImmutableList<Records.Request> children(boolean watch) {
        final ZNodePath path = getPath();
        final Records.Request sync = Operations.Requests.sync().setPath(path).build();
        final Records.Request getChildren = 
                Operations.Requests.getChildren().setPath(path).setWatch(watch).build();
        return ImmutableList.of(sync, getChildren);
    }
    
    public ImmutableList<Records.Request> watchChildren() {
        return children(true);
    }
    
    public ImmutableList<Records.Request> children() {
        return children(false);
    }
    
    public class VolumeSchemaRequests {
        protected final Identifier volume;
        
        public VolumeSchemaRequests(Identifier volume) {
            this.volume = volume;
        }
        
        public Identifier getVolume() {
            return volume;
        }
        
        public VolumesSchemaRequests<O> volumes() {
            return VolumesSchemaRequests.this;
        }

        public AbsoluteZNodePath getPath() {
            return ControlSchema.Safari.Volumes.Volume.pathOf(
                    volume);
        }
        
        public ImmutableList<Records.MultiOpRequest> create(
                final ZNodePath path) {
            final Records.MultiOpRequest createVolume = 
                    (Records.MultiOpRequest) materializer.create(
                            ControlSchema.Safari.Volumes.Volume.pathOf(volume)).get().build();
            final Records.MultiOpRequest createPath = 
                    (Records.MultiOpRequest) materializer.create(
                            ControlSchema.Safari.Volumes.Volume.Path.pathOf(volume),
                            path).get().build();
            final Records.MultiOpRequest createLog = 
                    (Records.MultiOpRequest) materializer.create(
                            ControlSchema.Safari.Volumes.Volume.Log.pathOf(volume)).get().build();
            return ImmutableList.of(createVolume, createPath, createLog);
        }

        public ImmutableList<Records.MultiOpRequest> delete() {
            final AbsoluteZNodePath path = getPath();
            final Operations.Requests.Delete delete = Operations.Requests.delete();
            return ImmutableList.<Records.MultiOpRequest>of(
                    delete.setPath(path.join(ControlSchema.Safari.Volumes.Volume.Log.LABEL)).build(),
                    delete.setPath(path.join(ControlSchema.Safari.Volumes.Volume.Path.LABEL)).build(),
                    delete.setPath(path).build());
        }
        
        public VolumeVersionSchemaRequests version(
                final UnsignedLong version) {
            return new VolumeVersionSchemaRequests(version);
        }

        public ImmutableList<Records.Request> exists() {
            final ZNodePath path = getPath();
            final Records.Request sync = Operations.Requests.sync().setPath(path).build();
            final Records.Request exists = 
                    Operations.Requests.exists().setPath(path).build();
            return ImmutableList.of(sync, exists);
        }
        
        public ImmutableList<Records.Request> children(boolean watch) {
            final ZNodePath path = getPath();
            final Records.Request sync = Operations.Requests.sync().setPath(path).build();
            final Records.Request getChildren = 
                    Operations.Requests.getChildren().setPath(path).setWatch(watch).build();
            return ImmutableList.of(sync, getChildren);
        }
        
        public ImmutableList<Records.Request> watchChildren() {
            return children(true);
        }
        
        public ImmutableList<Records.Request> children() {
            return children(false);
        }

        public VolumeLatestSchemaRequests latest() {
            return new VolumeLatestSchemaRequests();
        }

        public VolumePathSchemaRequests path() {
            return new VolumePathSchemaRequests();
        }
        
        public class VolumePathSchemaRequests {

            public VolumePathSchemaRequests() {}

            public AbsoluteZNodePath getPath() {
                return ControlSchema.Safari.Volumes.Volume.Path.pathOf(
                        volume);
            }

            public ImmutableList<Records.Request> get() {
                ZNodePath path = getPath();
                return ImmutableList.<Records.Request>of(
                        Operations.Requests.sync().setPath(path).build(),
                        Operations.Requests.getData().setPath(path).build());
            }
        }
        
        public class VolumeLatestSchemaRequests {

            public VolumeLatestSchemaRequests() {}

            public AbsoluteZNodePath getPath() {
                return ControlSchema.Safari.Volumes.Volume.Log.Latest.pathOf(
                        volume);
            }

            public ImmutableList<Records.Request> get() {
                ZNodePath path = getPath();
                return ImmutableList.<Records.Request>of(
                        Operations.Requests.sync().setPath(path).build(),
                        Operations.Requests.getData().setPath(path).build());
            }
        }
        
        public class VolumeVersionSchemaRequests {
            protected final UnsignedLong version;
            
            public VolumeVersionSchemaRequests(UnsignedLong version) {
                this.version = version;
            }
            
            public UnsignedLong getVersion() {
                return version;
            }
            
            public AbsoluteZNodePath getPath() {
                return ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(
                        volume, version);
            }
            
            public VolumeSchemaRequests volume() {
                return VolumeSchemaRequests.this;
            }
            
            public ImmutableList<Records.Request> children(boolean watch) {
                final ZNodePath path = getPath();
                final Records.Request sync = Operations.Requests.sync().setPath(path).build();
                final Records.Request getChildren = 
                        Operations.Requests.getChildren().setPath(path).setWatch(watch).build();
                return ImmutableList.of(sync, getChildren);
            }
            
            public ImmutableList<Records.Request> watchChildren() {
                return children(true);
            }
            
            public ImmutableList<Records.Request> children() {
                return children(false);
            }
            
            public VolumeLatestSchemaRequests latest() {
                return new VolumeLatestSchemaRequests();
            }

            public ImmutableList<Records.MultiOpRequest> create(Optional<RegionAndLeaves> state) {
                final ZNodePath path = getPath();
                final Records.MultiOpRequest createVersion = 
                        (Records.MultiOpRequest) materializer.create(
                                path).get().build();
                final Records.MultiOpRequest createState = 
                        (Records.MultiOpRequest) materializer.create(
                                path.join(ControlSchema.Safari.Volumes.Volume.Log.Version.State.LABEL), 
                                state.orNull()).get().build();
                return ImmutableList.of(createVersion, createState);
            }

            public Records.MultiOpRequest logOperator(BoundVolumeOperator<?> operator) {
                return log(OperatorVolumeLogEntry.create(operator));
            }

            public Records.MultiOpRequest logLinkTo(VolumeLogEntryPath toEntry) {
                return logLink(toEntry.path().relative(ControlSchema.Safari.Volumes.PATH));
            }

            public Records.MultiOpRequest logLink(RelativeZNodePath link) {
                return log(LinkVolumeLogEntry.create(link));
            }
            
            public Records.MultiOpRequest log(VolumeLogEntry<?> entry) {
                return (Records.MultiOpRequest) materializer.create(
                                ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.pathOf(
                                        volume, version), entry)
                                    .get().build();
            }
            
            public VolumeLeaseSchemaRequests lease() {
                return new VolumeLeaseSchemaRequests();
            }
            
            public VolumeLogEntrySchemaRequests entry(Sequential<String, ?> entry) {
                return new VolumeLogEntrySchemaRequests(entry);
            }
            
            public VolumeStateSchemaRequests state() {
                return new VolumeStateSchemaRequests();
            }
            
            public class VolumeStateSchemaRequests {

                public AbsoluteZNodePath getPath() {
                    return ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(
                            volume, version);
                }

                public ImmutableList<Records.Request> get() {
                    ZNodePath path = getPath();
                    return ImmutableList.<Records.Request>of(
                            Operations.Requests.sync().setPath(path).build(),
                            Operations.Requests.getData().setPath(path).build());
                }
            }
            
            public class VolumeLatestSchemaRequests {

                public VolumeLatestSchemaRequests() {}

                public AbsoluteZNodePath getPath() {
                    return ControlSchema.Safari.Volumes.Volume.Log.Latest.pathOf(
                            volume);
                }

                public Records.MultiOpRequest create() {
                    return (Records.MultiOpRequest) materializer.create(
                                    getPath(), version)
                                        .get().build();
                }
                
                public Records.MultiOpRequest update() {
                    return (Records.MultiOpRequest) materializer.setData(
                                    getPath(), version)
                                        .get().build();
                }
            }

            public class VolumeLeaseSchemaRequests {
                
                public VolumeLeaseSchemaRequests() {}
                
                public AbsoluteZNodePath getPath() {
                    return ControlSchema.Safari.Volumes.Volume.Log.Version.Lease.pathOf(
                            volume, version);
                }

                public Records.MultiOpRequest create(UnsignedLong duration) {
                    return (Records.MultiOpRequest) materializer.create(
                            getPath(), duration)
                                .get().build();
                }
                
                public Records.MultiOpRequest update(UnsignedLong duration) {
                    return (Records.MultiOpRequest) materializer.setData(
                            getPath(), duration)
                                .get().build();
                }
                
                public ImmutableList<Records.Request> get() {
                    AbsoluteZNodePath path = getPath();
                    return ImmutableList.<Records.Request>of(
                            Operations.Requests.sync().setPath(path).build(),
                            Operations.Requests.getData().setPath(path).build());
                }
            }
            
            public class VolumeLogEntrySchemaRequests {
                protected final Sequential<String, ?> entry;
                
                public VolumeLogEntrySchemaRequests(Sequential<String, ?> entry) {
                    this.entry = entry;
                }
                
                public AbsoluteZNodePath getPath() {
                    return ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.pathOf(
                            volume, version, entry);
                }
                
                public VolumeLogEntryVoteSchemaRequests vote() {
                    return new VolumeLogEntryVoteSchemaRequests();
                }
                
                public VolumeLogEntryCommitSchemaRequests commit() {
                    return new VolumeLogEntryCommitSchemaRequests();
                }
                
                public class VolumeLogEntryVoteSchemaRequests {
                    
                    public AbsoluteZNodePath getPath() {
                        return VolumeLogEntrySchemaRequests.this.getPath().join(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote.LABEL);
                    }
                    
                    public Records.MultiOpRequest create(Boolean vote) {
                        return (Records.MultiOpRequest) materializer.create(
                                        getPath(), vote)
                                            .get().build();
                    }
                    
                    public ImmutableList<Records.Request> get() {
                        AbsoluteZNodePath path = getPath();
                        return ImmutableList.<Records.Request>of(
                                Operations.Requests.sync().setPath(path).build(),
                                Operations.Requests.getData().setPath(path).build());
                    }
                }
                
                public class VolumeLogEntryCommitSchemaRequests {
                    
                    public AbsoluteZNodePath getPath() {
                        return VolumeLogEntrySchemaRequests.this.getPath().join(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit.LABEL);
                    }
                    
                    public Records.MultiOpRequest create(Boolean commit) {
                        return (Records.MultiOpRequest) materializer.create(
                                        getPath(), commit)
                                            .get().build();
                    }
                    
                    public ImmutableList<Records.Request> get() {
                        AbsoluteZNodePath path = getPath();
                        return ImmutableList.<Records.Request>of(
                                Operations.Requests.sync().setPath(path).build(),
                                Operations.Requests.getData().setPath(path).build());
                    }
                }
            }
        }
    }
}
