package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.storage.StorageClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class VolumeLeaseRenewer extends Service.Listener implements AsyncFunction<VersionedId, Boolean> {

    public static AsyncFunction<Identifier, Boolean> noSnapshot(
            final Service service,
            final Materializer<StorageZNode<?>, ?> materializer,
            final WatchListeners notifications) {
        return new AsyncFunction<Identifier, Boolean>() {
            @Override
            public ListenableFuture<Boolean> apply(final Identifier volume)
                    throws Exception {
                final ZNodePath path = StorageSchema.Safari.Volumes.Volume.Snapshot.pathOf(volume);
                return Watchers.AbsenceWatcher.listen(
                        path,
                        new Supplier<Boolean>(){
                            @Override
                            public Boolean get() {
                                return Boolean.TRUE;
                            }
                }, 
                materializer, 
                service, 
                notifications);
            }
        };
    }

    public static VolumeLeaseRenewer listen(
            final Service service,
            final ControlClientService control,
            final StorageClientService storage) {
        final AsyncFunction<Identifier, Boolean> noSnapshot = noSnapshot(
                service, 
                storage.materializer(), 
                storage.notifications());
        final VolumeLeaseRenewer listener = new VolumeLeaseRenewer(
                service, 
                control, 
                noSnapshot);
        listener.listen();
        return listener;
    }
    
    private final AsyncFunction<Identifier, Boolean> noSnapshot; 
    private final ConcurrentMap<Identifier, Pair<NoSnapshot, VolumeEntryAcceptor>> renewers;
    private final Service service;
    private final ControlClientService control;
    
    protected VolumeLeaseRenewer(
            Service service,
            ControlClientService control,
            AsyncFunction<Identifier, Boolean> noSnapshot) {
        this.renewers = new MapMaker().makeMap();
        this.service = service;
        this.control = control;
        this.noSnapshot = noSnapshot;
    }
    
    public void listen() {
        Services.listen(this, service);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ListenableFuture<Boolean> apply(
            final VersionedId volume) throws Exception {
        Pair<NoSnapshot, VolumeEntryAcceptor> renewer = renewers.get(volume.getValue());
        final ListenableFuture<Boolean> future;
        if ((renewer == null) || !renewer.second().volume().getVersion().equals(volume.getVersion())) {
            VolumeEntryAcceptor acceptor = VolumeEntryAcceptor.defaults(
                    volume, 
                    service, 
                    control);
            renewer = Pair.create((renewer == null) ? new NoSnapshot(volume.getValue()) : renewer.first(), acceptor);
            renewers.put(volume.getValue(), renewer);
            future = Futures.transform(
                    SubmittedRequests.submit(
                            control.materializer(), 
                            PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.getChildren())
                                    .apply(ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(volume.getValue(), volume.getVersion()))),
                    new Callback(volume),
                    SameThreadExecutor.getInstance());
        } else {
            future = Futures.transform(
                    renewer.second().call(),
                    renewer.first(),
                    SameThreadExecutor.getInstance());
        }
        return future;
    }
    
    @Override
    public void stopping(Service.State from) {
        renewers.clear();
    }
    
    protected final class Callback implements AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {
        
        private final VersionedId volume;

        public Callback(VersionedId volume) {
            this.volume = volume;
        }
        
        @Override
        public ListenableFuture<Boolean> apply(List<? extends Operation.ProtocolResponse<?>> input)
                throws Exception {
            for (Operation.ProtocolResponse<?> response: input) {
                Operations.unlessError(response.record());
            }
            return VolumeLeaseRenewer.this.apply(volume);
        }
    }
    
    protected final class NoSnapshot implements AsyncFunction<Optional<? extends Sequential<String,?>>,Boolean> {
        
        private final Identifier volume;
        
        public NoSnapshot(Identifier volume) {
            this.volume = volume;
        }

        @Override
        public ListenableFuture<Boolean> apply(
                Optional<? extends Sequential<String,?>> input) throws Exception {
            if (!input.isPresent()) {
                return noSnapshot.apply(volume);
            } else {
                return Futures.immediateFuture(Boolean.FALSE);
            }
        }
    };
}
