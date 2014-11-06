package edu.uw.zookeeper.safari.frontend;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.peer.Peer;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnections;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public class RegionsConnectionsService extends ServiceListenersService implements AsyncFunction<Identifier, ClientPeerConnection<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}

        @Provides @Singleton
        public RegionsConnectionsService getRegionsConnectionsService(
                @Peer Identifier peer,
                @Region Identifier region,
                SchemaClientService<ControlZNode<?>,?> control,
                final ClientPeerConnections connections,
                final DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Regions.Region> regions,
                ScheduledExecutorService scheduler,
                ServiceMonitor monitor) {
            RegionsConnectionsService instance = RegionsConnectionsService.defaults(
                        connections.asLookup(),
                        peer,
                        region, 
                        control,
                        regions,
                        scheduler,
                        // TODO configurable
                        TimeValue.seconds(10L),
                        ImmutableList.of(new Service.Listener() {
                            @Override
                            public void starting() {
                                Services.startAndWait(connections);
                            }
                        }));
            monitor.add(instance);
            return instance;
        }

        @Override
        protected void configure() {
        }
    }

    public static RegionsConnectionsService defaults(
            AsyncFunction<Identifier, ClientPeerConnection<?>> connector,
            Identifier myId,
            Identifier myRegion,
            SchemaClientService<ControlZNode<?>,?> client,
            final DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Regions.Region> entries,
            final ScheduledExecutorService scheduler,
            final TimeValue timeOut,
            Iterable<? extends Service.Listener> listeners) {
        final Logger logger = LogManager.getLogger(RegionsConnectionsService.class);
        final RegionConnections regions = RegionConnections.create(
                myRegion, 
                SelectSelfTask.defaults(myId, myRegion, client.materializer(), logger), 
                connector,
                new Function<Runnable, ScheduledFuture<?>>() {
                    @Override
                    public ScheduledFuture<?> apply(Runnable input) {
                        return scheduler.schedule(input, timeOut.value(), timeOut.unit());
                    }
                });
        final RegionsConnectionsService instance = new RegionsConnectionsService(
                regions,
                listeners);
        RegionCreatedListener.listen(entries, client.materializer().cache().cache(), instance);
        return instance;
    }
    
    public static CachedFunction<Identifier, List<Identifier>> getRegionMembers(
            final Materializer<ControlZNode<?>,?> materializer) {
        final Function<Identifier, List<Identifier>> cached = new Function<Identifier, List<Identifier>>() {
            @Override
            @Nullable
            public
            List<Identifier> apply(final Identifier region) {
                materializer.cache().lock().readLock().lock();
                try {
                    ControlSchema.Safari.Regions.Region.Members node = 
                            (ControlSchema.Safari.Regions.Region.Members) materializer.cache().cache().get(ControlSchema.Safari.Regions.Region.Members.pathOf(region));
                    if (node == null) {
                        return null;
                    }
                    ImmutableList.Builder<Identifier> members = ImmutableList.builder();
                    for (ZNodeName child: node.keySet()) {
                        members.add(Identifier.valueOf(child.toString()));
                    }
                    return members.build();
                } finally {
                    materializer.cache().lock().readLock().unlock();
                }
            }
        };
        final AsyncFunction<Identifier, List<Identifier>> lookup = new AsyncFunction<Identifier, List<Identifier>>() {
            @Override
            public ListenableFuture<List<Identifier>> apply(final Identifier region) {
                final ZNodePath path = ControlSchema.Safari.Regions.Region.Members.pathOf(region);
                materializer.sync(path).call();
                return Futures.transform(
                        materializer.getChildren(path).call(),
                        new AsyncFunction<Operation.ProtocolResponse<?>, List<Identifier>>() {
                            @Override
                            @Nullable
                            public ListenableFuture<List<Identifier>> apply(Operation.ProtocolResponse<?> input) throws KeeperException {
                                Operations.unlessError(input.record());
                                return Futures.immediateFuture(cached.apply(region));
                            }
                        });
            }
        };
        return CachedFunction.create(cached, lookup, LogManager.getLogger(RegionsConnectionsService.class));
    }
    
    private final RegionConnections regions;
    
    protected RegionsConnectionsService(
            RegionConnections regions,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.regions = regions;
    }
    
    public RegionConnections regions() {
        return regions;
    }
    
    @Override
    public RegionClientPeerConnection apply(Identifier region) {
        switch (state()) {
        case FAILED:
            throw new IllegalStateException(failureCause());
        case STARTING:
        case RUNNING:
            return regions.apply(region);
        default:
            throw new IllegalStateException();
        }
    }
    
    @Override
    protected Executor executor() {
        return MoreExecutors.directExecutor();
    }
    
    protected static final class RegionConnections implements Function<Identifier, RegionClientPeerConnection>, Supplier<ConcurrentMap<Identifier, RegionClientPeerConnection>> {

        public static RegionConnections create(
                Identifier region,
                AsyncFunction<Identifier, Optional<Identifier>> selector,
                AsyncFunction<Identifier, ClientPeerConnection<?>> connector,
                Function<? super Runnable, ? extends ScheduledFuture<?>> schedule) {
            ConcurrentMap<Identifier, RegionClientPeerConnection> regions = new MapMaker().makeMap();
            return new RegionConnections(region, selector, connector, schedule, regions);
        }
        
        private final Identifier region;
        private final ConcurrentMap<Identifier, RegionClientPeerConnection> regions;
        private final AsyncFunction<Identifier, Optional<Identifier>> selector;
        private final AsyncFunction<Identifier, ClientPeerConnection<?>> connector;
        private final Function<? super Runnable, ? extends ScheduledFuture<?>> schedule;
        
        protected RegionConnections(
                Identifier region,
                AsyncFunction<Identifier, Optional<Identifier>> selector,
                AsyncFunction<Identifier, ClientPeerConnection<?>> connector,
                Function<? super Runnable, ? extends ScheduledFuture<?>> schedule,
                ConcurrentMap<Identifier, RegionClientPeerConnection> regions) {
            this.regions = regions;
            this.region = region;
            this.selector = selector;
            this.connector = connector;
            this.schedule = schedule;
        }
        
        @Override
        public RegionClientPeerConnection apply(Identifier region) {
            if (region.equals(Identifier.zero())) {
                region = this.region;
            }
            RegionClientPeerConnection connection = regions.get(region);
            if (connection == null) {
                connection = RegionClientPeerConnection.create(region, selector, connector, schedule);
                if (regions.putIfAbsent(region, connection) == null) {
                    connection.run();
                } else {
                    return apply(region);
                }
            }
            return connection;
        }

        @Override
        public ConcurrentMap<Identifier, RegionClientPeerConnection> get() {
            return regions;
        }
    }

    protected static final class RegionCreatedListener extends Watchers.StopServiceOnFailure<ControlSchema.Safari.Regions.Region,RegionsConnectionsService> {
    
        public static RegionCreatedListener listen(
                final DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Regions.Region> entries,
                final NameTrie<ControlZNode<?>> trie,
                final RegionsConnectionsService service) {
            final RegionCreatedListener instance = new RegionCreatedListener(service);
            service.addListener(new Service.Listener() {
                        @Override
                        public void running() {
                            DirectoryEntryListener.entryCreatedCallback(
                                    Watchers.EventToPathCallback.create(
                                            Watchers.PathToNodeCallback.create(
                                                    instance, trie)), 
                                    entries);
                        }
                    }, 
                    MoreExecutors.directExecutor());
            return instance;
        }
        
        protected RegionCreatedListener(
                RegionsConnectionsService service) {
            super(service, service.logger());
        }
    
        @Override
        public void onSuccess(ControlSchema.Safari.Regions.Region result) {
            if (result != null) {
                try {
                    service().apply(result.name());
                } catch (IllegalStateException e) {}
            }
        }
    }

    public static final class SelectRandom<V> implements Function<List<V>, V> {
        
        public static <V> SelectRandom<V> create() {
            return new SelectRandom<V> ();
        }
        
        private final Random random;
        
        public SelectRandom() {
            this.random = new Random();
        }
    
        @Override
        @Nullable
        public V apply(List<V> input) {
            if (input.isEmpty()) {
                return null;
            } else {
                int index = random.nextInt(input.size());
                return input.get(index);
            }
        }
    }
    
    public static final class SelectSelfTask implements AsyncFunction<Identifier, Optional<Identifier>> {

        public static SelectSelfTask defaults(
                Identifier myId,
                Identifier myRegion,
                Materializer<ControlZNode<?>,?> materializer,
                Logger logger) {
            return new SelectSelfTask(myId, myRegion, 
                    SelectMemberTask.defaults(materializer, logger));
        }
        
        protected final Identifier myId;
        protected final Identifier myRegion;
        protected final AsyncFunction<Identifier, Optional<Identifier>> fallback;
        
        public SelectSelfTask(
                Identifier myId,
                Identifier myEnsemble,
                AsyncFunction<Identifier, Optional<Identifier>> fallback) {
            this.myId = myId;
            this.myRegion = myEnsemble;
            this.fallback = fallback;
        }

        @Override
        public ListenableFuture<Optional<Identifier>> apply(Identifier region)
                throws Exception {
            return region.equals(myRegion) ?
                    Futures.immediateFuture(Optional.of(myId)) : 
                        fallback.apply(region);
        }
    }
    
    public static final class SelectMemberTask<T> implements AsyncFunction<Identifier, Optional<Identifier>> {

        public static SelectMemberTask<List<Identifier>> defaults(
                Materializer<ControlZNode<?>,?> materializer,
                Logger logger) {
            return new SelectMemberTask<List<Identifier>>(
                    getRegionMembers(materializer),
                    SelectPresentMemberTask.random(materializer, logger));
        }
        
        protected final CachedFunction<Identifier, ? extends T> memberLookup;
        protected final AsyncFunction<? super T, Optional<Identifier>> selector;
        
        public SelectMemberTask(
                CachedFunction<Identifier, ? extends T> memberLookup,
                AsyncFunction<? super T, Optional<Identifier>> selector) {
            this.memberLookup = memberLookup;
            this.selector = selector;
        }

        @Override
        public ListenableFuture<Optional<Identifier>> apply(Identifier ensemble)
                throws Exception {
            // never used cached members
            return Futures.transform(
                    memberLookup.async().apply(ensemble), 
                    selector);
        }
    }

    public static final class SelectPresentMemberTask implements AsyncFunction<List<Identifier>, Optional<Identifier>> {

        public static SelectPresentMemberTask random(
                ClientExecutor<? super Records.Request, ?, ?> client,
                Logger logger) {
            return new SelectPresentMemberTask(
                    client,
                    SelectRandom.<Identifier>create(),
                    logger);
        }

        protected final Logger logger;
        protected final ClientExecutor<? super Records.Request, ?, ?> client;
        protected final Function<List<Identifier>, Identifier> selector;
        
        protected SelectPresentMemberTask(
                ClientExecutor<? super Records.Request, ?, ?> client,
                Function<List<Identifier>, Identifier> selector,
                Logger logger) {
            this.logger = logger;
            this.client = client;
            this.selector = selector;
        }
        
        @Override
        public ListenableFuture<Optional<Identifier>> apply(
                List<Identifier> members) {
            logger.debug("Selecting from region members {}", members);
            return SelectPresentMemberFunction.submit(members, selector, client, logger);
        }
    }
    
    public static final class SelectPresentMemberFunction<O extends Operation.ProtocolResponse<?>> extends ForwardingListenableFuture<List<O>> implements Callable<Optional<Optional<Identifier>>>, Runnable {

        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<Identifier>> submit(
                List<Identifier> members,
                Function<List<Identifier>, Identifier> selector,
                ClientExecutor<? super Records.Request, O, ?> client,
                Logger logger) {
            final Promise<Optional<Identifier>> promise = SettableFuturePromise.create();
            @SuppressWarnings("unchecked")
            final PathToRequests toRequests = PathToRequests.forRequests(Operations.Requests.sync(), Operations.Requests.exists());
            final ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
            for (Identifier member: members) {
                requests.addAll(toRequests.apply(ControlSchema.Safari.Peers.Peer.Presence.pathOf(member)));
            }
            new SelectPresentMemberFunction<O>(
                    members, 
                    selector, 
                    SubmittedRequests.submit(client, requests.build()), 
                    logger, 
                    promise);
            return promise;
        }
        
        private final Logger logger;
        private final List<Identifier> members;
        private final Function<List<Identifier>, Identifier> selector;
        private final SubmittedRequests<? extends Records.Request,O> future;
        private final CallablePromiseTask<?,Optional<Identifier>> delegate;
        
        protected SelectPresentMemberFunction(
                List<Identifier> members,
                Function<List<Identifier>, Identifier> selector,
                SubmittedRequests<? extends Records.Request,O> future,
                Logger logger,
                Promise<Optional<Identifier>> promise) {
            this.future = future;
            this.logger = logger;
            this.members = members;
            this.selector = selector;
            this.delegate = CallablePromiseTask.listen(this, promise);
        }

        @Override
        public void run() {
            delegate.run();
        }

        @Override
        public Optional<Optional<Identifier>> call() throws Exception {
            if (isDone()) {
                final List<O> responses = get();
                final List<Identifier> living = Lists.newArrayListWithCapacity(members.size());
                Iterator<O> response = responses.iterator();
                for (Identifier member: members) {
                    if (!Operations.maybeError(Iterators.get(response, 1).record(), KeeperException.Code.NONODE).isPresent()) {
                        living.add(member);
                    }
                }
                final Identifier selected;
                if (living.isEmpty()) {
                    selected = null;
                    logger.info("No live region members: {}", members);
                } else {
                    selected = selector.apply(living);
                    logger.info("Selected {} from live region members: {}", selected, living);
                }
                return Optional.of(Optional.fromNullable(selected));
            }
            return Optional.absent();
        }
        
        @Override
        public String toString() {
            return ToStringListenableFuture.toString(this);
        }

        @Override
        protected ListenableFuture<List<O>> delegate() {
            return future;
        }
    }
}
