package edu.uw.zookeeper.safari.frontend;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.Call;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.peer.Peer;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnections;
import edu.uw.zookeeper.safari.region.Region;

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
                ControlClientService control,
                final ClientPeerConnections connections,
                ServiceMonitor monitor) {
            RegionsConnectionsService instance = RegionsConnectionsService.defaults(
                        connections.asLookup(),
                        peer,
                        region, 
                        control.materializer(),
                        control.cacheEvents(),
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
            Materializer<ControlZNode<?>,?> control,
            WatchListeners watch,
            Iterable<? extends Service.Listener> listeners) {
        AsyncFunction<Identifier, Identifier> selector = 
                SelectSelfTask.defaults(myId, myRegion, control);
        RegionsConnectionsService instance = new RegionsConnectionsService(
                myRegion,
                selector,
                connector,
                listeners);
        instance.new RegionCreatedListener(control.cache(), watch).listen();
        newRegionDirectoryWatcher(instance, watch, control);
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

    public static Watchers.RunnableWatcher<?> newRegionDirectoryWatcher(
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,?,?> client) {
        final WatchMatcher matcher = WatchMatcher.exact(
                ControlSchema.Safari.Regions.PATH,
                Watcher.Event.EventType.NodeCreated,
                Watcher.Event.EventType.NodeChildrenChanged);
        final FixedQuery<?> query = FixedQuery.forRequests(client, 
                Operations.Requests.sync().setPath(matcher.getPath()).build(),
                Operations.Requests.getChildren().setPath(matcher.getPath()).setWatch(true).build());
        return Watchers.RunnableWatcher.listen(Call.create(query), service, watch, matcher);
    }
    
    private final Identifier region;
    private final ConcurrentMap<Identifier, RegionClientPeerConnection> regions;
    private final AsyncFunction<Identifier, Identifier> selector;
    private final AsyncFunction<Identifier, ClientPeerConnection<?>> connector;
    
    protected RegionsConnectionsService(
            Identifier region,
            AsyncFunction<Identifier, Identifier> selector,
            AsyncFunction<Identifier, ClientPeerConnection<?>> connector,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.regions = new MapMaker().makeMap();
        this.region = region;
        this.selector = selector;
        this.connector = connector;
    }
    
    public ConcurrentMap<Identifier, RegionClientPeerConnection> regions() {
        return regions;
    }
    
    @Override
    public RegionClientPeerConnection apply(Identifier region) {
        if (region.equals(Identifier.zero())) {
            region = this.region;
        }
        RegionClientPeerConnection connection = regions.get(region);
        if (connection == null) {
            synchronized (regions) {
                connection = regions.get(region);
                if (connection == null) {
                    connection = RegionClientPeerConnection.newInstance(region, selector, connector);
                    regions.put(region, connection);
                }
            }
        }
        return connection;
    }
    
    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }

    public static class SelectRandom<V> implements Function<List<V>, V> {
        
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
    
    public static class SelectSelfTask implements AsyncFunction<Identifier, Identifier> {

        public static SelectSelfTask defaults(
                Identifier myId,
                Identifier myRegion,
                Materializer<ControlZNode<?>,?> materializer) {
            return new SelectSelfTask(myId, myRegion, 
                    SelectMemberTask.defaults(materializer));
        }
        
        protected final Identifier myId;
        protected final Identifier myRegion;
        protected final AsyncFunction<Identifier, Identifier> fallback;
        
        public SelectSelfTask(
                Identifier myId,
                Identifier myEnsemble,
                AsyncFunction<Identifier, Identifier> fallback) {
            this.myId = myId;
            this.myRegion = myEnsemble;
            this.fallback = fallback;
        }

        @Override
        public ListenableFuture<Identifier> apply(Identifier region)
                throws Exception {
            return region.equals(myRegion) ?
                    Futures.immediateFuture(myId) : 
                        fallback.apply(region);
        }
    }
    
    public static class SelectMemberTask<T> implements AsyncFunction<Identifier, Identifier> {

        public static SelectMemberTask<List<Identifier>> defaults(Materializer<ControlZNode<?>,?> materializer) {
            return new SelectMemberTask<List<Identifier>>(getRegionMembers(materializer),
                    SelectPresentMemberTask.defaults(materializer));
        }
        
        protected final CachedFunction<Identifier, ? extends T> memberLookup;
        protected final AsyncFunction<? super T, Identifier> selector;
        
        public SelectMemberTask(
                CachedFunction<Identifier, ? extends T> memberLookup,
                AsyncFunction<? super T, Identifier> selector) {
            this.memberLookup = memberLookup;
            this.selector = selector;
        }

        @Override
        public ListenableFuture<Identifier> apply(Identifier ensemble)
                throws Exception {
            // never used cached members
            return Futures.transform(
                    memberLookup.async().apply(ensemble), 
                    selector, 
                    SameThreadExecutor.getInstance());
        }
    }

    public static class SelectPresentMemberTask implements AsyncFunction<List<Identifier>, Identifier> {

        public static SelectPresentMemberTask defaults(
                ClientExecutor<? super Records.Request, ?, ?> client) {
            return new SelectPresentMemberTask(
                    client,
                    SelectRandom.<Identifier>create());
        }

        protected final Logger logger;
        protected final ClientExecutor<? super Records.Request, ?, ?> client;
        protected final Function<List<Identifier>, Identifier> selector;
        
        public SelectPresentMemberTask(
                ClientExecutor<? super Records.Request, ?, ?> client,
                Function<List<Identifier>, Identifier> selector) {
            this.logger = LogManager.getLogger(getClass());
            this.client = client;
            this.selector = selector;
        }
        
        @Override
        public ListenableFuture<Identifier> apply(
                List<Identifier> members) {
            logger.debug("Selecting from region members {}", members);
            List<ListenableFuture<? extends Operation.ProtocolResponse<?>>> presence = Lists.newArrayListWithCapacity(members.size());
            Operations.Requests.Exists exists = Operations.Requests.exists();
            for (Identifier member: members) {
                presence.add(client.submit(exists.setPath(ControlSchema.Safari.Peers.Peer.Presence.pathOf(member)).build()));
            }
            ListenableFuture<List<Operation.ProtocolResponse<?>>> future = Futures.successfulAsList(presence);
            return Futures.transform(future, 
                    SelectPresentMemberFunction.defaults(members, selector), 
                    SameThreadExecutor.getInstance());
        }
    }
    
    public static class SelectPresentMemberFunction implements Function<List<Operation.ProtocolResponse<?>>, Identifier> {

        public static SelectPresentMemberFunction defaults(
                List<Identifier> members,
                Function<List<Identifier>, Identifier> selector) {
            return new SelectPresentMemberFunction(members, selector);
        }
        
        protected final Logger logger;
        protected final List<Identifier> members;
        protected final Function<List<Identifier>, Identifier> selector;
        
        public SelectPresentMemberFunction(
                List<Identifier> members,
                Function<List<Identifier>, Identifier> selector) {
            this.logger = LogManager.getLogger(getClass());
            this.members = members;
            this.selector = selector;
        }
        
        @Override
        public Identifier apply(List<Operation.ProtocolResponse<?>> presence) {
            List<Identifier> living = Lists.newArrayListWithCapacity(members.size());
            for (int i=0; i<members.size(); ++i) {
                try {
                    if (Boolean.TRUE.equals(presence.get(i))) {
                        living.add(members.get(i));
                    }
                } catch (Exception e) {}
            }
            Identifier selected = selector.apply(living);
            logger.info("Selected {} from live region members: {}", selected, living);
            return (selected == null) ? null : selected;
        }
    }
    
    protected class RegionCreatedListener extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> {

        public RegionCreatedListener(
                LockableZNodeCache<ControlZNode<?>,Records.Request,?> cache,
                WatchListeners watch) {
            super(ControlSchema.Safari.Regions.Region.PATH, cache, RegionsConnectionsService.this, watch);
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            if (service.isRunning()) {
                Identifier region = Identifier.valueOf(((AbsoluteZNodePath) event.getPath()).label().toString());
                apply(region);
            }
        }
    }
}
