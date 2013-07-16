package edu.uw.zookeeper.orchestra.backend;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.net.intravm.IntraVmTest;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.VolumeLookup;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.util.EventBusPublisher;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;

@RunWith(JUnit4.class)
public class ShardedClientConnectionExecutorTest {

    public static IntraVmTest.EndpointFactory<InetSocketAddress> endpointFactory() {
        Reference<? extends Executor> executors = Factories.holderOf(MoreExecutors.sameThreadExecutor());
        Factory<? extends Publisher> publishers = new Factory<EventBusPublisher>() {
            @Override
            public EventBusPublisher get() {
                return EventBusPublisher.newInstance();
            }
        };
        return IntraVmTest.EndpointFactory.newInstance(
                IntraVmTest.loopbackAddresses(1),
                publishers, executors);
    }
    
    @Test
    public void test() {
        
        final VolumeLookup volumes = VolumeLookup.newInstance();
        Function<ZNodeLabel.Path, Identifier> lookup = new Function<ZNodeLabel.Path, Identifier>() {
            @Override
            public Identifier apply(ZNodeLabel.Path input) {
                return volumes.get(input).getVolume().getId();
            }
        };
        VolumeShardedOperationTranslators translator = new VolumeShardedOperationTranslators(
                new Function<Identifier, Volume>() {
                    @Override
                    public Volume apply(Identifier input) {
                        return volumes.byVolumeId(input).getVolume();
                    }
                });
        
        IntraVmTest.EndpointFactory<InetSocketAddress> endpointFactory = endpointFactory();
        Pair<IntraVmConnection<InetSocketAddress>, IntraVmConnection<InetSocketAddress>> connections = IntraVmConnection.createPair(endpointFactory.get());
        Session session = Session.create(1, Session.Parameters.uninitialized());
        ShardedClientConnectionExecutor<IntraVmConnection<InetSocketAddress>> client = ShardedClientConnectionExecutor.newInstance(
                translator, 
                lookup, 
                Futures.immediateFuture(ConnectMessage.Response.Valid.newInstance(session)), 
                connections.second());
    }
}
