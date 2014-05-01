package edu.uw.zookeeper.safari.peer;

import static com.google.common.base.Preconditions.*;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlZNode;

public final class LeaderEpoch {

    public static LeaderEpoch of(int epoch, Identifier leader) {
        return new LeaderEpoch(epoch, leader);
    }
    
    public static Function<ControlZNode<?>, LeaderEpoch> fromMaterializer() {
        return LeaderEpochFromMaterializer.LEADER_EPOCH_FROM_MATERIALIZER;
    }
    
    protected enum LeaderEpochFromMaterializer implements Function<ControlZNode<?>, LeaderEpoch> {
        LEADER_EPOCH_FROM_MATERIALIZER;

        @Override
        public LeaderEpoch apply(@Nullable ControlZNode<?> node) {
            if (node != null) {
                StampedValue<?> nodeData = node.data();
                StampedValue<Records.ZNodeStatGetter> nodeStat = node.stat();
                if (nodeData.stamp()  == nodeStat.stamp()) {
                    Identifier leader = (Identifier) nodeData.get();
                    Records.ZNodeStatGetter stat = nodeStat.get();
                    if ((leader != null) && (stat != null)) {
                        return LeaderEpoch.of(stat.getVersion(), leader);
                    }
                }
            }
            return null;
        }
    }

    private final int epoch;
    private final Identifier leader;
    
    public LeaderEpoch(int epoch, Identifier leader) {
        super();
        checkArgument(epoch >= 0);
        this.epoch = epoch;
        this.leader = checkNotNull(leader);
    }
    
    public int getEpoch() {
        return epoch;
    }
    
    public Identifier getLeader() {
        return leader;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("epoch", epoch).add("leader", leader).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (! (obj instanceof LeaderEpoch)) {
            return false;
        }
        
        LeaderEpoch other = (LeaderEpoch) obj;
        return (epoch == other.epoch) && leader.equals(other.leader);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(epoch, leader);
    }
}
