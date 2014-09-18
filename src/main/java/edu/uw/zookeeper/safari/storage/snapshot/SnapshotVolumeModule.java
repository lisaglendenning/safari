package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.Map;

import com.google.common.primitives.UnsignedLong;
import com.google.inject.AbstractModule;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.SplitParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;

public class SnapshotVolumeModule extends AbstractModule {
    
    public static SnapshotVolumeModule create(
            VolumeOperation<?> operation,
            Map<Identifier, ZNodePath> paths) {
        final Identifier fromVolume = operation.getVolume().getValue();
        final Identifier toVolume;
        final ZNodeName fromBranch;
        final ZNodeName toBranch;
        final UnsignedLong fromVersion = operation.getVolume().getVersion();
        final UnsignedLong toVersion = operation.getOperator().getParameters().getVersion();
        switch (operation.getOperator().getOperator()) {
        case MERGE:
        {
            MergeParameters parameters = (MergeParameters) operation.getOperator().getParameters();
            toVolume = parameters.getParent().getValue();
            toBranch = paths.get(fromVolume).suffix(paths.get(toVolume));
            fromBranch = ZNodeLabel.empty();
            break;
        }
        case SPLIT:
        {
            SplitParameters parameters = (SplitParameters) operation.getOperator().getParameters();
            toVolume = parameters.getLeaf();
            toBranch = ZNodeLabel.empty();
            fromBranch = parameters.getBranch();
            break;
        }
        case TRANSFER:
        {
            toVolume = fromVolume;
            toBranch = ZNodeLabel.empty();
            fromBranch = ZNodeLabel.empty();
            break;
        }
        default:
            throw new AssertionError();
        }
        return new SnapshotVolumeModule(
                new SnapshotVolumeParameters(fromVolume, fromBranch, fromVersion), 
                new SnapshotVolumeParameters(toVolume, toBranch, toVersion));
    }

    private final SnapshotVolumeParameters fromVolume;
    private final SnapshotVolumeParameters toVolume;
    
    protected SnapshotVolumeModule(
            SnapshotVolumeParameters fromVolume,
            SnapshotVolumeParameters toVolume) {
        this.fromVolume = fromVolume;
        this.toVolume = toVolume;
    }
    
    @Override
    protected void configure() {
        bind(SnapshotVolumeParameters.class).annotatedWith(To.class).toInstance(toVolume);
        bind(SnapshotVolumeParameters.class).annotatedWith(From.class).toInstance(fromVolume);
    }
}