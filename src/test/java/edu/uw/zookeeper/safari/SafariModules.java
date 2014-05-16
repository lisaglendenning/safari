package edu.uw.zookeeper.safari;

import java.util.List;

import com.google.common.collect.ImmutableList;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.frontend.FrontendModules;
import edu.uw.zookeeper.safari.region.RegionModules;
import edu.uw.zookeeper.safari.storage.StorageModules;

public class SafariModules {

    public static List<Component<?>> newSingletonSafari() {
        final Component<?> root = Modules.newRootComponent();
        return ImmutableList.<Component<?>>builder().add(root).addAll(newSingletonSafari(root)).build();
    }
    
    public static List<Component<?>> newSingletonSafari(
            final Component<?> root) {
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        final Component<?> server = StorageModules.newStorageSingletonEnsemble(root);
        final Component<?> member = SafariModules.newSingletonSafariServer(
                server, ImmutableList.of(root, control));
        return ImmutableList.of(control, server, member);
    }
    
    public static Component<?> newSingletonSafariServer(
            final Component<?> server, final Iterable<? extends Component<?>> components) {
        return RegionModules.newSingletonRegionMember(
                server, 
                components, 
                ImmutableList.<SafariModule>of(
                        edu.uw.zookeeper.safari.backend.Module.create(),
                        edu.uw.zookeeper.safari.frontend.Module.create()),
                FrontendModules.FrontendProvider.class);
    }

    public static List<Component<?>> newSafari() {
        final Component<?> root = Modules.newRootComponent();
        return ImmutableList.<Component<?>>builder().add(root).addAll(newSafari(root)).build();
    }
    
    public static List<Component<?>> newSafari(
            final Component<?> root) {
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        return ImmutableList.<Component<?>>builder().add(control).addAll(newSafari(root, control)).build();
    }
    
    public static List<Component<?>> newSafari(
            final Component<?> root,
            final Component<?> control) {
        final int size = 3;
        final List<Component<?>> storage = StorageModules.newStorageEnsemble(root, size);
        return ImmutableList.<Component<?>>builder().addAll(storage)
                .addAll(newSafariRegion(
                        storage,
                        ImmutableList.of(root, control),
                        "member-%d")).build();
    }
    
    public static List<Component<?>> newSafariRegion(
            final List<Component<?>> storage,
            final Iterable<? extends Component<?>> components,
            final String format) {
        return RegionModules.newRegion(
                storage,
                components,
                format,
                ImmutableList.<com.google.inject.Module>of(),
                ImmutableList.<SafariModule>of(
                        edu.uw.zookeeper.safari.backend.Module.create(),
                        edu.uw.zookeeper.safari.frontend.Module.create()),
                Modules.SafariServerModuleProvider.class,
                FrontendModules.FrontendProvider.class);
    }
}
