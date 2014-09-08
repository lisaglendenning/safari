package edu.uw.zookeeper.safari;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

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
    
    public static @Named("safari") Component<?> newSingletonSafariServer(
            final Component<?> server, final Iterable<? extends Component<?>> components) {
        return newSingletonSafariServer(
                server, 
                components, 
                Names.named("safari"));
    }
    
    public static Component<?> newSingletonSafariServer(
            final Component<?> server, 
            final Iterable<? extends Component<?>> components,
            final Named name) {
        return RegionModules.newSingletonRegionMember(
                server, 
                components, 
                name,
                getModules(),
                FrontendModules.FrontendProvider.class);
    }

    public static List<Component<?>> newSingletonSafariRegions(
            int size) {
        final Component<?> root = Modules.newRootComponent();
        return ImmutableList.<Component<?>>builder().add(root).addAll(newSingletonSafariRegions(size, root)).build();
    }

    public static List<Component<?>> newSingletonSafariRegions(
            int size,
            final Component<?> root) {
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        return ImmutableList.<Component<?>>builder().add(control).addAll(newSingletonSafariRegions(size, root, control)).build();
    }

    public static List<Component<?>> newSingletonSafariRegions(
            int size,
            final Component<?> root,
            final Component<?> control) {
        final ImmutableList.Builder<Component<?>> components = ImmutableList.builder();
        for (int i=1; i<=size; ++i) {
            Named name = Names.named(String.format("storage-%d", i));
            Component<?> storage = StorageModules.newStorageSingletonEnsemble(root, name);
            components.add(storage);
            name = Names.named(String.format("region-%d", i));
            components.add(SafariModules.newSingletonSafariServer(storage, ImmutableList.of(root, control), name));
        }
        return components.build();
    }
    
    public static List<Component<?>> newSafariRegion() {
        final Component<?> root = Modules.newRootComponent();
        return ImmutableList.<Component<?>>builder().add(root).addAll(newSafariRegion(root)).build();
    }
    
    public static List<Component<?>> newSafariRegion(
            final Component<?> root) {
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        return ImmutableList.<Component<?>>builder().add(control).addAll(newSafariRegion(root, control)).build();
    }

    public static List<Component<?>> newSafariRegion(
            final Component<?> root,
            final Component<?> control) {
        return newSafariRegion(root, control, "safari-%d");
    }
    
    public static List<Component<?>> newSafariRegion(
            final Component<?> root,
            final Component<?> control,
            String format) {
        final int size = 3;
        final List<Component<?>> storage = StorageModules.newStorageEnsemble(root, size);
        return ImmutableList.<Component<?>>builder().addAll(storage)
                .addAll(newSafariRegion(
                        storage,
                        ImmutableList.of(root, control),
                        format)).build();
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
                getModules(),
                Modules.SafariServerModuleProvider.class,
                FrontendModules.FrontendProvider.class);
    }
    
    public static ImmutableList<SafariModule> getModules() {
        return ImmutableList.<SafariModule>of(
                edu.uw.zookeeper.safari.control.volumes.Module.create(),
                edu.uw.zookeeper.safari.storage.volumes.Module.create(),
                edu.uw.zookeeper.safari.storage.snapshot.Module.create(),
                edu.uw.zookeeper.safari.volumes.Module.create(),
                edu.uw.zookeeper.safari.backend.Module.create(),
                edu.uw.zookeeper.safari.frontend.Module.create());
    }
}
