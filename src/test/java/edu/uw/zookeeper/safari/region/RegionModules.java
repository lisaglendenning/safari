package edu.uw.zookeeper.safari.region;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.peer.PeerModules;
import edu.uw.zookeeper.safari.storage.StorageModules;

public class RegionModules {

    public static @Named("member") Component<?> newSingletonRegionMember(
            final Component<?> storage,
            final Iterable<? extends Component<?>> components) {
        return newSingletonRegionMember(
                storage, components, ImmutableList.<SafariModule>of());
    }
    
    public static @Named("member") Component<?> newSingletonRegionMember(
            final Component<?> storage,
            final Iterable<? extends Component<?>> components,
            final List<SafariModule> modules) {
        return newSingletonRegionMember(
                storage,
                components,
                modules,
                RegionMemberProvider.class);
    }
    
    public static @Named("member") Component<?> newSingletonRegionMember(
            final Component<?> storage,
            final Iterable<? extends Component<?>> components,
            final List<SafariModule> modules,
            final Class<? extends Provider<Component<?>>> provider) {
        return newSingletonRegionMember(
                storage, 
                components, 
                Names.named("member"),
                modules,
                provider);
    }
    
    public static Component<?> newSingletonRegionMember(
            final Component<?> storage,
            final Iterable<? extends Component<?>> components,
            final Named name,
            final List<SafariModule> modules,
            final Class<? extends Provider<Component<?>>> provider) {
        return newRegionMember(
                ImmutableList.<Component<?>>of(storage), 
                0, 
                components, 
                name,
                modules,
                provider);
    }
    
    public static Component<?> newSingletonRegionMember(
            final Component<?> storage,
            final Iterable<? extends Component<?>> components,
            final Named name,
            final Iterable<? extends com.google.inject.Module> modules,
            final List<SafariModule> safariModules,
            final Class<? extends Provider<Component<?>>> provider) {
        return newRegionMember(
                ImmutableList.<Component<?>>of(storage), 
                0, 
                components, 
                name,
                modules,
                safariModules,
                provider);
    }

    public static Component<?> newRegionMember(
            final List<Component<?>> storage,
            final int server,
            final Iterable<? extends Component<?>> components,
            final Named name,
            final List<SafariModule> modules) {
        return newRegionMember(
                storage, 
                server, 
                components, 
                name,
                modules, 
                RegionMemberProvider.class);
    }

    public static Component<?> newRegionMember(
            final List<Component<?>> storage,
            final int server,
            final Iterable<? extends Component<?>> components,
            final Named name,
            final List<SafariModule> modules,
            Class<? extends Provider<Component<?>>> provider) {
        return newRegionMember(
                storage, 
                server, 
                components, 
                name, 
                ImmutableList.<com.google.inject.Module>of(), 
                modules, 
                provider);
    }

    public static Component<?> newRegionMember(
            final List<Component<?>> storage,
            final int server,
            final Iterable<? extends Component<?>> components,
            final Named name,
            final Iterable<? extends com.google.inject.Module> modules,
            final List<SafariModule> safariModules,
            Class<? extends Provider<Component<?>>> provider) {
        return newRegionMember(
                storage, 
                server, 
                components, 
                name, 
                modules, 
                safariModules, 
                Modules.SafariServerModuleProvider.class, 
                provider);
    }
    
    public static Component<?> newRegionMember(
            final List<Component<?>> storage,
            final int server,
            final Iterable<? extends Component<?>> components,
            final Named name,
            final Iterable<? extends com.google.inject.Module> modules,
            final Iterable<? extends SafariModule> safariModules,
            final Class<? extends Provider<List<com.google.inject.Module>>> module,
            final Class<? extends Provider<? extends Component<?>>> provider) {
        return StorageModules.newStorageClient(
                storage, server, components, name, modules, provider,
                ImmutableList.<SafariModule>builder()
                    .add(edu.uw.zookeeper.safari.control.Module.create())
                    .add(edu.uw.zookeeper.safari.peer.Module.create())
                    .add(Module.create())
                    .addAll(safariModules)
                    .build(),
                module);
    }
    
    public static List<Component<?>> newRegion(
            final List<Component<?>> storage,
            final Iterable<? extends Component<?>> components) {
        return newRegion(storage, components, "member-%d");
    }
    
    public static List<Component<?>> newRegion(
            final List<Component<?>> storage,
            final Iterable<? extends Component<?>> components,
            final String format) {
        return newRegion(storage, components, format, ImmutableList.<SafariModule>of());
    }
    
    public static List<Component<?>> newRegion(
            final List<Component<?>> storage,
            final Iterable<? extends Component<?>> components,
            final String format,
            final List<SafariModule> modules) {
        ImmutableList.Builder<Component<?>> members = ImmutableList.builder();
        for (int i=0; i<storage.size(); ++i) {
            final Named name = Names.named(String.format(format, i+1));
            members.add(newRegionMember(storage, i, components, name, modules));
        }
        return members.build();
    }
    
    public static List<Component<?>> newRegion(
            final List<Component<?>> storage,
            final Iterable<? extends Component<?>> components,
            final String format,
            final Iterable<? extends com.google.inject.Module> modules,
            final Iterable<? extends SafariModule> safariModules,
            final Class<? extends Provider<List<com.google.inject.Module>>> module,
            final Class<? extends Provider<? extends Component<?>>> provider) {
        ImmutableList.Builder<Component<?>> members = ImmutableList.builder();
        for (int i=0; i<storage.size(); ++i) {
            final Named name = Names.named(String.format(format, i+1));
            members.add(newRegionMember(
                    storage, i, components, name, modules, safariModules, module, provider));
        }
        return members.build();
    }

    public static class RegionMemberProvider extends StorageModules.StorageClientProvider {

        @Inject
        public RegionMemberProvider(
                @Named("storage") List<Component<?>> storage,
                @Named("server") Component<?> server,
                RuntimeModule runtime,
                Injector injector) {
            this(storage, server, runtime, injector, PeerModules.PeerProvider.class);
        }

        protected RegionMemberProvider(
                @Named("storage") List<? extends Component<?>> storage,
                @Named("server") Component<?> server,
                RuntimeModule runtime,
                Injector injector,
                Class<? extends Provider<? extends Component<?>>> delegate) {
            super(storage, server, runtime, injector, delegate);
        }
    }
    
    protected RegionModules() {}
}
