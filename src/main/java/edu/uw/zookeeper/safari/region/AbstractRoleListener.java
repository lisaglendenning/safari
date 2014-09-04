package edu.uw.zookeeper.safari.region;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.SameThreadExecutor;

public class AbstractRoleListener<T> extends LoggingServiceListener<T> implements Runnable, FutureCallback<RegionRoleService> {

    protected final Supplier<FutureTransition<RegionRoleService>> role;
    
    protected AbstractRoleListener(
            Supplier<FutureTransition<RegionRoleService>> role,
            T delegate,
            Logger logger) {
        super(delegate, logger);
        this.role = role;
    }
    
    protected AbstractRoleListener(
            Supplier<FutureTransition<RegionRoleService>> role,
            T delegate) {
        super(delegate);
        this.role = role;
    }
    
    protected AbstractRoleListener(
            Supplier<FutureTransition<RegionRoleService>> role,
            Logger logger) {
        super(logger);
        this.role = role;
    }
    
    protected AbstractRoleListener(
            Supplier<FutureTransition<RegionRoleService>> role) {
        super();
        this.role = role;
    }

    @Override
    public void run() {
        RegionRoleService role;
        FutureTransition<RegionRoleService> transition = this.role.get();
        if (transition.getNext().isDone()) {
            try {
                role = transition.getNext().get();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (CancellationException e) {
                return;
            }  catch (ExecutionException e) {
                onFailure(e);
                return;
            } 
        } else {
            role = transition.getCurrent().orNull();
        }
        if (role != null) {
            onSuccess(role);
        }
        this.role.get().getNext().addListener(this, SameThreadExecutor.getInstance());
    }
    
    @Override
    public void running() {
        super.running();
        run();
    }

    @Override
    public void onSuccess(RegionRoleService result) {
        logger.debug("ROLE ({}) ({})", delegate(), result);
    }
    
    @Override
    public void onFailure(Throwable t) {
        logger.warn(t);
    }
}
