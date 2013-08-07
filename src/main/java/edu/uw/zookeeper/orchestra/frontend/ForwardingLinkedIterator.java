package edu.uw.zookeeper.orchestra.frontend;

public abstract class ForwardingLinkedIterator<E> implements LinkedIterator<E> {

    @Override
    public boolean hasNext() {
        return delegate().hasNext();
    }

    @Override
    public E next() {
        return delegate().next();
    }

    @Override
    public void remove() {
        delegate().remove();
    }

    @Override
    public E peekNext() {
        return delegate().peekNext();
    }

    @Override
    public E peekPrevious() {
        return delegate().peekPrevious();
    }

    @Override
    public boolean hasPrevious() {
        return delegate().hasPrevious();
    }

    @Override
    public E previous() {
        return delegate().previous();
    }

    @Override
    public void add(E value) {
        delegate().add(value);
    }
    
    @Override
    public String toString() {
        return delegate().toString();
    }

    protected abstract LinkedIterator<E> delegate();
}
