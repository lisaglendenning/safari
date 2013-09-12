package edu.uw.zookeeper.safari.common;

import java.util.Iterator;

public interface LinkedIterator<E> extends Iterator<E> {
    E peekNext();

    E peekPrevious();

    boolean hasPrevious();

    E previous();
    
    void add(E value);
}