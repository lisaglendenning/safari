package edu.uw.zookeeper.orchestra.common;

import static com.google.common.base.Preconditions.*;

import java.util.AbstractQueue;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

public class LinkedQueue<E> extends AbstractQueue<E> implements Queue<E> {

    public static <E> LinkedQueue<E> create() {
        return new LinkedQueue<E>();
    }
    
    public static <E> LinkedQueue<E> create(Iterator<E> elements) {
        return new LinkedQueue<E>(elements);
    }

    public static <E> LinkedQueue<E> create(Iterable<E> elements) {
        return create(elements.iterator());
    }
    
    /**
     * Not thread-safe
     */
    protected class Itr implements LinkedIterator<E> {
    
        protected Entry<E> previous;
        protected Entry<E> next;
        protected Entry<E> last;
        
        protected Itr(Entry<E> next) {
            this.next = next;
            this.previous = null;
            this.last = null;
        }
        
        @Override
        public E peekNext() {
            if (next == null) {
                throw new ConcurrentModificationException();
            }
            return next.value;
        }
    
        @Override
        public E peekPrevious() {
            if (next == null) {
                throw new ConcurrentModificationException();
            }
            if (previous == null) {
                next.readLock.lock();
                try {
                    previous = next.previous;
                } finally {
                    next.readLock.unlock();
                }
            }
            if (previous == null) {
                throw new ConcurrentModificationException();
            }
            return previous.value;
        }
    
        @Override
        public boolean hasNext() {
            return (peekNext() != null);
        }
    
        @Override
        public boolean hasPrevious() {
            return (peekPrevious() != null);
        }
    
        @Override
        public E next() {
            E value = peekNext();
            if (value == null) {
                throw new NoSuchElementException();
            }
            assert (next != null);
            Entry<E> newNext;
            next.readLock.lock();
            try {
                newNext = next.next;
            } finally {
                next.readLock.unlock();
            }
            last = next;
            next = newNext;
            previous = null;
            return value;
        }
    
        @Override
        public E previous() {
            E value = peekPrevious();
            if (value == null) {
                throw new NoSuchElementException();
            }
            assert (previous != null);
            last = next = previous;
            previous = null;
            return value;
        }
    
        @Override
        public void add(E value) {
            checkNotNull(value);
            if (next == null) {
                throw new ConcurrentModificationException();
            }
            for (;;) {
                Entry<E> first = next.previous;
                if (first == null) {
                    throw new ConcurrentModificationException();
                }
                first.writeLock.lock();
                try {
                    Entry<E> third = first.next;
                    if (third == next) {
                        third.writeLock.lock();
                        try {
                            insert(value, first, third);
                            break;
                        } finally {
                            third.writeLock.unlock();
                        }
                    } else {
                        continue;
                    }
                } finally {
                    first.writeLock.unlock();
                }
            }
            previous = last = null;
        }
    
        @Override
        public void remove() {
            checkState(last != null);
            for (;;) {
                Entry<E> first = last.previous;
                if (first == null) {
                    // already deleted
                    break;
                }
                first.writeLock.lock();
                try {
                    Entry<E> second = first.next;
                    if (second == last) {
                        second.writeLock.lock();
                        try {
                            Entry<E> third = second.next;
                            // this should never be null
                            // because after acquiring the first lock
                            // and checking that second is not null
                            // it should be impossible for someone else
                            // to remove second
                            assert (third != null);
                            third.writeLock.lock();
                            try {
                                unlink(first, second, third);
                            } finally {
                                third.writeLock.unlock();
                            }
                        } finally {
                            second.writeLock.unlock();
                        }
                    } else {
                        continue;
                    }
                } finally {
                    first.writeLock.unlock();
                }
            }
            previous = last = null;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("next", next).add("previous", previous).add("last", last).toString();
        }
    }

    protected static class Entry<E> {
        
        public static <E> Entry<E> sentinel() {
            return new Entry<E>();
        }

        protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        protected final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        protected final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

        protected final E value;
        // volatile may be unnecessary
        // but currently there are some unguarded accesses
        protected volatile Entry<E> next;
        protected volatile Entry<E> previous;

        protected Entry() {
            this.value = null;
            this.next = null;
            this.previous = null;
        }

        protected Entry(E value, Entry<E> previous, Entry<E> next) {
            assert (value != null);
            assert (previous != null);
            assert (next != null);
            this.value = value;
            this.next = next;
            this.previous = previous;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .addValue(hashCode())
                    .add("value", value)
                    .add("next", (next == null) ? "null" : next.hashCode())
                    .add("previous", (previous == null) ? "null" : previous.hashCode())
                    .toString();
        }
    }
    
    protected final Entry<E> head;
    protected final Entry<E> tail;

    public LinkedQueue() {
        this.head = Entry.sentinel();
        this.tail = Entry.sentinel();
        head.next = tail;
        tail.previous = head;
    }

    public LinkedQueue(Iterator<E> values) {
        this();
        while (values.hasNext()) {
            add(values.next());
        }
    }

    @Override
    public boolean isEmpty() {
        head.readLock.lock();
        try {
            assert (head.next != null);
            return (head.next == tail);
        } finally {
            head.readLock.unlock();
        }
    }

    @Override
    public E peek() {
        head.readLock.lock();
        try {
            assert (head.next != null);
            return head.next.value;
        } finally {
            head.readLock.unlock();
        }
    }

    @Override
    public E poll() {
        head.writeLock.lock();
        try {
            Entry<E> second = head.next;
            assert (second != null);
            if (second != tail) {
                second.writeLock.lock();
                try {
                    Entry<E> third = second.next;
                    assert (third != null);
                    third.writeLock.lock();
                    try {
                        unlink(head, second, third);
                    } finally {
                        third.writeLock.unlock();
                    }
                } finally {
                    second.writeLock.unlock();
                }
            }
            return second.value;
        } finally {
            head.writeLock.unlock();
        }
    }

    @Override
    public boolean offer(E value) {
        checkNotNull(value);
        for (;;) {
            Entry<E> first = tail.previous;
            assert (first != null);
            first.writeLock.lock();
            try {
                Entry<E> third = first.next;
                if (third == tail) {
                    third.writeLock.lock();
                    try {
                        insert(value, first, third);
                        break;
                    } finally {
                        third.writeLock.unlock();
                    }
                } else {
                    continue;
                }
            } finally {
                first.writeLock.unlock();
            }
        }
        return true;
    }

    @Override
    public LinkedIterator<E> iterator() {
        head.readLock.lock();
        try {
            assert (head.next != null);
            return new Itr(head.next);
        } finally {
            head.readLock.unlock();
        }
    }

    @Override
    public int size() {
        return Iterators.size(iterator());
    }

    @Override
    public boolean contains(Object value) {
        checkNotNull(value);
        Iterator<E> itr = iterator();
        while (itr.hasNext()) {
            if (itr.next().equals(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean remove(Object value) {
        checkNotNull(value);
        Iterator<E> itr = iterator();
        while (itr.hasNext()) {
            if (itr.next().equals(value)) {
                itr.remove();
                return true;
            }
        }
        return false;
    }
    
    protected Entry<E> insert(E value, Entry<E> previous, Entry<E> next) {
        assert (value != null);
        assert (previous != null);
        assert (next != null);
        assert (previous.lock.isWriteLockedByCurrentThread());
        assert (next.lock.isWriteLockedByCurrentThread());
        assert (previous.next == next);
        assert (next.previous == previous);
        Entry<E> e = new Entry<E>(value, previous, next);
        previous.next = e;
        next.previous = e;
        return e;
    }
    
    protected void unlink(Entry<E> previous, Entry<E> entry, Entry<E> next) {
        assert (entry != null);
        assert (previous != null);
        assert (next != null);
        assert (entry.lock.isWriteLockedByCurrentThread());
        assert (entry != head);
        assert (entry != tail);
        assert (previous.lock.isWriteLockedByCurrentThread());
        assert (next.lock.isWriteLockedByCurrentThread());
        assert (entry.next == next);
        assert (next.previous == entry);
        assert (entry.previous == previous);
        assert (previous.next == entry);
        previous.next = next;
        next.previous = previous;
        entry.next = null;
        entry.previous = null;
    }
}
