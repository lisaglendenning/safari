package edu.uw.zookeeper.orchestra.frontend;

public class RetryLinkedIterator<E> extends ForwardingLinkedIterator<E> {

    public static <E> RetryLinkedIterator<E> create(
            LinkedQueue<E> queue) {
        return new RetryLinkedIterator<E>(queue, queue.iterator());
    }
    
    protected final LinkedQueue<E> queue;
    protected volatile LinkedIterator<E> delegate;
    
    protected RetryLinkedIterator(
            LinkedQueue<E> queue,
            LinkedIterator<E> delegate) {
        this.queue = queue;
        this.delegate = delegate;
    }
    
    public void reset() {
        this.delegate = queue.iterator();
    }
    
    public LinkedQueue<E> queue() {
        return queue;
    }
    
    @Override
    protected LinkedIterator<E> delegate() {
        return delegate;
    }
}
