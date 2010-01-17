package org.grumpysoft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author james
 *
 * @param <E> the underlying element that will be stored in the queue
 * @param <KeyType> the type of key that will be used to work out whether to coalesce or not
 */
public class CoalescingBlockingQueue<E, KeyType> implements BlockingQueue<E> {
	
	private final BlockingQueue<E> impl_;
	private final CoalescingPolicy<E> policy_;
	private final LockSmith<E, KeyType> smith_;
	private final ConcurrentHashMap<KeyType, E> latest_ = new ConcurrentHashMap<KeyType, E>();

	/**
	 * @param toWrap The *actual* implementation of a blocking
	 * queue - we don't want to reinvent that particular wheel!
	 * @param decider @see org.grumpysoft.CoalescingPolicy
	 * @param jones Generates keys for queue objects that may coalesce
	 */
	public CoalescingBlockingQueue(final BlockingQueue<E> toWrap,
			final CoalescingPolicy<E> decider, final LockSmith<E, KeyType> jones) {
		impl_ = toWrap;
		policy_ = decider;
		smith_ = jones;
	}

	/**
	 * @see java.util.concurrent.BlockingQueue#add(java.lang.Object)
	 */
	public boolean add(E o) {
		if (!policy_.shouldCoalesce(o))
			return impl_.add(o);
		if (impl_.add(o)) {
			latest_.put(smith_.makeKey(o), o);
			return true;
		}
		return false;
	}

	private int drainWithCoalescing(final Collection<E> from,
			final Collection<? super E> c, final Collection<E> latest) {
		int drainCount = 0;
		for (final E element : from) {
			if (!policy_.shouldCoalesce(element)) {
				c.add(element);
				++drainCount;
			} else {
				if (latest.contains(element)) {
					c.add(element);
					++drainCount;
				}
			}
		}
		return drainCount;
	}
	
	/**
	 * @see java.util.concurrent.BlockingQueue#drainTo(java.util.Collection)
	 * In this implementation only elements that *would not* coalesce are
	 * drained. Coalescing elements are forgotten.
	 */
	public int drainTo(Collection<? super E> c) {
		final Collection<E> entries = latest_.values();
		final ArrayList<E> queueContents = new ArrayList<E>();
		impl_.drainTo(queueContents);
		return drainWithCoalescing(queueContents, c, entries);
	}

	/**
	 * @see java.util.concurrent.BlockingQueue#drainTo(java.util.Collection, int)
	 * This method, like the limitless drainTo, will drop any elements
	 * that ought to coalesce.
	 */
	public int drainTo(Collection<? super E> c, int maxElements) {
		final Collection<E> entries = latest_.values();
		ArrayList<E> queueContents = new ArrayList<E>();
		impl_.drainTo(queueContents, maxElements);
		int drainCount = drainWithCoalescing(queueContents, c, entries);
		if (drainCount == maxElements || queueContents.size() < maxElements)
			return drainCount;
		else
			return drainCount + drainTo(c, maxElements - c.size());
	}

	/**
	 * @see java.util.concurrent.BlockingQueue#offer(java.lang.Object)
	 */
	public boolean offer(final E o) {
		if (!policy_.shouldCoalesce(o))
			return impl_.offer(o);
		if (impl_.offer(o)) {
			latest_.put(smith_.makeKey(o), o);
			return true;
		}
		return false;
	}

	/**
	 * @see java.util.concurrent.BlockingQueue#offer(java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	public boolean offer(final E o, final long timeout, final TimeUnit unit)
			throws InterruptedException {
		if (!policy_.shouldCoalesce(o))
			return impl_.offer(o, timeout, unit);
		if (impl_.offer(o, timeout, unit)) {
			latest_.put(smith_.makeKey(o), o);
			return true;
		}
		return false;
	}

	/**
	 * @see java.util.concurrent.BlockingQueue#poll(long, java.util.concurrent.TimeUnit)
	 * Attempts to poll for the given amount of time and returns null if no
	 * element is available at the end of that time.
	 */
	public E poll(final long timeout, final TimeUnit unit) throws InterruptedException {
		E polled = impl_.poll(timeout, unit);
		return loop_poll(polled);
	}

	/**
	 * @see java.util.concurrent.BlockingQueue#put(java.lang.Object)
	 */
	public void put(final E o) throws InterruptedException {
		if (!policy_.shouldCoalesce(o)) {
			impl_.put(o);
			return;
		}
		impl_.put(o);
		latest_.put(smith_.makeKey(o), o);
	}

	/**
	 * @see java.util.concurrent.BlockingQueue#remainingCapacity()
	 */
	public int remainingCapacity() {
		return impl_.remainingCapacity();
	}
	
	private final boolean wouldCoalesce(final E el) {
		return !el.equals(latest_.get(smith_.makeKey(el)));
	}
	/**
	 * Will return the first element that hasn't or
	 * cannot coalesce. 
	 * @see java.util.concurrent.BlockingQueue#take()
	 * for more information.
	 */
	public E take() throws InterruptedException {
		while (true) {
			E next = impl_.take();
			if (!policy_.shouldCoalesce(next))
				return next;
			if (!wouldCoalesce(next))
				return next;
		}
	}

	/**
	 * @see java.util.Queue#element() this method *does not coalesce*
	 */
	public E element() {
		return impl_.element();
	}

	/**
	 * @see java.util.Queue#peek() this method *does not coalesce*
	 */
	public E peek() {
		return impl_.peek();
	}
	
	private E loop_poll(E original) {
		while (true) {
			if (original == null)
				return original;
			if (!policy_.shouldCoalesce(original))
				return original;
			if (!wouldCoalesce(original))
				return original;
			original = impl_.poll();
		}
	}

	/** 
	 * @see java.util.Queue#poll()
	 * This element will never return an element that would
	 * coalesce. It will return null as a queue would, should the queue
	 * be empty.
	 */
	public E poll() {
		E polled = impl_.poll();
		return loop_poll(polled);
	}

	/**
	 * Removes the first coalescable element. All other behaviour
	 * is kept from the original Queue javadoc here:
	 * @see java.util.Queue#remove()
	 */
	public E remove() {
		final E polled = poll();
		if (polled == null)
			throw new NoSuchElementException();
		else
			return polled;
	}

	/**
	 * @see java.util.Collection#addAll(java.util.Collection)
	 */
	public boolean addAll(final Collection<? extends E> c) {
		boolean collectionChanged = false;
		for (final E element: c) {
			if (add(element))
				collectionChanged = true;
			else
				return collectionChanged;		
		}
		return collectionChanged;
	}

	/**
	 * @see java.util.Collection#clear()
	 */
	public void clear() {
		impl_.clear();
	}

	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * @see java.util.Collection#iterator()
	 */
	public Iterator<E> iterator() {
		return new CoalescingIterator();
	}

	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private class CoalescingIterator implements Iterator<E> {

		private final Iterator<E> it_impl_;
		
		public CoalescingIterator() {
			it_impl_ = impl_.iterator();
		}
		
		public boolean hasNext() {
			return it_impl_.hasNext();
		}

		public E next() {
			while (true) {
				final E candidate = it_impl_.next();
				if (!policy_.shouldCoalesce(candidate))
					return candidate;
				else if (!wouldCoalesce(candidate))
					return candidate;
			}
		}

		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
	}

}
