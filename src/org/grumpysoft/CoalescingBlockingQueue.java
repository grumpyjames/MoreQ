package org.grumpysoft;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public class CoalescingBlockingQueue<E, KeyType> implements BlockingQueue<E> {

	private final BlockingQueue<E> impl_;
	private final CoalescingPolicy<E> policy_;
	private final LockSmith<E, KeyType> smith_;
	private final ConcurrentHashMap<KeyType, E> latest_ = 
		new ConcurrentHashMap<KeyType, E>();
	
	public CoalescingBlockingQueue (
			final BlockingQueue<E> toWrap,
			final CoalescingPolicy<E> decider,
			final LockSmith<E, KeyType> jones) {
		impl_ = toWrap;
		policy_ = decider;
		smith_ = jones;
	}
	
	public boolean add(E o) {
		if (!policy_.shouldCoalesce(o))
			return impl_.add(o);
		if (impl_.add(o)) {
			latest_.put(smith_.makeKey(o), o);
			return true;
		}
		return false;
	}

	public int drainTo(Collection<? super E> c) {
		final Collection<E> entries = latest_.values();
		final ArrayList<E> queueContents = new ArrayList<E>();
		int drainCount = 0;
		impl_.drainTo(queueContents);
		for (final E element: queueContents) {
			if (!policy_.shouldCoalesce(element)) {
				c.add(element);
				++drainCount;
			}
			else {
				if (entries.contains(element)) {
						c.add(element);
						++drainCount;
				}
			}
		}
		return drainCount;
	}

	public int drainTo(Collection<? super E> c, int maxElements) {
		//FIXME Again, only drain the latest
		return impl_.drainTo(c, maxElements);
	}

	public boolean offer(E o) {
		if (!policy_.shouldCoalesce(o))
			return impl_.offer(o);
		if (impl_.offer(o)) {
			latest_.put(smith_.makeKey(o), o);
			return true;
		}
		return false;
	}

	public boolean offer(E o, long timeout, TimeUnit unit)
			throws InterruptedException {
		if (!policy_.shouldCoalesce(o))
			return impl_.offer(o, timeout, unit);
		if (impl_.offer(o, timeout, unit)) {
			latest_.put(smith_.makeKey(o), o);
			return true;
		}
		return false;
	}

	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	public void put(E o) throws InterruptedException {
		if (!policy_.shouldCoalesce(o)) {
			impl_.put(o);
			return;
		}
		impl_.put(o);
		latest_.put(smith_.makeKey(o), o);
	}

	public int remainingCapacity() {
		return impl_.remainingCapacity();
	}

	public E take() throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	public E element() {
		// TODO Auto-generated method stub
		return null;
	}

	public E peek() {
		// TODO Auto-generated method stub
		return null;
	}

	public E poll() {
		final E polled = impl_.poll();
		if (polled == null)
			return polled;
		if (!policy_.shouldCoalesce(polled))
			return polled;
		return null;
	}

	public E remove() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public void clear() {
		// TODO Auto-generated method stub
		
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

	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
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

}
