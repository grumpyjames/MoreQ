package org.grumpysoft;

import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

public class CoalescingBlockingQueueTest extends TestCase {
	
	public void testAddPassesThroughForNonCoalescer() {
		LinkedBlockingQueue<String> underlying = new LinkedBlockingQueue<String>();
		CoalescingBlockingQueue<String, Integer> cbq =
			new CoalescingBlockingQueue<String, Integer> (
					underlying,
					new SimpleStringPolicy(),
					new HashCodeRedirector()
					);
		String fool = new String("Fool");
		String diamonds = new String("Diamonds");
		cbq.add(fool);
		cbq.add(diamonds);
		assertEquals(fool, underlying.poll());
		assertEquals(diamonds, underlying.poll());
	}
	
	public void testPollPassesThroughForNonCoalescer() {
		LinkedBlockingQueue<String> underlying = new LinkedBlockingQueue<String>();
		String fool = new String("Fool");
		String diamonds = new String("Diamonds");
		underlying.add(fool);
		underlying.add(diamonds);
		CoalescingBlockingQueue<String, Integer> cbq =
			new CoalescingBlockingQueue<String, Integer> (
					underlying,
					new SimpleStringPolicy(),
					new HashCodeRedirector()
					);
		assertEquals(fool, cbq.poll());
		assertEquals(diamonds, cbq.poll());
		assertEquals(null, cbq.poll());
	}
	
	private class SimpleStringPolicy implements CoalescingPolicy<String> {
		public boolean shouldCoalesce(String coalesceCandidate) {
			return false;
		}
	}
	
	private class HashCodeRedirector implements LockSmith<String, Integer> {

		public Integer makeKey(String toGenerateFrom) {
			return new Integer(toGenerateFrom.hashCode());
		}
		
	}
}
