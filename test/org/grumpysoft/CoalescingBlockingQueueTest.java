package org.grumpysoft;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

public class CoalescingBlockingQueueTest extends TestCase {
	
	public void testAddPassesThroughForNonCoalescer() {
		LinkedBlockingQueue<String> underlying = new LinkedBlockingQueue<String>();
		CoalescingBlockingQueue<String, Integer> cbq =
			new CoalescingBlockingQueue<String, Integer> (
					underlying,
					new NeverCoalescePolicy(),
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
					new NeverCoalescePolicy(),
					new HashCodeRedirector()
					);
		assertEquals(fool, cbq.poll());
		assertEquals(diamonds, cbq.poll());
		assertEquals(null, cbq.poll());
	}
	
	public void testDrainToForgetsOldMessages() {
		CoalescingBlockingQueue<String, String> cbq =
			new CoalescingBlockingQueue<String, String> (
					new LinkedBlockingQueue<String>(),
					new AlwaysCoalescePolicy(),
					new HashCodeOfFirstLetterRedirector()
					);
		String fool = new String("fool");
		String diamonds = new String("diamonds");
		String horse = new String("horse");
		String delight = new String("delight");
		cbq.add(horse);
		cbq.add(diamonds);
		cbq.add(fool);
		cbq.add(delight);
		ArrayList<String> drainpipe = new ArrayList<String>();
		int numberDrained = cbq.drainTo(drainpipe);
		assertEquals(3, drainpipe.size()); //diamonds should coalesce with delight
		assertEquals(3, numberDrained);
		assertEquals(horse, drainpipe.get(0));
		assertEquals(fool, drainpipe.get(1));
		assertEquals(delight, drainpipe.get(2));
	}
	
	private class NeverCoalescePolicy implements CoalescingPolicy<String> {
		public boolean shouldCoalesce(String coalesceCandidate) {
			return false;
		}
	}
	
	private class AlwaysCoalescePolicy implements CoalescingPolicy<String> {

		public boolean shouldCoalesce(String coalesceCandidate) {
			return true;
		}
		
	}
	
	private class HashCodeRedirector implements LockSmith<String, Integer> {

		public Integer makeKey(String toGenerateFrom) {
			return new Integer(toGenerateFrom.hashCode());
		}
		
	}
	
	private class HashCodeOfFirstLetterRedirector implements LockSmith<String, String> {

		public String makeKey(String toGenerateFrom) {
			return toGenerateFrom.substring(0,1);
		}
		
	}
}
