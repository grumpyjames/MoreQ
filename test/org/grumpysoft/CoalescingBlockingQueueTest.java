package org.grumpysoft;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

/**
 * @author james
 * This class aims to test the functionality of the queue in a single
 * threaded environment. We assume that the existing implementations
 * provided by the java.util.concurrency package will look after things
 * ...up to a certain point.
 */
public class CoalescingBlockingQueueTest extends TestCase {
	
	/**
	 * 
	 */
	public void testAddPassesThroughForNonCoalescer() {
		final LinkedBlockingQueue<String> underlying = new LinkedBlockingQueue<String>();
		final CoalescingBlockingQueue<String, Integer> cbq =
			new CoalescingBlockingQueue<String, Integer> (
					underlying,
					new NeverCoalescePolicy(),
					new HashCodeRedirector()
					);
		final String fool = new String("Fool");
		final String diamonds = new String("Diamonds");
		cbq.add(fool);
		cbq.add(diamonds);
		assertEquals(fool, underlying.poll());
		assertEquals(diamonds, underlying.poll());
	}
	
	public void testPollPassesThroughForNonCoalescer() {
		final LinkedBlockingQueue<String> underlying = new LinkedBlockingQueue<String>();
		final String fool = new String("Fool");
		final String diamonds = new String("Diamonds");
		underlying.add(fool);
		underlying.add(diamonds);
		final CoalescingBlockingQueue<String, Integer> cbq =
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
		final CoalescingBlockingQueue<String, String> cbq =
			new CoalescingBlockingQueue<String, String> (
					new LinkedBlockingQueue<String>(),
					new AlwaysCoalescePolicy(),
					new HashCodeOfFirstLetterRedirector()
					);
		final String fool = new String("fool");
		final String diamonds = new String("diamonds");
		final String horse = new String("horse");
		final String delight = new String("delight");
		cbq.add(horse);
		cbq.add(diamonds);
		cbq.add(fool);
		cbq.add(delight);
		final ArrayList<String> drainpipe = new ArrayList<String>();
		final int numberDrained = cbq.drainTo(drainpipe);
		assertEquals(3, drainpipe.size()); //diamonds should coalesce with delight
		assertEquals(3, numberDrained);
		assertEquals(horse, drainpipe.get(0));
		assertEquals(fool, drainpipe.get(1));
		assertEquals(delight, drainpipe.get(2));
	}
	
	/**
	 * @throws InterruptedException
	 * 
	 */
	public void testTakeCoalesces() throws InterruptedException {
		final CoalescingBlockingQueue<String, String> cbq =
			new CoalescingBlockingQueue<String, String> (
					new LinkedBlockingQueue<String>(),
					new AlwaysCoalescePolicy(),
					new HashCodeOfFirstLetterRedirector()
					);
		final String fool = new String("fool");
		final String diamonds = new String("diamonds");
		final String horse = new String("horse");
		final String delight = new String("delight");
		cbq.add(horse);
		cbq.add(diamonds);
		cbq.add(fool);
		cbq.add(delight);
		assertEquals(horse, cbq.take());
		assertEquals(fool, cbq.take());
		assertEquals(delight, cbq.take());
	}
	
	/**
	 * 
	 */
	public void testPeekAndElementPassThroughUnchanged() {
		final LinkedBlockingQueue<String> underlying = new LinkedBlockingQueue<String>();
		final String fool = new String("Fool");
		final String diamonds = new String("Diamonds");
		underlying.add(fool);
		underlying.add(diamonds);
		final CoalescingBlockingQueue<String, Integer> cbq =
			new CoalescingBlockingQueue<String, Integer> (
					underlying,
					new NeverCoalescePolicy(),
					new HashCodeRedirector()
					);
		assertEquals(cbq.peek(), underlying.peek());
		assertEquals(cbq.element(), underlying.element());
	}
	
	private class NeverCoalescePolicy implements CoalescingPolicy<String> {
		public boolean shouldCoalesce(final String coalesceCandidate) {
			return false;
		}
	}
	
	private class AlwaysCoalescePolicy implements CoalescingPolicy<String> {

		public boolean shouldCoalesce(final String coalesceCandidate) {
			return true;
		}
		
	}
	
	private class HashCodeRedirector implements LockSmith<String, Integer> {

		public Integer makeKey(final String toGenerateFrom) {
			return new Integer(toGenerateFrom.hashCode());
		}
		
	}
	
	private class HashCodeOfFirstLetterRedirector implements LockSmith<String, String> {

		public String makeKey(final String toGenerateFrom) {
			return toGenerateFrom.substring(0,1);
		}
		
	}
}
