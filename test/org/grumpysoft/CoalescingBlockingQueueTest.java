package org.grumpysoft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
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
	
	/**
	 * 
	 */
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
	
	/**
	 * 
	 */
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
	 * 
	 */
	public void testPartialDrainTo() {
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
		final int numberDrained = cbq.drainTo(drainpipe, 2);
		assertEquals(2, numberDrained);
		assertEquals(drainpipe.get(0), horse);
		assertEquals(drainpipe.get(1), fool);
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
	 * @throws InterruptedException
	 */
	public void testPollCoalesces() throws InterruptedException {
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
		assertEquals(horse, cbq.poll());
		assertEquals(fool, cbq.poll());
		assertEquals(delight, cbq.poll());
		assertEquals(null, cbq.poll());
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
	
	/**
	 * 
	 */
	public void testRemoveIsSane() {
		final CoalescingBlockingQueue<String, String> cbq =
			new CoalescingBlockingQueue<String, String> (
					new LinkedBlockingQueue<String>(),
					new AlwaysCoalescePolicy(),
					new HashCodeOfFirstLetterRedirector()
					);
		try {
			cbq.remove();
			fail();
		}
		catch (final NoSuchElementException nse) {
			//good!
		}
		final String fool = new String("fool");
		final String diamonds = new String("diamonds");
		final String horse = new String("horse");
		final String delight = new String("delight");
		cbq.add(horse);
		cbq.add(diamonds);
		cbq.add(fool);
		cbq.add(delight);
		assertEquals(cbq.remove(), horse);
		assertEquals(cbq.remove(), fool);
		assertEquals(cbq.remove(), delight);
		try {
			cbq.remove();
			fail();
		}
		catch (final NoSuchElementException nse) {
			//good!
		}
	}
	
	/**
	 * @throws InterruptedException 
	 * 
	 */
	public void testAddAllUpdatesLatestAndPassesThrough() throws InterruptedException {
		final LinkedBlockingQueue<String> underlying = new LinkedBlockingQueue<String>();
		final CoalescingBlockingQueue<String, String> cbq =
			new CoalescingBlockingQueue<String, String> (
					underlying,
					new AlwaysCoalescePolicy(),
					new HashCodeOfFirstLetterRedirector()
					);
		final String fool = new String("fool");
		final String diamonds = new String("diamonds");
		final String horse = new String("horse");
		final String delight = new String("delight");
		ArrayList<String> someStrings = new ArrayList<String>();
		someStrings.add(fool);
		someStrings.add(diamonds);
		someStrings.add(horse);
		someStrings.add(delight);
		assertTrue(cbq.addAll(someStrings));
		assertEquals(4,underlying.size());
		assertEquals(fool, underlying.peek());
		assertEquals(fool, cbq.take());
		assertEquals(horse, cbq.take());
		assertEquals(delight, cbq.take());
	}
	
	/**
	 * 
	 */
	public void testIterator() {
		final CoalescingBlockingQueue<String,String> cbq =
				new CoalescingBlockingQueue<String, String> (
						new LinkedBlockingQueue<String>(),
						new AlwaysCoalescePolicy(),
						new HashCodeOfFirstLetterRedirector()
						);
		Iterator<String> it = cbq.iterator();
		assertTrue(!it.hasNext());
		final String fool = new String("fool");
		final String diamonds = new String("diamonds");
		final String horse = new String("horse");
		final String delight = new String("delight");
		cbq.add(fool);
		cbq.add(diamonds);
		cbq.add(horse);
		cbq.add(delight);
		it = cbq.iterator();
		assertTrue(it.hasNext());
		while (it.hasNext()) {
			String el = it.next();
			assertFalse(diamonds.equals(el));
		}
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
