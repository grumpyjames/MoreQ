package org.grumpysoft;

public interface CoalescingPolicy<T> {
	public boolean shouldCoalesce(final T coalesceCandidate);
}
