package org.grumpysoft;

public interface LockSmith<KeySource,KeyType> {
	public KeyType makeKey(KeySource toGenerateFrom);
}
