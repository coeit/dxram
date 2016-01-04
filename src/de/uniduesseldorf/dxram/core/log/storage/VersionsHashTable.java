
package de.uniduesseldorf.dxram.core.log.storage;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.log.EpochVersion;

/**
 * HashTable to store versions (Linear probing)
 * @author Kevin Beineke
 *         28.11.2014
 */
public class VersionsHashTable {

	// Attributes
	private int[] m_table;
	private int m_count;
	private int m_intCapacity;
	private int m_elementCapacity;
	private int m_threshold;
	private float m_loadFactor;

	private ReentrantLock m_lock;

	// Constructors
	/**
	 * Creates an instance of VersionsHashTable
	 * @param p_initialElementCapacity
	 *            the initial capacity of VersionsHashTable
	 * @param p_loadFactor
	 *            the load factor of VersionsHashTable
	 */
	public VersionsHashTable(final int p_initialElementCapacity, final float p_loadFactor) {
		super();

		m_count = 0;
		m_elementCapacity = p_initialElementCapacity;
		m_intCapacity = m_elementCapacity * 4;
		m_loadFactor = p_loadFactor;

		if (m_elementCapacity == 0) {
			m_table = new int[4];
			m_threshold = (int) m_loadFactor;
		} else {
			m_table = new int[m_intCapacity];
			m_threshold = (int) (m_elementCapacity * m_loadFactor);
		}

		m_lock = new ReentrantLock(false);
	}

	// Getter
	/**
	 * Returns the number of keys in VersionsHashTable
	 * @return the number of keys in VersionsHashTable
	 */
	public final int size() {
		return m_count;
	}

	/**
	 * Checks if VersionsHashTable is empty
	 * @return true if VersionsHashTable maps no keys to values, false otherwise
	 */
	public final boolean isEmpty() {
		return m_count == 0;
	}

	// Methods
	/**
	 * Returns the value to which the specified key is mapped in VersionsHashTable
	 * @param p_key
	 *            the searched key (is incremented before insertion to avoid 0)
	 * @return the value to which the key is mapped in VersionsHashTable
	 */
	public final EpochVersion get(final long p_key) {
		EpochVersion ret = null;
		int index;
		long iter;
		final long key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		m_lock.lock();
		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				ret = new EpochVersion((short) getEpoch(index), getVersion(index));
				break;
			}
			iter = getKey(++index);
		}
		m_lock.unlock();

		return ret;
	}

	/**
	 * Maps the given key to the given value in VersionsHashTable
	 * @param p_key
	 *            the key (is incremented before insertion to avoid 0)
	 * @param p_epoch
	 *            the epoch
	 * @param p_version
	 *            the version
	 */
	public void put(final long p_key, final int p_epoch, final int p_version) {
		int index;
		long iter;
		final long key = p_key + 1;

		index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

		m_lock.lock();
		iter = getKey(index);
		while (iter != 0) {
			if (iter == key) {
				set(index, key, p_epoch, p_version);
				break;
			}
			iter = getKey(++index);
		}
		if (iter == 0) {
			// Key unknown until now
			set(index, key, p_epoch, p_version);
			m_count++;
		}

		if (m_count >= m_threshold) {
			rehash();
		}
		m_lock.unlock();
	}

	/**
	 * Clears VersionsHashTable
	 */
	public final void clear() {
		Arrays.fill(m_table, 0);
		m_count = 0;
	}

	/**
	 * Gets the key at given index
	 * @param p_index
	 *            the index
	 * @return the key
	 */
	private long getKey(final int p_index) {
		int index;

		index = p_index % m_elementCapacity * 4;
		return (long) m_table[index] << 32 | m_table[index + 1] & 0xFFFFFFFFL;
	}

	/**
	 * Gets the epoch at given index
	 * @param p_index
	 *            the index
	 * @return the epoch
	 */
	private int getEpoch(final int p_index) {
		return m_table[p_index % m_elementCapacity * 4 + 2];
	}

	/**
	 * Gets the version at given index
	 * @param p_index
	 *            the index
	 * @return the version
	 */
	private int getVersion(final int p_index) {
		return m_table[p_index % m_elementCapacity * 4 + 3];
	}

	/**
	 * Sets the key-value tuple at given index
	 * @param p_index
	 *            the index
	 * @param p_key
	 *            the key
	 * @param p_epoch
	 *            the epoch
	 * @param p_version
	 *            the version
	 */
	private void set(final int p_index, final long p_key, final int p_epoch, final int p_version) {
		int index;

		index = p_index % m_elementCapacity * 4;
		m_table[index] = (int) (p_key >> 32);
		m_table[index + 1] = (int) p_key;
		m_table[index + 2] = p_epoch;
		m_table[index + 3] = p_version;
	}

	/**
	 * Hashes the given key
	 * @param p_key
	 *            the key
	 * @return the hash value
	 */
	private int hash(final long p_key) {
		long hash = p_key;

		hash ^= hash >>> 20 ^ hash >>> 12;
		return (int) (hash ^ hash >>> 7 ^ hash >>> 4);
	}

	/**
	 * Increases the capacity of and internally reorganizes VersionsHashTable
	 */
	private void rehash() {
		int index = 0;
		int oldCapacity;
		int[] oldMap;
		int[] newMap;

		oldCapacity = m_intCapacity;
		oldMap = m_table;

		m_elementCapacity = m_elementCapacity * 2 + 1;
		m_intCapacity = m_elementCapacity * 4;
		newMap = new int[m_intCapacity];
		m_threshold = (int) (m_elementCapacity * m_loadFactor);
		m_table = newMap;

		m_count = 0;
		while (index < oldCapacity) {
			if (((long) oldMap[index] << 32 | oldMap[index + 1] & 0xFFFFFFFFL) != 0) {
				put(((long) oldMap[index] << 32 | oldMap[index + 1] & 0xFFFFFFFFL) - 1, oldMap[index + 2], oldMap[index + 3]);
			}
			index += 4;
		}
	}

}
