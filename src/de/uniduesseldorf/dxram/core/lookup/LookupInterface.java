
package de.uniduesseldorf.dxram.core.lookup;

import java.util.List;

import de.uniduesseldorf.dxram.core.CoreComponent;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;

/**
 * Methods for accesing the meta data
 * @author Florian Klein
 *         09.03.2012
 */
public interface LookupInterface extends CoreComponent {

	// Methods
	/**
	 * Initializes chunk handler
	 * @throws DXRAMException
	 *             if the component could not be initialized
	 */
	void initChunkHandler() throws DXRAMException;

	/**
	 * Get the corresponding NodeID for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @return the corresponding NodeID
	 * @throws LookupException
	 *             if the NodeID could not be get
	 */
	Locations get(long p_chunkID) throws LookupException;

	/**
	 * Store migration in meta-data management
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 */
	void migrate(long p_chunkID, short p_nodeID) throws LookupException;

	/**
	 * Store migration of ID range in meta-data management
	 * @param p_startCID
	 *            the first ID
	 * @param p_endCID
	 *            the last ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 */
	void migrateRange(long p_startCID, long p_endCID, short p_nodeID) throws LookupException;

	/**
	 * Store migration in meta-data management; Concerns not created chunks during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 * @note is called during promotion and is unsafe (no requests possible!)
	 */
	void migrateNotCreatedChunk(long p_chunkID, short p_nodeID) throws LookupException;

	/**
	 * Store migration in meta-data management; Concerns own chunks during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_nodeID
	 *            the NodeID
	 * @throws LookupException
	 *             if the migration could not be executed
	 * @note is called during promotion and is unsafe (no requests possible!)
	 */
	void migrateOwnChunk(long p_chunkID, short p_nodeID) throws LookupException;

	/**
	 * Initializes the given ID range in CIDTree
	 * @param p_firstChunkIDOrRangeID
	 *            the last ID
	 * @param p_locations
	 *            the NodeID of creator and backup nodes
	 * @throws LookupException
	 *             if the initialization could not be executed
	 */
	void initRange(long p_firstChunkIDOrRangeID, Locations p_locations) throws LookupException;

	/**
	 * Remove the corresponding NodeID for the given ID
	 * @param p_chunkID
	 *            the ID
	 * @throws LookupException
	 *             if the NodeID could not be removed
	 */
	void remove(long p_chunkID) throws LookupException;

	/**
	 * Insert identifier to ChunkID mapping
	 * @param p_id
	 *            the identifier
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws LookupException
	 *             if the id could not be inserted
	 */
	void insertID(int p_id, long p_chunkID) throws LookupException;

	/**
	 * Insert identifier to ChunkID mapping
	 * @param p_id
	 *            the identifier
	 * @throws LookupException
	 *             if the id could not be found
	 * @return the ChunkID
	 */
	long getChunkID(int p_id) throws LookupException;

	/**
	 * Return the number of identifier mappings
	 * @return the number of mappings
	 * @throws LookupException
	 *             if mapping count could not be gotten
	 */
	long getMappingCount() throws LookupException;

	/**
	 * Invalidates the cache entry for given ChunkIDs
	 * @param p_chunkIDs
	 *            the IDs
	 */
	void invalidate(final long... p_chunkIDs);

	/**
	 * Invalidates the cache entry for given ChunkID range
	 * @param p_startCID
	 *            the first ChunkID
	 * @param p_endCID
	 *            the last ChunkID
	 */
	void invalidate(final long p_startCID, final long p_endCID);

	/**
	 * Returns if given node is still available as a peer
	 * @param p_creator
	 *            the creator's NodeID
	 * @return true if given node is available and a peer, false otherwise
	 */
	boolean creatorAvailable(final short p_creator);

	/**
	 * Verifies if there is another node in the superpeer overlay
	 * @return true if there is another node, false otherwise
	 */
	boolean isLastSuperpeer();

	/**
	 * Returns a list with all superpeers
	 * @return all superpeers
	 */
	List<Short> getSuperpeers();

	/**
	 * Checks if all superpeers are known
	 * @return true if all superpeers are known, false otherwise
	 */
	boolean overlayIsStable();
}