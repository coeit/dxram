
package de.uniduesseldorf.utils;

/**
 * Wrapper for accessing GNU readline lib
 * @author Michael Schoettner 07.09.2015
 */
public final class JNIconsole {

	/**
	 * Constructor
	 */
	private JNIconsole() {}

	// Statics
	/**
	 * Provide the path to the native implementation.
	 * @param p_pathNativeLibrary Path to the library with the native implementation.
	 */
	public static void load(final String p_pathNativeLibrary) {
		System.load(p_pathNativeLibrary);
	}

	// Methods

	/**
	 * Read one line from console.
	 * @return the read line
	 */
	public static native byte[] readline();

}
