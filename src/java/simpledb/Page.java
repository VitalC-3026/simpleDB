package simpledb;

/**
 * Page is the interface used to represent pages that are resident in the
 * BufferPool.  Typically, DbFiles will read and write pages from disk.
 * <p>
 * Pages may be "dirty", indicating that they have been modified since they
 * were last written out to disk.
 *
 * For recovery purposes, pages MUST have a single constructor of the form:
 *     Page(PageId id, byte[] data)
 */
public interface Page {

    /**
     * Return the id of this page.  The id is a unique identifier for a page
     * that can be used to look up the page on disk or determine if the page
     * is resident in the buffer pool.
     *
     * @return the id of this page
     */
    public PageId getId();

    /**
     * Get the id of the transaction that last dirtied this page, or null if the page is clean..
     *
     * @return The id of the transaction that last dirtied this page, or null
     */
    public TransactionId isDirty();

  /**
   * Set the dirty state of this page as dirtied by a particular transaction
   */
    public void markDirty(boolean dirty, TransactionId tid);

  /**
   * Generates a byte array representing the contents of this page.
   * Used to serialize this page to disk.
   * <p>
   * The invariant here is that it should be possible to pass the byte array
   * generated by getPageData to the Page constructor and have it produce
   * an identical Page object.
   *
   * @return A byte array correspond to the bytes of this page.
   */

    public byte[] getPageData() throws NoSuchFieldException;

    /** Provide a representation of this page before any modifications were made
        to it.  Used by recovery.
    */
    public Page getBeforeImage();

    /*
     * a transaction that wrote this page just committed it.
     * copy current content to the before image.
     */
    public void setBeforeImage() throws NoSuchFieldException;
}
