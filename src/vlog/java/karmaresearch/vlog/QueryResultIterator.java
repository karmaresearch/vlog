package karmaresearch.vlog;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Encapsulates the result of a query.
 */
public class QueryResultIterator implements Iterator<long[]> {

    private final long handle;
    private boolean cleaned = false;

    /**
     * Creates a query result iterator. The parameter provided is a handle to a
     * native underlying object. This constructor is to be called from native
     * code.
     *
     * @param handle
     *            the handle.
     */
    public QueryResultIterator(long handle) {
        this.handle = handle;
    }

    /**
     * Returns whether there are more results to the query.
     *
     * @return whether there are more results.
     */
    public boolean hasNext() {
        return hasNext(handle);
    }

    /**
     * Returns the next result.
     *
     * @return the next result
     * @exception NoSuchElementException
     *                is thrown when no more elements exist.
     */
    public long[] next() {
        long[] v = next(handle);
        if (v == null) {
            throw new NoSuchElementException("No more query results");
        }
        return v;
    }

    /**
     * Cleans up the underlying VLog iterator, if not done before.
     */
    public void cleanup() {
        if (!cleaned) {
            cleanup(handle);
            cleaned = true;
        }
    }

    // Called by GC. In case we forget.
    @Override
    protected void finalize() {
        cleanup();
    }

    private native void cleanup(long handle);

    private native boolean hasNext(long handle);

    private native long[] next(long handle);
};
