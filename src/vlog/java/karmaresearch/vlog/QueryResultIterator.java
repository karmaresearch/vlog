package karmaresearch.vlog;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Encapsulates the result of a query.
 */
public class QueryResultIterator implements Iterator<long[]>, AutoCloseable {

    private final long handle;
    private boolean cleaned = false;
    private boolean hasNextCalled = false;
    private boolean hasNextValue = false;
    private final boolean filterBlanks;
    private long[] saved = null;

    /**
     * Creates a query result iterator. The parameter provided is a handle to a
     * native underlying object. This constructor is to be called from native
     * code.
     *
     * @param handle
     *            the handle.
     * @param filterBlanks
     *            whether results with blanks in them should be filtered out
     */
    public QueryResultIterator(long handle, boolean filterBlanks) {
        this.handle = handle;
        this.filterBlanks = filterBlanks;
    }

    /**
     * Returns whether there are more results to the query.
     *
     * @return whether there are more results.
     */
    public boolean hasNext() {
        if (!filterBlanks) {
            return hasNext(handle);
        }
        if (hasNextCalled) {
            return hasNextValue;
        }
        hasNextCalled = true;
        hasNextValue = false;
        while (hasNext(handle)) {
            saved = next(handle);
            if (!hasBlanks(saved)) {
                hasNextValue = true;
                break;
            }
        }
        return hasNextValue;
    }

    /**
     * Returns the next result.
     *
     * @return the next result
     * @exception NoSuchElementException
     *                is thrown when no more elements exist.
     */
    public long[] next() {
        if (!filterBlanks) {
            long[] v = next(handle);
            if (v == null) {
                throw new NoSuchElementException("No more query results");
            }
            return v;
        }
        if (!hasNextCalled) {
            if (!hasNext()) {
                throw new NoSuchElementException("No more query results");
            }
        }
        if (!hasNextValue) {
            throw new NoSuchElementException("No more query results");
        }
        long[] retval = saved;
        hasNextCalled = false;
        saved = null;
        return retval;
    }

    /**
     * Cleans up the underlying VLog iterator, if not done before.
     */
    @Deprecated
    public void cleanup() {
        close();
    }

    // Called by GC. In case we forget.
    @Override
    protected void finalize() {
        close();
    }

    private native void cleanup(long handle);

    private native boolean hasNext(long handle);

    private native long[] next(long handle);

    private native boolean hasBlanks(long[] v);

    @Override
    public void close() {
        if (!cleaned) {
            cleanup(handle);
            cleaned = true;
        }
    }
};
