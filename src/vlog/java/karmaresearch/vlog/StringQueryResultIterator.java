package karmaresearch.vlog;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Encapsulates the result of a query.
 */
public class StringQueryResultIterator
        implements Iterator<String[]>, AutoCloseable {

    private final VLog vlog;
    private final QueryResultIterator iter;

    /**
     * Creates a query result enumeration with the results as strings.
     *
     * @param vlog
     *            the VLog JNI interface object.
     * @param iter
     *            the underlying vlog result enumeration.
     */
    public StringQueryResultIterator(VLog vlog, QueryResultIterator iter) {
        this.vlog = vlog;
        this.iter = iter;
    }

    /**
     * Returns whether there are more results to the query.
     *
     * @return whether there are more results.
     */
    public boolean hasNext() {
        return iter.hasNext();
    }

    /**
     * Returns the next result.
     *
     * @return the next result
     * @exception NoSuchElementException
     *                is thrown when no more elements exist.
     */
    public String[] next() {
        long[] v = iter.next();
        String[] result = new String[v.length];
        for (int i = 0; i < v.length; i++) {
            try {
                result[i] = vlog.getConstant(v[i]);
            } catch (NotStartedException e) {
                // Should not happen, we just did a query ...
            }
        }
        return result;
    }

    @Deprecated
    public void cleanup() {
        close();
    }

    public void close() {
        iter.close();
    }
};
