package karmaresearch.vlog;

import java.util.Enumeration;
import java.util.NoSuchElementException;

/**
 * Encapsulates the result of a query.
 */
public class StringQueryResultEnumeration implements Enumeration<String[]> {

    private final VLog vlog;
    private final QueryResultEnumeration iter;

    /**
     * Creates a query result enumeration with the results as strings.
     *
     * @param vlog
     *            the VLog JNI interface object.
     * @param iter
     *            the underlying vlog result enumeration.
     */
    public StringQueryResultEnumeration(VLog vlog,
            QueryResultEnumeration iter) {
        this.vlog = vlog;
        this.iter = iter;
    }

    /**
     * Returns whether there are more results to the query.
     *
     * @return whether there are more results.
     */
    public boolean hasMoreElements() {
        return iter.hasMoreElements();
    }

    /**
     * Returns the next result.
     *
     * @return the next result
     * @exception NoSuchElementException
     *                is thrown when no more elements exist.
     */
    public String[] nextElement() {
        long[] v = iter.nextElement();
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

    public void cleanup() {
        iter.cleanup();
    }
};
