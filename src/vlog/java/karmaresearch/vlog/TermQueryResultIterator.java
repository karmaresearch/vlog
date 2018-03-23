package karmaresearch.vlog;

import java.util.Iterator;
import java.util.NoSuchElementException;

import karmaresearch.vlog.Term.TermType;

/**
 * Encapsulates the result of a query.
 */
public class TermQueryResultIterator
        implements Iterator<Term[]>, AutoCloseable {

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
    public TermQueryResultIterator(VLog vlog, QueryResultIterator iter) {
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
    public Term[] next() {
        long[] v = iter.next();
        Term[] result = new Term[v.length];
        for (int i = 0; i < v.length; i++) {
            try {
                long val = v[i];
                String s = vlog.getConstant(val);
                if (s == null) {
                    result[i] = new Term(TermType.BLANK, "" + (val >> 40) + "_"
                            + ((val >> 32) & 0377) + "_" + (val & 0xffffffffL));

                } else {
                    result[i] = new Term(TermType.CONSTANT, s);
                }
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
