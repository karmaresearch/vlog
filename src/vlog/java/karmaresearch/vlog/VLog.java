package karmaresearch.vlog;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The <code>VLog class exposes, at a low level, VLog to Java.
 */
public class VLog {

    static {
        // System.loadLibrary("kognac-log");
        // System.loadLibrary("trident-core");
        // System.loadLibrary("trident-sparql");
        System.loadLibrary("vlog_jni");
    }

    /**
     * Start vlog with the specified edb configuration, as a string.
     * 
     * TODO: special exception for parse error in the configuration?
     *
     * @param edbconfig the edb configuration, as a string.
     * @exception IOException is thrown when the database could not be read for some reason.
     * @exception AlreadyStartedException is thrown when vlog was already started, and not stopped.
     */
    public native void start(String edbconfig) throws AlreadyStartedException, IOException;

    /**
     * Start vlog with the database as CSV files, in a directory specified by the parameter.
     * All files in this directory with suffix ".csv" are considered. The predicate name is the 
     * prefix of the file name.
     * 
     * @param directory the directory in which the CSV files live.
     * @exception IOException is thrown when the database could not be loaded for some reason.
     * @exception AlreadyStartedException is thrown when vlog was already started, and not stopped.
     */
    public native void startCSV(String directory) throws IOException, AlreadyStartedException;

    /**
     * This method stops and de-allocates the reasoner. This method should be called before beginning
     * runs on another database. If vlog is not started yet, this call does nothing, so it does no
     * harm to call it more than once.
     */
    public native void stop();

    /**
     * Return the internal representation of the predicate.
     * Internally, VLog uses integers to represent predicates. This method allows the user to look up
     * this internal number.
     * @param predicate the predicate to look up
     * @return the predicate id, or -1 if not found.
     * @exception NotStartedException is thrown when vlog is not started yet.
     */
    public native int getPredicateId(String predicate) throws NotStartedException;

    /**
     * Return the predicate.
     * Internally, VLog uses integers to represent predicates. This method allows the user to look up
     * the predicate name, when provided with the predicate id.
     * @param predicateId the predicate to look up
     * @return the predicate string, or <code>null</code> if not found.
     * @exception NotStartedException is thrown when vlog is not started yet.
     */
    public native String getPredicate(int predicateId) throws NotStartedException;

    /**
     * Return the internal representation of the literal.
     * Internally, VLog uses longs to represent literals. This method allows the user to look up
     * this internal number.
     * @param literal the literal to look up
     * @return the literal id, or -1 if not found.
     * @exception NotStartedException is thrown when vlog is not started yet.
     */
    public native long getLiteralId(String literal) throws NotStartedException;

    /**
     * Return the literal.
     * Internally, VLog uses longs to represent literals. This method allows the user to look up
     * the literal string, when provided with the literal id.
     * @param literalId the literal to look up
     * @return the literal string, or <code>null</code> if not found.
     * @exception NotStartedException is thrown when vlog is not started yet.
     */
    public native String getLiteral(long literalId) throws NotStartedException;

    /**
     * Queries the current, so possibly materialized, database, and returns an iterator that delivers the answers,
     * one by one.
     * 
     * TODO: is having variables as negative values OK?
     *
     * @param predicate the predicate id of the query
     * @param terms the literal values or variables. If the term is negative, it is assumed to be a variable.
     * @exception NotStartedException is thrown when vlog is not started yet.
     */
    public native QueryResultEnumeration query(int predicate, long[] terms) throws NotStartedException;

    /**
     * Queries the current, so possibly materialized, database, and returns an iterator that delivers the answers,
     * one by one.
     *
     * TODO: is having variables start with a questionmark OK?
     *
     * TODO: deal with not-found predicates, terms.
     *
     * @param predicate the predicate of the query
     * @param terms the literals or variables. If the term starts with a question mark, it is a variable,
     * otherwise it is a literal.
     * @exception NotStartedException is thrown when vlog is not started yet.
     */
    public StringQueryResultEnumeration query(String predicate, String[] terms) throws NotStartedException {
        int intPred = getPredicateId(predicate);
        ArrayList<String> variables = new ArrayList<>();
        long[] longTerms = new long[terms.length];
        for (int i = 0; i < terms.length; i++) {
            if (terms[i].startsWith("?")) {
                boolean found = false;
                for (int j = 0; i < variables.size(); j++) {
                    if (variables.get(j).equals(terms[i])) {
                        found = true;
                        longTerms[i] = -j - 1;
                        break;
                    }
                }
                if (! found) {
                    variables.add(terms[i]);
                    longTerms[i] = -variables.size();
                }
            } else {
                longTerms[i] = getLiteralId(terms[i]);
            }
        }
        return new StringQueryResultEnumeration(this, query(intPred, longTerms));
    }

    /*
     * Materializes the database under the specified rules.
     *
     * TODO: We probably need flags: restricted or skolem, maybe limit number of iterations? (Currently not in vlog, but could be added)
     *
     * TODO: Should there be a separate method to specify rules?
     *
     * TODO: whether we should store the result of the materialization somewhere, for instance as CSV files?
     *
     * TODO: special exception for parse error in rules?
     *
     * @param rules the rules
     * @exception NotStartedException is thrown when vlog is not started yet.
     */
    public native void materialize(String[] rules) throws NotStartedException;

    // For testing purposes ...
    public static void main(String[] args) throws Exception {
        VLog vlog = new VLog();
        vlog.start("Test string");
    }
}
