package karmaresearch.vlog;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import karmaresearch.vlog.Term.TermType;

/**
 * The <code>VLog</code> class exposes, at a low level, VLog to Java.
 */
public class VLog {

    private static final boolean DEBUG = true;

    private static final AtomicInteger VlogCounter = new AtomicInteger(0);

    static {
        loadLibrary("kognac-log");
        loadLibrary("kognac");
        loadLibrary("trident-core");
        loadLibrary("trident-sparql");
        loadLibrary("vlog-core");
        loadLibrary("vlog-java");
    };

    private static void loadLibrary(String s) {
        // First try to just load the shared library.
        try {
            System.loadLibrary(s);
            if (DEBUG) {
                System.out
                        .println("Loaded " + s + " with System.loadLibrary()");
            }
        } catch (Throwable ex) {
            // Did not work, now try to load it from the same directory as the
            // jar file. First determine prefix and suffix, depending on OS.

            // First determine jar file;
            File jarFile;
            try {
                jarFile = new File(VLog.class.getProtectionDomain()
                        .getCodeSource().getLocation().toURI());
            } catch (Throwable e) {
                throw new UnsatisfiedLinkError(e.getMessage());
            }

            // Next, determine OS.
            String os = System.getProperty("os.name");
            String nativeSuffix = ".so";
            String nativePrefix = "lib";
            if (os != null) {
                os = os.toLowerCase();
                if (os.contains("windows")) {
                    nativePrefix = "";
                    nativeSuffix = ".dll";
                } else if (os.contains("mac")) {
                    nativeSuffix = ".dylib";
                }
            }

            // Determine library name.
            String libName = nativePrefix + s + nativeSuffix;

            try {
                loadFromDir(jarFile, libName);
                if (DEBUG) {
                    System.out.println("Loaded " + s
                            + " from the directory in which the jar resides");
                }
            } catch (Throwable e) {
                try {
                    loadFromJar(jarFile, libName, os);
                    if (DEBUG) {
                        System.out.println("Loaded " + s + " from the jar");
                    }
                } catch (Throwable e1) {
                    System.err.println("Could not load library " + s
                            + ", exceptions are temporarily disabled to accomodate windows version");
                    // throw new UnsatisfiedLinkError(e1.getMessage());
                }
            }
        }
    }

    private static void loadFromDir(File jarFile, String libName) {
        String dir = jarFile.getParent();
        if (dir == null) {
            dir = ".";
        }
        // Only support one size, i.e. 64-bit?
        String lib = dir + File.separator + libName;
        System.load(lib);
    }

    private static void loadFromJar(File jarFile, String libName, String os)
            throws IOException {
        InputStream is = new BufferedInputStream(
                VLog.class.getResourceAsStream("/resources/" + libName));
        File targetDir = Files.createTempDirectory("VLog-tmp").toFile();
        targetDir.deleteOnExit();
        File target = new File(targetDir, libName);
        target.deleteOnExit();
        targetDir.deleteOnExit();
        Files.copy(is, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
        try {
            System.load(target.getAbsolutePath());
        } finally {
            if (os == null || !os.contains("windows")) {
                // If not on windows, we can delete the files now.
                target.delete();
                targetDir.delete();
            }
        }
    }

    // Identification of this VLog instance.
    private final int myVlog;

    public VLog() {
        myVlog = VlogCounter.getAndAdd(1);
    }

    /**
     * Currently implemented rule rewriting strategies.
     *
     * <code>NONE</code> means heads are never split, <code>AGGRESSIVE</code>
     * means heads are split whenever possible.
     */
    public enum RuleRewriteStrategy {
        NONE, AGGRESSIVE
    };

    /**
     * Sets the log level of the VLog logger. Possible values of the parameter
     * are: "debug", "info", "warning", "error". The default log level is
     * "info". If the specified level string is not recognized, the default is
     * used.
     *
     * @param level
     *            the log level.
     *
     */
    public native void setLogLevel(String level);

    /**
     * Redirects the logging of VLog to a file.
     *
     * @param filename
     *            the file.
     *
     */
    public native void setLogFile(String filename);

    /**
     * Starts vlog with the specified edb configuration. The edb configuration
     * can either be specified directly as a string, in which case the
     * <code>isFile</code> parameter should be <code>false</code>, or as a file
     * name, in which case the <code>isFile</code> parameter should be
     * <code>true</code>.
     *
     * @param edbconfig
     *            the edb configuration, as a string or as a filename.
     * @param isFile
     *            whether it is a file, or an edb configuration as a string.
     * @exception IOException
     *                is thrown when the database could not be read for some
     *                reason, or <code>isFile</code> is set and the file does
     *                not exist.
     * @exception AlreadyStartedException
     *                is thrown when vlog was already started, and not stopped.
     * @exception EDBConfigurationException
     *                is thrown when there is an error in the EDB configuration.
     */
    public native void start(String edbconfig, boolean isFile)
            throws AlreadyStartedException, EDBConfigurationException,
            IOException;

    /**
     * Adds the data for the specified predicate to the database. If VLog is not
     * started yet, it will be started with an empty configuration.
     *
     * @param predicate
     *            the predicate
     * @param contents
     *            the data
     * @exception EDBConfigurationException
     *                is thrown when the rows don't all have the same arity.
     */
    public native void addData(String predicate, String[][] contents)
            throws EDBConfigurationException;

    /**
     * Stops and de-allocates the reasoner. If vlog is not started yet, this
     * call does nothing, so it does no harm to call it more than once.
     */
    public native void stop();

    /**
     * Returns the internal representation of the predicate. Internally, VLog
     * uses integers to represent predicates. This method allows the user to
     * look up this internal number.
     *
     * @param predicate
     *            the predicate to look up
     * @return the predicate id, or -1 if not found.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public native int getPredicateId(String predicate)
            throws NotStartedException;

    /**
     * Returns the predicate. Internally, VLog uses integers to represent
     * predicates. This method allows the user to look up the predicate name,
     * when provided with the predicate id.
     *
     * @param predicateId
     *            the predicate to look up
     * @return the predicate string, or <code>null</code> if not found.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public native String getPredicate(int predicateId)
            throws NotStartedException;

    /**
     * Returns the arity of the predicate.
     *
     * @param predicate
     *            the predicate to look up
     * @return the predicate arity, or -1 if not found.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public native int getPredicateArity(String predicate)
            throws NotStartedException;

    /**
     * Returns the internal representation of the constant. Internally, VLog
     * uses longs to represent constants. This method allows the user to look up
     * this internal number.
     *
     * @param constant
     *            the constant to look up
     * @return the constant id, or -1 if not found.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public native long getConstantId(String constant)
            throws NotStartedException;

    /**
     * Returns the constant. Internally, VLog uses longs to represent constants.
     * This method allows the user to look up the constant string, when provided
     * with the constant id.
     *
     * @param constantId
     *            the constant to look up
     * @return the constant string, or <code>null</code> if not found.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public native String getConstant(long constantId)
            throws NotStartedException;

    /**
     * Queries the current, so possibly materialized, database, and returns an
     * iterator that delivers the answers, one by one.
     *
     * TODO: is having variables as negative values OK?
     *
     * @param predicate
     *            the predicate id of the query
     * @param terms
     *            the constant values or variables. If the term is negative, it
     *            is assumed to be a variable.
     * @param includeConstants
     *            whether to include the constants in the results.
     * @param filterBlanks
     *            whether results with blanks in them should be filtered out
     * @return the result iterator.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public native QueryResultIterator query(int predicate, long[] terms,
            boolean includeConstants, boolean filterBlanks)
            throws NotStartedException;

    private long[] extractTerms(Term[] terms) throws NotStartedException {
        ArrayList<String> variables = new ArrayList<>();
        long[] longTerms = new long[terms.length];
        for (int i = 0; i < terms.length; i++) {
            if (terms[i].getTermType() == TermType.VARIABLE) {
                boolean found = false;
                for (int j = 0; j < variables.size(); j++) {
                    if (variables.get(j).equals(terms[i].getName())) {
                        found = true;
                        longTerms[i] = -j - 1;
                        break;
                    }
                }
                if (!found) {
                    variables.add(terms[i].getName());
                    longTerms[i] = -variables.size();
                }
            } else {
                longTerms[i] = getConstantId(terms[i].getName());
                if (longTerms[i] == -1) {
                    // Non-existing ..., but make sure that VLog won't interpret
                    // it as a variable.
                    longTerms[i] = Long.MAX_VALUE;
                }
            }
        }
        return longTerms;
    }

    /**
     * Queries the current, so possibly materialized, database, and returns an
     * iterator that delivers the answers, one by one.
     *
     * TODO: deal with not-found predicates, terms.
     *
     * @param query
     *            the query, as an atom.
     * @return the result iterator.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public TermQueryResultIterator query(Atom query)
            throws NotStartedException {
        return query(query, true, false);
    }

    /**
     * Queries the current, so possibly materialized, database, and returns an
     * iterator that delivers the answers, one by one.
     *
     * TODO: deal with not-found predicates, terms.
     *
     * @param query
     *            the query, as an atom.
     * @param includeConstants
     *            whether to include the constants of the query in the results.
     * @param filterBlanks
     *            whether results with blanks in them should be filtered out
     * @return the result iterator.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */

    public TermQueryResultIterator query(Atom query, boolean includeConstants,
            boolean filterBlanks) throws NotStartedException {
        query.checkNoBlank();
        int intPred = getPredicateId(query.getPredicate());
        long[] longTerms = extractTerms(query.getTerms());
        return new TermQueryResultIterator(this,
                query(intPred, longTerms, includeConstants, filterBlanks));
    }

    private native void queryToCsv(int predicate, long[] term, String fileName,
            boolean filterBlanks) throws IOException;

    /**
     * Writes the result of a query to a CSV file.
     *
     * @param query
     *            the query
     * @param fileName
     *            the file to write to.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet, or materialization
     *                has not run yet
     * @exception IOException
     *                is thrown when the file could not be written for some
     *                reason
     */
    public void writeQueryResultsToCsv(Atom query, String fileName)
            throws NotStartedException, IOException {
        writeQueryResultsToCsv(query, fileName, false);
    }

    /**
     * Writes the result of a query to a CSV file.
     *
     * @param query
     *            the query
     * @param fileName
     *            the file to write to.
     * @param filterBlanks
     *            whether results with blanks in them should be filtered out
     * @exception NotStartedException
     *                is thrown when vlog is not started yet, or materialization
     *                has not run yet
     * @exception IOException
     *                is thrown when the file could not be written for some
     *                reason
     */
    public void writeQueryResultsToCsv(Atom query, String fileName,
            boolean filterBlanks) throws NotStartedException, IOException {
        query.checkNoBlank();
        int intPred = getPredicateId(query.getPredicate());
        long[] longTerms = extractTerms(query.getTerms());
        queryToCsv(intPred, longTerms, fileName, filterBlanks);
    }

    /**
     * Sets the rules for the VLog run. Any existing rules are removed.
     *
     * @param rules
     *            the rules
     * @param rewriteStrategy
     *            whether multiple-head rules should be rewritten when possible.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public native void setRules(Rule[] rules,
            RuleRewriteStrategy rewriteStrategy) throws NotStartedException;

    /**
     * Sets the rules for the VLog run to the rules in the specified file. Any
     * existing rules are removed. For testing only.
     *
     * @param rulesFile
     *            the file name of the file containing the rules
     * @param rewriteStrategy
     *            whether multiple-head rules should be rewritten when possible.
     *
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     * @exception IOException
     *                is thrown when the file could not be read for some reason
     */
    private native void setRulesFile(String rulesFile,
            RuleRewriteStrategy rewriteStrategy)
            throws NotStartedException, IOException;

    /**
     * Materializes the database under the specified rules.
     *
     * TODO: maybe limit number of iterations? (Currently not in vlog, but could
     * be added)
     *
     * @param skolem
     *            whether to use skolem chase <code>true</code> or restricted
     *            chase <code>false</code>.
     * @exception NotStartedException
     *                is thrown when vlog is not started yet.
     */
    public native void materialize(boolean skolem) throws NotStartedException;

    /**
     * Creates a CSV file at the specified location, for the specified
     * predicate.
     *
     * @param predicateName
     *            the predicate name
     * @param fileName
     *            the location
     * @exception NotStartedException
     *                is thrown when vlog is not started yet, or materialization
     *                has not run yet
     * @exception IOException
     *                is thrown when the file could not be written for some
     *                reason
     */
    public native void writePredicateToCsv(String predicateName,
            String fileName) throws NotStartedException, IOException;

    @Override
    protected void finalize() {
        stop();
    }

    // For testing purposes ...
    public static void main(String[] args) throws Exception {
        Test.runTest(args[0]);
    }
}
