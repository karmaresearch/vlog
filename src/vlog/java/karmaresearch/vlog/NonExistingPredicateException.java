package karmaresearch.vlog;

/**
 * This exception gets thrown when an attempt is made to perform a query with a non-existing predicate.
 */

public class NonExistingPredicateException extends Exception {

    private static final long serialVersionUID = 877780226511090797L;

    /**
     * Constructs an <code>NonExistingPredicateException</code> with <code>null</code> as
     * its error detail message.
     */
    public NonExistingPredicateException() {
        super();
    }

    /**
     * Constructs an <code>NonExistingPredicateException</code> with the specified detail
     * message.
     *
     * @param message
     *            the detail message
     */
    public NonExistingPredicateException(String message) {
        super(message);
    }

    /**
     * Constructs an <code>NonExistingPredicateException</code> with the specified cause.
     *
     * @param cause
     *            the cause
     */
    public NonExistingPredicateException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an <code>NonExistingPredicateException</code> with the specified detail
     * message and cause.
     *
     * @param message
     *            the detail message
     * @param cause
     *            the cause
     */
    public NonExistingPredicateException(String message, Throwable cause) {
        super(message, cause);
    }
}
