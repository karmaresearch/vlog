package karmaresearch.vlog;

/**
 * This exception gets thrown when an attempt is made to materialize with a rule
 * set that either turns out not to be stratifiable, or the variables in a
 * negated atom cannot be bound.
 */

public class MaterializationException extends RuntimeException {

    private static final long serialVersionUID = 877780226511090797L;

    /**
     * Constructs an <code>MaterializationException</code> with
     * <code>null</code> as its error detail message.
     */
    public MaterializationException() {
        super();
    }

    /**
     * Constructs an <code>MaterializationException</code> with the specified
     * detail message.
     *
     * @param message
     *            the detail message
     */
    public MaterializationException(String message) {
        super(message);
    }

    /**
     * Constructs an <code>MaterializationException</code> with the specified
     * cause.
     *
     * @param cause
     *            the cause
     */
    public MaterializationException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an <code>MaterializationException</code> with the specified
     * detail message and cause.
     *
     * @param message
     *            the detail message
     * @param cause
     *            the cause
     */
    public MaterializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
