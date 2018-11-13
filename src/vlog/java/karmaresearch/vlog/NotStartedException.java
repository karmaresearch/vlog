package karmaresearch.vlog;

/**
 * This exception gets thrown when an attempt is made to do something with VLog
 * when it is not started yet.
 */

public class NotStartedException extends Exception {

    private static final long serialVersionUID = 877780226511090797L;

    /**
     * Constructs an <code>NotStartedException</code> with <code>null</code> as
     * its error detail message.
     */
    public NotStartedException() {
        super();
    }

    /**
     * Constructs an <code>NotStartedException</code> with the specified detail
     * message.
     *
     * @param message
     *            the detail message
     */
    public NotStartedException(String message) {
        super(message);
    }

    /**
     * Constructs an <code>NotStartedException</code> with the specified cause.
     *
     * @param cause
     *            the cause
     */
    public NotStartedException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an <code>NotStartedException</code> with the specified detail
     * message and cause.
     *
     * @param message
     *            the detail message
     * @param cause
     *            the cause
     */
    public NotStartedException(String message, Throwable cause) {
        super(message, cause);
    }
}
