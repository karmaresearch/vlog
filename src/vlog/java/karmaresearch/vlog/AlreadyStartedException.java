package karmaresearch.vlog;

/**
 * This exception gets thrown when an attempt is made to start VLog when it is
 * already started.
 */

public class AlreadyStartedException extends Exception {

    private static final long serialVersionUID = 1660661300435414890L;

    /**
     * Constructs an <code>AlreadyStartedException</code> with <code>null</code>
     * as its error detail message.
     */
    public AlreadyStartedException() {
        super();
    }

    /**
     * Constructs an <code>AlreadyStartedException</code> with the specified
     * detail message.
     *
     * @param message
     *            the detail message
     */
    public AlreadyStartedException(String message) {
        super(message);
    }

    /**
     * Constructs an <code>AlreadyStartedException</code> with the specified
     * cause.
     *
     * @param cause
     *            the cause
     */
    public AlreadyStartedException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an <code>AlreadyStartedException</code> with the specified
     * detail message and cause.
     *
     * @param message
     *            the detail message
     * @param cause
     *            the cause
     */
    public AlreadyStartedException(String message, Throwable cause) {
        super(message, cause);
    }
}
