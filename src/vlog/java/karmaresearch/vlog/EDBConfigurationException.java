package karmaresearch.vlog;

/**
 * This exception gets thrown when an error is found in the EDB configuration.
 */
public class EDBConfigurationException extends Exception {

    /**
     * Constructs an <code>EDBConfigurationException</code> with
     * <code>null</code> as its error detail message.
     */
    public EDBConfigurationException() {
        super();
    }

    /**
     * Constructs an <code>EDBConfigurationException</code> with the specified
     * detail message and cause.
     *
     * @param message
     *            the detail message
     * @param cause
     *            the cause
     */
    public EDBConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an <code>EDBConfigurationException</code> with the specified
     * detail message.
     *
     * @param message
     *            the detail message
     */
    public EDBConfigurationException(String message) {
        super(message);
    }

    /**
     * Constructs an <code>EDBConfigurationException</code> with the specified
     * cause.
     *
     * @param cause
     *            the cause
     */
    public EDBConfigurationException(Throwable cause) {
        super(cause);
    }

}
