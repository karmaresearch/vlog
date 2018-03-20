package karmaresearch.vlog;

/**
 * Represents a single term of an atom.
 */
public class Term {

    public enum TermType {
        CONSTANT, VARIABLE
    };

    private final TermType termType;

    private final String name;

    /**
     * Constructs a <code>Term</code> object with the specified parameters.
     *
     * @param termType
     *            the type
     * @param name
     *            the name of the variable, or the string representation of the
     *            constant.
     */
    public Term(TermType termType, String name) {
        if (name == null || termType == null) {
            throw new IllegalArgumentException(
                    "null argument to Term constructor");
        }
        this.termType = termType;
        this.name = name;
    }

    /**
     * Returns the type: variable or constant.
     *
     * @return the type.
     */
    public TermType getTermType() {
        return termType;
    }

    /**
     * Returns the name of the variable, or the string representation of the
     * constant.
     *
     * @return the name.
     */
    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Term)) {
            return false;
        }
        Term v = (Term) o;
        return v.name.equals(name) && v.termType == termType;
    }
}