package karmaresearch.vlog;

import java.util.Arrays;

/**
 * Represents a single atom of a rule, or a query.
 */
public class Atom {
    private final String predicate;
    private final Term[] terms;

    /**
     * Constructs an <code>Atom</code> from the specified parameters.
     *
     * @param predicate
     *            the predicate
     * @param terms
     *            the terms
     */
    public Atom(String predicate, Term... terms) {
        if (predicate == null || terms == null) {
            throw new IllegalArgumentException(
                    "null argument to Atom constructor");
        }
        for (Term t : terms) {
            if (t == null) {
                throw new IllegalArgumentException(
                        "null term in Atom constructor");
            }
        }

        this.predicate = predicate;
        this.terms = terms.clone();
    }

    /**
     * Returns the predicate of this atom.
     *
     * @return the predicate
     */
    public String getPredicate() {
        return predicate;
    }

    /**
     * Returns the terms of this atom.
     *
     * @return the terms.
     */
    public Term[] getTerms() {
        return terms.clone();
    }

    @Override
    public int hashCode() {
        return predicate.hashCode() + Arrays.hashCode(terms);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Atom)) {
            return false;
        }
        Atom atom = (Atom) o;
        return predicate.equals(atom.predicate)
                && Arrays.equals(terms, atom.terms);
    }
}