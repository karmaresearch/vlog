package karmaresearch.vlog;

import java.util.Arrays;

/**
 * Represents a single atom of a rule, or a query.
 */
public class Atom {
    private final String predicate;
    private final Term[] terms;
    private final boolean negated;

    /**
     * Constructs an <code>Atom</code> from the specified parameters.
     *
     * @param predicate
     *            the predicate
     * @param terms
     *            the terms
     */
    public Atom(String predicate, Term... terms) {
        this(predicate, false, terms);
    }

    /**
     * Constructs an <code>Atom</code> from the specified parameters.
     *
     * @param predicate
     *            the predicate
     * @param negated
     *            mostly ignored, except in the body of rules, where it
     *            indicates that the negated variant is intended
     * @param terms
     *            the terms
     */
    public Atom(String predicate, boolean negated, Term... terms) {
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
        this.negated = negated;
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
     * Returns whether this atom is negated.
     * 
     * @return whether this atom is negated.
     */
    public boolean isNegated() {
        return negated;
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
        final int prime = 31;
        int result = 1;
        result = prime * result + (negated ? 1231 : 1237);
        result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
        result = prime * result + Arrays.hashCode(terms);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Atom other = (Atom) obj;
        if (negated != other.negated)
            return false;
        if (predicate == null) {
            if (other.predicate != null)
                return false;
        } else if (!predicate.equals(other.predicate))
            return false;
        if (!Arrays.equals(terms, other.terms))
            return false;
        return true;
    }

    /**
     * Checks that the Atom does not contain a blank, and throws an
     * IllegalArgumentException if it does.
     */
    public void checkNoBlank() {
        for (Term t : terms) {
            if (t.getTermType() == Term.TermType.BLANK) {
                throw new IllegalArgumentException("Blank not allowed here");
            }
        }
    }

    @Override
    public String toString() {
        String ret = "";
        if (negated) {
            ret = "~";
        }
        ret += predicate + Arrays.toString(terms);
        return ret;
    }

}
