package karmaresearch.vlog;

import java.util.Arrays;

/**
 * This class represents rules, as used in VLog.
 */
public class Rule {

    private final Atom[] head;
    private final Atom[] body;

    /**
     * Constructs a <code>Rule</code> object with the specified head and body.
     *
     * @param head
     *            the rule head
     * @param body
     *            the rule body
     */
    public Rule(Atom[] head, Atom[] body) {
        if (head == null || body == null) {
            throw new IllegalArgumentException(
                    "null argument to Rule constructor");
        }
        for (Atom h : head) {
            h.checkNoBlank();
        }
        for (Atom h : body) {
            h.checkNoBlank();
        }
        // Clone, so we don't keep references to user-arrays.
        this.head = head.clone();
        this.body = body.clone();
    }

    /**
     * Returns the atoms of the head.
     *
     * @return the head
     */
    public Atom[] getHead() {
        // Clone, so that we don't give access to internal arrays.
        return head.clone();
    }

    /**
     * Returns the atoms of the body.
     *
     * @return the body.
     */
    public Atom[] getBody() {
        // Clone, so that we don't give access to internal arrays.
        return body.clone();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(head) + Arrays.hashCode(body);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Rule)) {
            return false;
        }
        Rule rule = (Rule) o;
        return Arrays.equals(head, rule.head) && Arrays.equals(body, rule.body);
    }

    @Override
    public String toString() {
        return "HEAD: " + Arrays.toString(head) + ", BODY: "
                + Arrays.toString(body);
    }
}
