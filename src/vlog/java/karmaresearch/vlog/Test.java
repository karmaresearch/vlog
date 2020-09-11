package karmaresearch.vlog;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import karmaresearch.vlog.Term.TermType;
import karmaresearch.vlog.VLog.LogLevel;
import karmaresearch.vlog.VLog.RuleRewriteStrategy;

class Test {

    private static String edbConf = "EDB0_predname=FatherOf\n"
            + "EDB0_type=SPARQL\n"
            + "EDB0_param0=http://query.wikidata.org/sparql\n"
            + "EDB0_param1=b,a\n" + "EDB0_param2=?a p:P22 ?b\n"
            + "EDB1_predname=MotherOf\n" + "EDB1_type=SPARQL\n"
            + "EDB1_param0=http://query.wikidata.org/sparql\n"
            + "EDB1_param1=b,a\n" + "EDB1_param2=?a p:P25 ?b\n";
    private static String edbConf1 = "EDB0_predname=lighthouse\n"
            + "EDB0_type=SPARQL\n"
            + "EDB0_param0=http://query.wikidata.org/sparql\n"
            + "EDB0_param1=subject,subjectLabel\n" + "EDB0_param2=?subject wdt:P31 wd:Q39715; rdfs:label ?subjectLabel\n";
    private static Atom isMotherOf = new Atom("MotherOf",
            new Term(TermType.VARIABLE, "a"), new Term(TermType.VARIABLE, "b"));
    private static Atom isFatherOf = new Atom("FatherOf",
            new Term(TermType.VARIABLE, "a"), new Term(TermType.VARIABLE, "b"));
    private static Atom isParentOf = new Atom("ParentOf",
            new Term(TermType.VARIABLE, "a"), new Term(TermType.VARIABLE, "b"));
    private static Atom isParent = new Atom("Parent",
            new Term(TermType.VARIABLE, "a"));

    private static Rule RuleParentOf1 = new Rule(new Atom[] { isParentOf },
            new Atom[] { isFatherOf });
    private static Rule RuleParentOf2 = new Rule(new Atom[] { isParentOf },
            new Atom[] { isMotherOf });
    private static Rule RuleParent = new Rule(new Atom[] { isParent },
            new Atom[] { isParentOf });

    private static Rule[] parentRules = new Rule[] { RuleParentOf1,
            RuleParentOf2, RuleParent };

    private static String[][] p1_contents = { { "a", "b" } };
    private static String[][] p2_contents = { { "c", "d", "e" },
            { "f", "g", "h" } };

    public static void testEmpty() throws Exception {
        VLog vlog = new VLog();
        vlog.setLogLevel(LogLevel.INFO);
        vlog.start("", false);
        try {
            try (TermQueryResultIterator e = vlog.query(
                    new Atom("A", new Term(Term.TermType.VARIABLE, "C")))) {
                System.err.println("vlog should have thrown an exception");
            }
        } catch (NonExistingPredicateException e) {
            // OK
        }
    }

    public static void testlanguagetag() throws Exception {
        VLog vlog = new VLog();
        vlog.setLogLevel(LogLevel.INFO);
        vlog.start(edbConf1, false);
        try (TermQueryResultIterator e = vlog.query(
                new Atom("lighthouse", new Term(Term.TermType.VARIABLE, "A"), new Term(TermType.VARIABLE, "B")))) {
            while (e.hasNext()) {
                Term[] t = e.next();
                System.out.println(Arrays.toString(t));
            }
        }
    }

    public static void testNonExisting() throws Exception {
        String edbConf = "EDB0_predname=TE\nEDB0_type=Trident\nEDB0_param0=nonExisting\n";
        VLog vlog = new VLog();
        vlog.setLogLevel(LogLevel.INFO);
        try {
            vlog.start(edbConf, false);
        } catch(IOException e) {
            // OK
            return;
        }
        System.err.println("vlog should have thrown an exception on non-existing trident db");
    }

    static void runTest() throws Exception {
        testEmpty();
        // testlanguagetag();
        testNonExisting();
        VLog vlog = new VLog();
        vlog.setLogLevel(LogLevel.INFO);
        try {
            vlog.start("blabla", false);
            System.err.println("vlog.start() should have thrown an exception.");
        } catch (EDBConfigurationException e) {
            // Good!
        }

        vlog = new VLog();
        // Test start with empty config
        vlog.start("", false);
        vlog.addData("p1", p1_contents);
        vlog.addData("p2", p2_contents);
        System.out.println("Arity of p1 = " + vlog.getPredicateArity("p1")
                + ", arity of p2 = " + vlog.getPredicateArity("p2"));
        System.out.println("Arity of non-existing predicate "
                + vlog.getPredicateArity("blabla"));
        List<Term> q1 = new ArrayList<Term>();
        q1.add(new Term(TermType.VARIABLE, "v1"));
        q1.add(new Term(TermType.CONSTANT, "d"));
        q1.add(new Term(TermType.VARIABLE, "v2"));
        vlog.writeQueryResultsToCsv(
                new Atom("p2", q1.toArray(new Term[q1.size()])), "blabla");
        byte[] readBack = Files.readAllBytes(Paths.get("blabla"));
        if (readBack.length > 7) {
            throw new Error(
                    "Error in query, check file 'blabla', should contain 'c,d,e'");
        }
        byte[] b2 = Arrays.copyOf(readBack, 5);
        char[] c = "c,d,e".toCharArray();
        byte[] b1 = Charset.forName("UTF-8").encode(CharBuffer.wrap(c)).array();
        if (!Arrays.equals(b1, b2)) {
            throw new Error(
                    "Error in query, check file 'blabla', should contain 'c,d,e'");
        }
        try {
            vlog.writeQueryResultsToCsv(
                    new Atom("p3", q1.toArray(new Term[q1.size()])), "blabla");
            System.err.println("vlog should have thrown an exception");
        } catch (NonExistingPredicateException e) {
            // OK
        }
        Files.delete(Paths.get("blabla"));

        vlog = new VLog();
        vlog.start("", false);
        vlog.addData("A", new String[][] { { "a" } });
        vlog.addData("A", new String[][] { { "a" }, { "b" } });
        vlog.addData("X", new String[][] { { "d", "e" }, { "c", "c" } });
        try (TermQueryResultIterator e = vlog
                .query(new Atom("A", new Term(Term.TermType.CONSTANT, "C")))) {
            if (e.hasNext()) {
                throw new Error("Error in query");
            }
            System.out.println("Trying EDB query without constants ...");
        }
        try (TermQueryResultIterator e = vlog.query(
                new Atom("A", new Term(Term.TermType.CONSTANT, "a")), false,
                false)) {
            if (!e.hasNext()) {
                throw new Error("Error in query");
            }
            Term[] v = e.next();
            if (v.length > 0) {
                throw new Error("Error in query");
            }
            if (e.hasNext()) {
                throw new Error("Error in query");
            }
        }
        System.out.println("Trying same EDB query with constants ...");
        try (TermQueryResultIterator e = vlog.query(
                new Atom("A", new Term(Term.TermType.CONSTANT, "a")), true,
                false)) {
            if (!e.hasNext()) {
                throw new Error("Error in query");
            }
            Term[] v = e.next();
            if (v.length != 1) {
                throw new Error("Error in query");
            }
            if (!v[0].getName().equals("a")) {
                throw new Error("Error in query");
            }
            if (e.hasNext()) {
                throw new Error("Error in query");
            }
        }
        System.out.println("Trying EDB query with variable ...");
        try (TermQueryResultIterator e = vlog
                .query(new Atom("A", new Term(Term.TermType.VARIABLE, "C")))) {
            while (e.hasNext()) {
                Term[] result = e.next();
                System.out.println("result: " + Arrays.toString(result));
            }
        }
        ArrayList<Rule> rules = new ArrayList<>();
        rules.add(new Rule(
                new Atom[] {
                        new Atom("B", new Term(Term.TermType.VARIABLE, "C")) },
                new Atom[] { new Atom("A",
                        new Term(Term.TermType.VARIABLE, "C")) }));
        rules.add(
                new Rule(
                        new Atom[] { new Atom("C",
                                new Term(Term.TermType.VARIABLE, "C"),
                                new Term(Term.TermType.VARIABLE, "D")) },
                        new Atom[] { new Atom("X",
                                new Term(Term.TermType.VARIABLE, "C"), new Term(
                                        Term.TermType.VARIABLE, "D")) }));
        vlog.setRules(rules.toArray(new Rule[rules.size()]),
                RuleRewriteStrategy.AGGRESSIVE);
        vlog.materialize(false);

        System.out.println("Running JNI interface of cyclic-check");
        if (vlog.checkCyclic("JA") != VLog.CyclicCheckResult.NON_CYCLIC) {
            throw new Error("Error in cyclic-check");
        }
        try (TermQueryResultIterator e = vlog
                .query(new Atom("B", new Term(Term.TermType.CONSTANT, "C")))) {
            if (e.hasNext()) {
                throw new Error("Error in query");
            }
        }
        System.out.println("Trying IDB query without constants ...");
        try (TermQueryResultIterator e = vlog.query(
                new Atom("B", new Term(Term.TermType.CONSTANT, "a")), false,
                false)) {
            if (!e.hasNext()) {
                throw new Error("Error in query");
            }
            Term[] v = e.next();
            if (v.length > 0) {
                throw new Error("Error in query");
            }
            if (e.hasNext()) {
                throw new Error("Error in query");
            }
        }
        System.out.println("Trying same IDB query with constants ...");
        try (TermQueryResultIterator e = vlog.query(
                new Atom("B", new Term(Term.TermType.CONSTANT, "a")), true,
                false)) {
            if (!e.hasNext()) {
                throw new Error("Error in query");
            }
            Term[] v = e.next();
            if (v.length != 1) {
                throw new Error("Error in query");
            }
            if (!v[0].getName().equals("a")) {
                throw new Error("Error in query");
            }
            if (e.hasNext()) {
                throw new Error("Error in query");
            }
        }
        System.out.println("Trying EDB query with same variable ...");
        try (TermQueryResultIterator e = vlog
                .query(new Atom("X", new Term(Term.TermType.VARIABLE, "x"),
                        new Term(Term.TermType.VARIABLE, "x")), true, false)) {
            if (!e.hasNext()) {
                throw new Error("Error in query");
            }
            Term[] v = e.next();
            System.out.println("Terms: " + Arrays.toString(v));

            if (!v[0].getName().equals("c")) {
                throw new Error("Error in query");
            }
            if (!v[1].getName().equals("c")) {
                throw new Error("Error in query");
            }
            if (e.hasNext()) {
                throw new Error("Error in query");
            }
        }
        System.out.println("Trying IDB query with same variable ...");
        try (TermQueryResultIterator e = vlog
                .query(new Atom("C", new Term(Term.TermType.VARIABLE, "x"),
                        new Term(Term.TermType.VARIABLE, "x")), true, false)) {
            if (!e.hasNext()) {
                throw new Error("Error in query");
            }
            Term[] v = e.next();
            System.out.println("Terms: " + Arrays.toString(v));

            if (!v[0].getName().equals("c")) {
                throw new Error("Error in query");
            }
            if (!v[1].getName().equals("c")) {
                throw new Error("Error in query");
            }
            if (e.hasNext()) {
                throw new Error("Error in query");
            }
        }

        vlog = new VLog();
        vlog.start(edbConf, false);

        vlog.setRules(parentRules, RuleRewriteStrategy.AGGRESSIVE);
        if (!vlog.materialize(false, 100)) {
            System.out.println("Timeout ... try again without ...");
            vlog.materialize(false);
        }

        vlog.writeQueryResultsToCsv(isParentOf, "isParentOf");
        vlog.writeQueryResultsToCsv(isParent, "isParent");
    }

    public static void main(String[] args) throws Exception {
        runTest();
    }

}
