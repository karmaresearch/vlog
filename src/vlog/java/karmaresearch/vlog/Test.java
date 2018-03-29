package karmaresearch.vlog;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import karmaresearch.vlog.Term.TermType;
import karmaresearch.vlog.VLog.RuleRewriteStrategy;

class Test {

    // prescription(ID,PATIENT,NPI,C1) :-
    // treatment(ID,PATIENT,HOSPITAL,NPI,CONF1),physician(NPI,NAME,SPEC,CONF2)
    // prescription(ID,PATIENT,NPI,C1) :-
    // medprescription(ID,PATIENT,NPI,DOCTOR,SPEC,CONF)
    // doctor(NPI,DOCTOR,SPEC,H,C2) :-
    // medprescription(ID,PATIENT,NPI,DOCTOR,SPEC,CONF)
    // doctor(NPI,NAME,SPEC,HOSPITAL,C2) :-
    // treatment(ID,PATIENT,HOSPITAL,NPI,CONF1),physician(NPI,NAME,SPEC,CONF2)
    // targethospital(DOCTOR,SPEC,HOSPITAL1,NPI1,HCONF1) :-
    // hospital(DOCTOR,SPEC,HOSPITAL1,NPI1,HCONF1)

    private static Term ID = new Term(TermType.VARIABLE, "ID");
    private static Term PATIENT = new Term(TermType.VARIABLE, "PATIENT");
    private static Term HOSPITAL = new Term(TermType.VARIABLE, "HOSPITAL");
    private static Term NPI = new Term(TermType.VARIABLE, "NPI");
    private static Term C1 = new Term(TermType.VARIABLE, "C1");
    private static Term HCONF1 = new Term(TermType.VARIABLE, "HCONF1");
    private static Term CONF1 = new Term(TermType.VARIABLE, "CONF1");
    private static Term SPEC = new Term(TermType.VARIABLE, "SPEC");
    private static Term CONF2 = new Term(TermType.VARIABLE, "CONF2");
    private static Term CONF = new Term(TermType.VARIABLE, "CONF");
    private static Term DOCTOR = new Term(TermType.VARIABLE, "DOCTOR");
    private static Term H = new Term(TermType.VARIABLE, "H");
    private static Term C2 = new Term(TermType.VARIABLE, "C2");
    private static Term NPI1 = new Term(TermType.VARIABLE, "NPI1");
    private static Term HOSPITAL1 = new Term(TermType.VARIABLE, "HOSPITAL1");
    private static Term NAME = new Term(TermType.VARIABLE, "NAME");

    private static Atom prescription1 = new Atom("prescription", ID, PATIENT,
            NPI, C1);
    private static Atom treatment1 = new Atom("treatment", ID, PATIENT,
            HOSPITAL, NPI, CONF1);
    private static Atom physician1 = new Atom("physician", NPI, NAME, SPEC,
            CONF2);
    private static Atom medprescription = new Atom("medprescription", ID,
            PATIENT, NPI, DOCTOR, SPEC, CONF);
    private static Atom doctor1 = new Atom("doctor", NPI, DOCTOR, SPEC, H, C2);
    private static Atom doctor2 = new Atom("doctor", NPI, DOCTOR, SPEC,
            HOSPITAL, C2);
    private static Atom targethospital = new Atom("targethospital", DOCTOR,
            SPEC, HOSPITAL1, NPI1, HCONF1);
    private static Atom hospital = new Atom("hospital", DOCTOR, SPEC, HOSPITAL1,
            NPI1, HCONF1);

    private static Rule rule1 = new Rule(new Atom[] { prescription1 },
            new Atom[] { treatment1, physician1 });
    private static Rule rule2 = new Rule(new Atom[] { prescription1 },
            new Atom[] { medprescription });
    private static Rule rule3 = new Rule(new Atom[] { doctor1 },
            new Atom[] { medprescription });
    private static Rule rule4 = new Rule(new Atom[] { doctor2 },
            new Atom[] { treatment1, physician1 });
    private static Rule rule5 = new Rule(new Atom[] { targethospital },
            new Atom[] { hospital });

    private static String[][] p1_contents = { { "a", "b" } };
    private static String[][] p2_contents = { { "c", "d", "e" },
            { "f", "g", "h" } };

    public static void testEmpty() throws Exception {
        VLog vlog = new VLog();
        vlog.setLogLevel("info");
        vlog.start("", false);
        vlog.writeQueryResultsToCsv(
                new Atom("p2", new Term(TermType.VARIABLE, "v1")), "blabla");
        ArrayList<Rule> rules = new ArrayList<>();
        rules.add(rule1);
        vlog.setRules(rules.toArray(new Rule[rules.size()]),
                RuleRewriteStrategy.AGGRESSIVE);
        vlog.materialize(false);
        vlog.writeQueryResultsToCsv(
                new Atom("p2", new Term(TermType.VARIABLE, "v1")), "blabla");
    }

    static void runTest(String fn) throws Exception {
        testEmpty();
        VLog vlog = new VLog();
        vlog.setLogLevel("info");
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
        vlog.writeQueryResultsToCsv(
                new Atom("p3", q1.toArray(new Term[q1.size()])), "blabla");
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
        vlog.start(fn, true);
        rules = new ArrayList<>();
        rules.add(rule1);

        vlog.setRules(rules.toArray(new Rule[rules.size()]),
                RuleRewriteStrategy.AGGRESSIVE);
        vlog.materialize(false);

        List<Term> q = new ArrayList<Term>();
        q.add(new Term(TermType.VARIABLE, "?ID"));
        q.add(new Term(TermType.VARIABLE, "?PATIENT"));
        q.add(new Term(TermType.CONSTANT, "186"));
        q.add(new Term(TermType.VARIABLE, "?C1"));
        Atom query = new Atom("prescription", q.toArray(new Term[q.size()]));

        try (TermQueryResultIterator result = vlog.query(query)) {
            while (result.hasNext()) {
                Term[] r = result.next();
                for (Term s : r) {
                    System.out.print(s + " ");
                }
                System.out.println();
            }
        }

        vlog.writeQueryResultsToCsv(query, "queryResult");
        vlog.writeQueryResultsToCsv(query, "filteredQueryResult", true);

        rules.clear();
        rules.add(rule1);
        rules.add(rule2);
        rules.add(rule3);
        rules.add(rule4);
        rules.add(rule5);

        vlog.setRules(rules.toArray(new Rule[rules.size()]),
                RuleRewriteStrategy.AGGRESSIVE);
        vlog.materialize(false);
        try (TermQueryResultIterator result = vlog.query(query, true, true)) {
            while (result.hasNext()) {
                Term[] r = result.next();
                for (Term s : r) {
                    System.out.print(s + " ");
                }
                System.out.println();
            }
            vlog.writePredicateToCsv("prescription", "testOutput");
        }
    }

    public static void main(String[] args) throws Exception {
        runTest(args[0]);
    }

}
