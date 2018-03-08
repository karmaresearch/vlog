package karmaresearch.vlog;

import java.util.ArrayList;
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

    private static Atom prescription1 = new Atom("prescription",
            new Term[] { ID, PATIENT, NPI, C1 });
    private static Atom treatment1 = new Atom("treatment",
            new Term[] { ID, PATIENT, HOSPITAL, NPI, CONF1 });
    private static Atom physician1 = new Atom("physician",
            new Term[] { NPI, NAME, SPEC, CONF2 });
    private static Atom medprescription = new Atom("medprescription",
            new Term[] { ID, PATIENT, NPI, DOCTOR, SPEC, CONF });
    private static Atom doctor1 = new Atom("doctor",
            new Term[] { NPI, DOCTOR, SPEC, H, C2 });
    private static Atom doctor2 = new Atom("doctor",
            new Term[] { NPI, DOCTOR, SPEC, HOSPITAL, C2 });
    private static Atom targethospital = new Atom("targethospital",
            new Term[] { DOCTOR, SPEC, HOSPITAL1, NPI1, HCONF1 });
    private static Atom hospital = new Atom("hospital",
            new Term[] { DOCTOR, SPEC, HOSPITAL1, NPI1, HCONF1 });

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

    static void runTest(String fn) throws Exception {
        VLog vlog = new VLog();
        vlog.setLogLevel("debug");
        try {
            vlog.start("blabla", false);
            System.err.println("vlog.start() should have thrown an exception.");
        } catch (EDBConfigurationException e) {
            // Good!
        }
        vlog.stop();
        vlog.start(fn, true);
        ArrayList<Rule> rules = new ArrayList<>();
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

        StringQueryResultEnumeration result = vlog.query(query);
        while (result.hasMoreElements()) {
            String[] r = result.nextElement();
            for (String s : r) {
                System.out.print(s + " ");
            }
            System.out.println();
        }
        result.cleanup();

        rules.clear();
        rules.add(rule1);
        rules.add(rule2);
        rules.add(rule3);
        rules.add(rule4);
        rules.add(rule5);

        vlog.setRules(rules.toArray(new Rule[rules.size()]),
                RuleRewriteStrategy.AGGRESSIVE);
        vlog.materialize(false);
        result = vlog.query(query);
        while (result.hasMoreElements()) {
            String[] r = result.nextElement();
            for (String s : r) {
                System.out.print(s + " ");
            }
            System.out.println();
        }
        vlog.writePredicateToCsv("prescription", "testOutput");
        result.cleanup();

    }

}
