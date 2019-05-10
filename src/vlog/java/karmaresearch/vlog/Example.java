package karmaresearch.vlog;

import karmaresearch.vlog.Term.TermType;
import karmaresearch.vlog.VLog.LogLevel;
import karmaresearch.vlog.VLog.RuleRewriteStrategy;

public class Example {

    private static String edbConf0 = "EDB0_predname=TE\n"
            + "EDB0_type=INMEMORY\n" + "EDB0_param0=/Users/ceriel/Downloads\n"
            + "EDB0_param1=iswc-2017-complete\n";

    // Gets a country from an affiliation. Note that the parameter "a" should be
    // bound.
    private static String edbConf1 = "EDB1_predname=InCountry\n"
            + "EDB1_type=SPARQL\n"
            + "EDB1_param0=http://query.wikidata.org/sparql\n"
            + "EDB1_param1=a,bLabel\n" + "EDB1_param2=SERVICE wikibase:mwapi {"
            + "  bd:serviceParam wikibase:api \"EntitySearch\" ."
            + "  bd:serviceParam wikibase:endpoint \"www.wikidata.org\" ."
            + "  bd:serviceParam mwapi:search ?a ."
            + "  bd:serviceParam mwapi:language \"en\" ."
            + "  ?c wikibase:apiOutputItem mwapi:item ." + " }"
            + " ?c wdt:P31/wdt:P279* wd:Q43229 ." + " ?c wdt:P17 ?b ."
            + " SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". }\n";

    public Term varA = new Term(TermType.VARIABLE, "A");
    public Term varB = new Term(TermType.VARIABLE, "B");
    public Term varX = new Term(TermType.VARIABLE, "X");
    public Term varC = new Term(TermType.VARIABLE, "C");
    public Term varD = new Term(TermType.VARIABLE, "D");
    public Term constCreator = new Term(TermType.CONSTANT,
            "<http://purl.org/dc/elements/1.1/creator>");
    public Term constHasAffiliation = new Term(TermType.CONSTANT,
            "<https://w3id.org/scholarlydata/ontology/conference-ontology.owl#hasAffiliation>");
    public Term constWithOrganization = new Term(TermType.CONSTANT,
            "<https://w3id.org/scholarlydata/ontology/conference-ontology.owl#withOrganisation>");
    public Term constName = new Term(TermType.CONSTANT,
            "<https://w3id.org/scholarlydata/ontology/conference-ontology.owl#name>");

    public Atom creatorXA = new Atom("TE", varX, constCreator, varA);
    public Atom creatorXB = new Atom("TE", varX, constCreator, varB);
    public Atom hasAffBX = new Atom("TE", varB, constHasAffiliation, varX);
    public Atom withOrgXA = new Atom("TE", varX, constWithOrganization, varA);
    public Atom nameAX = new Atom("TE", varA, constName, varX);
    public Atom pwtAB = new Atom("PWT", varA, varB);
    public Atom aoAB = new Atom("AO", varA, varB);
    public Atom afXB = new Atom("AF", varX, varB);

    public Rule[] rules = new Rule[] {
            // Rule: two creators of the same work work together.
            new Rule(new Atom[] { pwtAB }, new Atom[] { creatorXA, creatorXB }),
            // Rule: determines an organization from a creator
            // Note: there is no direct connection in the database.
            new Rule(new Atom[] { aoAB }, new Atom[] { hasAffBX, withOrgXA }),
            // Rule that determines the name of an organization
            new Rule(new Atom[] { afXB }, new Atom[] { aoAB, nameAX }),
            // Rule that determines which organizations work together.
            new Rule(new Atom[] { new Atom("AWT", varA, varB) },
                    new Atom[] { new Atom("PWT", varC, varD),
                            new Atom("AF", varA, varC),
                            new Atom("AF", varB, varD) }),
            // Rules extracting organizations.
            new Rule(new Atom[] { new Atom("A", varX) },
                    new Atom[] { new Atom("AWT", varX, varA) }),
            new Rule(new Atom[] { new Atom("A", varX) },
                    new Atom[] { new Atom("AWT", varA, varX) }),
            // Countries A and B working together: institutes C and D working
            // together, C is from A, D is from B.
            new Rule(new Atom[] { new Atom("CWT", varA, varB) },
                    new Atom[] { new Atom("AWT", varC, varD),
                            new Atom("InCountry", varC, varA),
                            new Atom("InCountry", varD, varB) }) };

    public void run() throws Exception {
        VLog vlog = new VLog();
        vlog.setLogLevel(LogLevel.INFO);
        vlog.start(edbConf0 + edbConf1, false);
        vlog.setRules(rules, RuleRewriteStrategy.NONE);
        vlog.materialize(false);

        // Print affiliations working together
        try (TermQueryResultIterator iter1 = vlog
                .query(new Atom("AWT", varA, varB))) {
            if (iter1.hasNext()) {
                while (iter1.hasNext()) {
                    Term[] terms = iter1.next();
                    System.out.println(
                            "Found affiliation pair " + terms[0].getName()
                                    + " and " + terms[1].getName());
                }
            } else {
                System.out.println("No pairs found");
            }
        }

        // Print countries working together.
        try (TermQueryResultIterator iter1 = vlog
                .query(new Atom("CWT", varA, varB))) {
            if (iter1.hasNext()) {
                while (iter1.hasNext()) {
                    Term[] terms = iter1.next();
                    System.out
                            .println("Found country pair " + terms[0].getName()
                                    + " and " + terms[1].getName());
                }
            } else {
                System.out.println("No pairs found");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Example().run();
    }
}
