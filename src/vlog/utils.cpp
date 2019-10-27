#include <vlog/utils.h>
#include <string>

#include <launcher/vloglayer.h>
#include <cts/parser/SPARQLLexer.hpp>
#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/plangen/PlanGen.hpp>
#include <cts/codegen/CodeGen.hpp>
#include <rts/runtime/Runtime.hpp>
#include <rts/runtime/QueryDict.hpp>
#include <rts/operator/Operator.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <rts/operator/ResultsPrinter.hpp>

// Utility to convert a string to escaped, as an entry for a CSV file.
std::string VLogUtils::csvString(std::string s) {
    auto pos = s.find_first_of(" \",\n\t\r");
    if (pos == std::string::npos) {
        return s;
    }
    // Now, we need to escape the string, which means quoting, and doubling every quote in the string.
    pos = s.find_first_of("\"");
    if (pos == std::string::npos) {
        // Just quoting is good enough.
        return "\"" + s + "\"";
    }
    std::string result = "\"";
    size_t beginpos = 0;
    while (pos != std::string::npos) {
        result += s.substr(beginpos, (pos-beginpos)+1) + "\"";
        beginpos = pos + 1;
        pos = s.find_first_of("\"", beginpos);
    }
    result += s.substr(beginpos, pos) + "\"";
    return result;
}

void VLogUtils::parseQuery(bool &success,
        SPARQLParser &parser,
        std::shared_ptr<QueryGraph> &queryGraph,
        QueryDict &queryDict,
        DBLayer &db) {

    //Sometimes the query introduces new constants which need an ID
    try {
        parser.parse();
    } catch (const SPARQLParser::ParserException& e) {
        cerr << "parse error: " << e.message << endl;
        success = false;
        return;
    }

    queryGraph = std::shared_ptr<QueryGraph>(new QueryGraph(parser.getVarCount()));

    // And perform the semantic anaylsis
    try {
        SemanticAnalysis semana(db, queryDict);
        semana.transform(parser, *queryGraph.get());
    } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        success = false;
        return;
    }
    if (queryGraph->knownEmpty()) {
        cout << "<empty result -- known empty>" << endl;
        success = false;
        return;
    }

    success = true;
    return;
}

void VLogUtils::execSPARQLQuery(std::string sparqlquery,
        bool explain,
        long nterms,
        DBLayer &db,
        bool printstdout,
        bool jsonoutput,
        JSON *jsonvars,
        JSON *jsonresults,
        JSON *jsonstats) {
    std::unique_ptr<QueryDict> queryDict = std::unique_ptr<QueryDict>(new QueryDict(nterms));
    bool parsingOk;

    std::unique_ptr<SPARQLLexer> lexer =
        std::unique_ptr<SPARQLLexer>(new SPARQLLexer(sparqlquery));
    std::unique_ptr<SPARQLParser> parser = std::unique_ptr<SPARQLParser>(
            new SPARQLParser(*lexer.get()));
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::shared_ptr<QueryGraph> queryGraph;
    parseQuery(parsingOk, *parser.get(), queryGraph, *queryDict.get(), db);
    if (!parsingOk) {
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime query: 0ms.";
        LOG(INFOL) << "Runtime total: " << duration.count() * 1000 << "ms.";
        LOG(INFOL) << "# rows = 0";
        return;
    }

    if (jsonvars) {
        //Copy the output of the query in the json vars
        for (QueryGraph::projection_iterator itr = queryGraph->projectionBegin();
                itr != queryGraph->projectionEnd(); ++itr) {
            std::string namevar = parser->getVariableName(*itr);
            JSON var;
            var.put("", namevar);
            jsonvars->push_back(var);
        }
    }

    // Run the optimizer
    PlanGen *plangen = new PlanGen();
    Plan* plan = plangen->translate(db, *queryGraph.get(), false);
    // delete plangen;  Commented out, because this also deletes all plans!
    // In particular, it corrupts the current plan.
    // --Ceriel
    if (!plan) {
        cerr << "internal error plan generation failed" << endl;
        delete plangen;
        return;
    }
    if (explain)
        plan->print(0);

    // Build a physical plan
    Runtime runtime(db, NULL, queryDict.get());
    Operator* operatorTree = CodeGen().translate(runtime, *queryGraph.get(), plan, false);

    // Execute it
    if (explain) {
        DebugPlanPrinter out(runtime, false);
        operatorTree->print(out);
        delete operatorTree;
    } else {
#if DEBUG
        DebugPlanPrinter out(runtime, false);
        operatorTree->print(out);
#endif
        //set up output options for the last operators
        ResultsPrinter *p = (ResultsPrinter*) operatorTree;
        p->setSilent(!printstdout);
        if (jsonoutput) {
            std::vector<std::string> jsonvars;
            p->setJSONOutput(jsonresults, jsonvars);
        }

        std::chrono::system_clock::time_point startQ = std::chrono::system_clock::now();
        if (operatorTree->first()) {
	    LOG(INFOL) << "Found another one";
            while (operatorTree->next()) {
		LOG(INFOL) << "Found another one";
	    }
        }
        std::chrono::duration<double> durationQ = std::chrono::system_clock::now() - startQ;
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime query: " << durationQ.count() * 1000 << "ms.";
        LOG(INFOL) << "Runtime total: " << duration.count() * 1000 << "ms.";
        if (jsonstats) {
            jsonstats->put("runtime", to_string(durationQ.count()));
            jsonstats->put("nresults", to_string(p->getPrintedRows()));

        }
        if (printstdout) {
            long nElements = p->getPrintedRows();
            LOG(INFOL) << "# rows = " << nElements;
        }
        delete operatorTree;
    }
    delete plangen;
}
