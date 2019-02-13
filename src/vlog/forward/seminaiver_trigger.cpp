#include <vlog/seminaiver_trigger.h>
#include <vlog/trigger/tgpath.h>

#include <unordered_map>

void TriggerSemiNaiver::run(std::string trigger_paths) {

    //Create all the execution plans, etc.
    std::vector<Rule> ruleset = program->getAllRules();
    std::vector<RuleExecutionDetails> allrules;
    int ruleid = 0;
    for (std::vector<Rule>::iterator itr = ruleset.begin(); itr != ruleset.end();
            ++itr) {
        allrules.push_back(RuleExecutionDetails(*itr, ruleid++));
    }

    //Do not check for duplicates anymore
    //setIgnoreDuplicatesElimination();

    LOG(DEBUGL) << "First load the trigger_path file";
    TGPaths paths(trigger_paths);
    LOG(DEBUGL) << "There are " << paths.getNPaths() << " paths to execute";

    size_t iteration = 0;
    std::unordered_map<std::string, size_t> iterations;
    std::vector<StatIteration> costRules;

    for(size_t i = 0; i < paths.getNPaths(); ++i) {
        LOG(DEBUGL) << "Executing path " << i;
        const TGPath &path = paths.getPath(i);
        //LOG(DEBUGL) << "Rule " << path.ruleid << " inputs " << path.inputs.size() << " output " << path.output;

        //Set up the inputs
        auto &ruleDetails = allrules[path.ruleid];

        //Create range vector corresponding to the inputs
        std::vector<std::pair<size_t, size_t>> ranges;
        for(auto &input : path.inputs) {
            if (input == "INPUT" || input.find("EDB") == 0) {
                ranges.push_back(std::make_pair(0, (size_t) - 1));
            } else {
                //Get the range from the map
                if (!iterations.count(input)) {
                    LOG(ERRORL) << "This should not happen! " << input << " never found before";
                    throw 10;
                }
                size_t it = iterations[input];
                ranges.push_back(std::make_pair(it, it));
            }
        }

        ruleDetails.createExecutionPlans(ranges, false);

        //Invoke the execution of the rule using the inputs specified
        std::chrono::system_clock::time_point start =
            std::chrono::system_clock::now();
        executeRule(ruleDetails, iteration, 0, NULL);
        std::chrono::duration<double> sec = std::chrono::system_clock::now()
            - start;
        StatIteration stat;
        stat.iteration = i;
        stat.rule = &ruleDetails.rule;
        stat.time = sec.count() * 1000;
        stat.derived = false;
        costRules.push_back(stat);

        iterations.insert(std::make_pair(path.output, iteration));
        iteration += 1;
    }
    LOG(INFOL) << "Triggers: " << triggers;

#ifdef DEBUG
    std::sort(costRules.begin(), costRules.end());
    int i = 0;
    double sum = 0;
    double sum10 = 0;
    for (auto &el : costRules) {
        LOG(DEBUGL) << "Cost iteration " << el.iteration << " " <<
            el.time;
        i++;
        if (i >= 20)
            break;

        sum += el.time;
        if (i <= 10)
            sum10 += el.time;
    }
    LOG(DEBUGL) << "Sum first 20 rules: " << sum
        << " first 10:" << sum10;
#endif

}
