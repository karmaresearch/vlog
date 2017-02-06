#ifndef _RULEEXECPLAN_H
#define _RULEEXECPLAN_H

#include <vlog/concepts.h>
#include <vector>

struct RuleExecutionPlan {

    std::vector<const Literal*> plan;
    std::vector<std::pair<size_t, size_t>> ranges;

    //This variable tells whether the last literal shares some values with the
    //head. This allows us to group the input to avoid duplicates.
    bool lastLiteralSharesWithHead;
    std::vector<uint8_t> lastSorting; //If the previous var is set, this
    //container tells which variables we should sort on.

    //These three variables record whether we can use the values of the last
    //IDB literal to collect tuples to filter out duplicates. If the last literal
    //is more generic than the head, then the position of the constants allow us to
    //filter it
    bool lastLiteralSubsumesHead;
    std::vector<Term_t> lastLiteralValueConstsInHead;
    std::vector<uint8_t> lastLiteralPosConstsInHead;

    //This variable is used to check whether we can filter out entries in the hashmap.
    //It is used if the rule might trigger derivation that is equal to the last literal
    bool filterLastHashMap;

    //Check if we can apply filtering HashMap. See comment above
    void checkIfFilteringHashMapIsPossible(const Literal &head);

    //The two functions above were written for a full materialization. As TODO
    //I need to remove them and replace them with the datastructurs below. They
    //do the roughly the same thing
    struct MatchVariables {
        uint8_t posLiteralInOrder;
        std::vector<std::pair<uint8_t, uint8_t>> matches;
    };
    std::vector<MatchVariables> matches;

    //When I execute the joins, the following variables contain the size of the
    //intermediate tuples,
    //and all the positions to join and copy the results
    std::vector<uint8_t> sizeOutputRelation;
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> joinCoordinates;
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> posFromFirst;
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> posFromSecond;

    void calculateJoinsCoordinates(const Literal &headLiteral);

    RuleExecutionPlan reorder(std::vector<uint8_t> &order,
                              const Literal &headLiteral) const;

    bool hasCartesian();
};

#endif
