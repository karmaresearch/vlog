#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <vlog/concepts.h>

#include <vector>

class Optimizer {

private:
    static std::vector<const Literal*> calculateBestPlan(
        std::vector<const Literal*> &existingPlan,
        std::vector<uint8_t> boundVars, std::vector<uint8_t> &existingVars,
        std::vector<const Literal*> &remainingLiterals);
public:
    static std::vector<const Literal*> rearrangeBodyAfterAdornment(
        std::vector<uint8_t> &boundVars, const std::vector<Literal> &body);
};

#endif
