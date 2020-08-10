#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <vlog/concepts.h>

#include <vector>

class Optimizer {

    private:
        static std::vector<const Literal*> calculateBestPlan(
                std::vector<const Literal*> &existingPlan,
                std::vector<Var_t> boundVars, std::vector<Var_t> &existingVars,
                std::vector<const Literal*> &remainingLiterals);
    public:
        static std::vector<const Literal*> rearrangeBodyAfterAdornment(
                std::vector<Var_t> &boundVars, const std::vector<Literal> &body);
};

#endif
