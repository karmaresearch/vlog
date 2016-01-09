/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Vlog.

   Vlog is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Vlog is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vlog.  If not, see <http://www.gnu.org/licenses/>.
*/

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
