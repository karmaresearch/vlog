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

#ifndef _WIZARD_H
#define _WIZARD_H

#include <vlog/concepts.h>

class Wizard {
private:

    Literal getMagicRelation(const bool priority, std::shared_ptr<Program> newProgram,
                             const Literal &head);

public:

    std::shared_ptr<Program> getAdornedProgram(Literal &query, Program &program);

    std::shared_ptr<Program> doMagic(const Literal &query,
                                     const std::shared_ptr<Program> inputProgram,
                                     std::pair<PredId_t, PredId_t> &inputOutputRelIDs);

};

#endif
