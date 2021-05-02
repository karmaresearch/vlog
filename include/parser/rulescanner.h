#ifndef __MCSCANNER_HPP__
#define __MCSCANNER_HPP__ 1

#if ! defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

#include "ruleparser.tab.hh"  //automatically generated
#include "location.hh"        //automatically generated

namespace MC{

class RuleScanner : public yyFlexLexer{
    public:

        RuleScanner(std::istream *in) : yyFlexLexer(in) {};

        virtual ~RuleScanner() {};

        //get rid of override virtual function warning
        using FlexLexer::yylex;

        virtual
        int yylex( MC::RuleParser::semantic_type * const lval,
                   MC::RuleParser::location_type *location );
        // YY_DECL defined in rulelexer.l
        // Method body created by flex in rulelexer.yy.cc


    private:
        /* yyval ptr */
        MC::RuleParser::semantic_type *yylval = nullptr;
    };

} /* end namespace MC */

#endif /* END __MCSCANNER_HPP__ */

