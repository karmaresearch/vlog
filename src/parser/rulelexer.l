%{
/* C++ string header, for string ops below */
#include <string>
#include <iostream>

/* Implementation of yyFlexScanner */
#include <parser/rulescanner.h>
#undef  YY_DECL
#define YY_DECL int MC::RuleScanner::yylex( MC::RuleParser::semantic_type * const lval, MC::RuleParser::location_type *loc)

/* typedef to make the returns for the tokens shorter */
using token = MC::RuleParser::token;

/* define yyterminate as this instead of NULL */
#define yyterminate() return( token::END )

/* msvc2010 requires that we exclude this header file. */
#define YY_NO_UNISTD_H

/* update location on matching */
#define YY_USER_ACTION loc->step(); loc->columns(yyleng);

%}

%option debug
%option nodefault
%option yyclass="MC::RuleScanner"
%option noyywrap
%option c++

 /* See https://github.com/dajobe/raptor/blob/master/src/turtle_lexer.l */
 /* Tokens from Turtle 2013 spec - lex-ifyed to remove unicode ranges */
HEX [0-9A-Fa-f]
UCHAR "\\u"{HEX}{HEX}{HEX}{HEX}|"\\U"{HEX}{HEX}{HEX}{HEX}{HEX}{HEX}{HEX}{HEX}
IRI "<"([^\x00-\x20<>\"{}\|^`\\]|{UCHAR})*">"

%%

%{          /** Code executed at the beginning of yylex **/
            yylval = lval;
%}


{IRI} { yylval->build< std::string >( yytext );
       return(token::IRI);
       }

neg\_ {return(token::NEGATE);}
~     {return(token::NEGATE);}

[a-z][a-zA-Z0-9]* {
                     /**
                     * Section 10.1.5.1 of the 3.0.2 Bison Manual says the
                     * following should work:
                     * yylval.build( yytext );
                     * but it doesn't.
                     * ref: http://goo.gl/KLn0w2
                     */
                     yylval->build< std::string >( yytext );
                     return(token::LOWERWORD);
                     }

\"([^\"\\\n\r]|\\[^\n\r])*\"   { yylval->build< std::string >( yytext );
                                 return(token::WORD);};
[A-Z][a-zA-Z0-9]* {  yylval->build< std::string >( yytext );
                     return(token::UPPERWORD);
                     }
\"         {return(token::LEFTPAR);}
\(         {return(token::LEFTPAR);}
\)         {return(token::RIGHTPAR);}
,          {return(token::COMMA);}
:-         {return(token::ARROW);}
\n+        {return(token::NEWLINE);}
[\ \t\v]+  {;}                              /*ignore spaces and tabs*/
.          {std::cout << "Lexer ERROR. Unrecognized character: " << yytext << std::endl;
            std::exit(1);
           }
%%

