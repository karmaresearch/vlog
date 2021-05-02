%skeleton "lalr1.cc"
%require  "3.0"
%debug 
%defines 
%define api.namespace {MC}
%define parser_class_name {RuleParser}

%code requires{
   namespace MC {
      class RuleAST;
      class RuleDriver;
      class RuleScanner;
   }

// The following definitions is missing when %locations isn't used
# ifndef YY_NULLPTR
#  if defined __cplusplus && 201103L <= __cplusplus
#   define YY_NULLPTR nullptr
#  else
#   define YY_NULLPTR 0
#  endif
# endif

}

%parse-param { RuleScanner &scanner }
%parse-param { RuleDriver  &driver  }

%code{
   #include <iostream>
   #include <cstdlib>
   #include <fstream>
   
   /* include for all driver functions */
   #include <parser/ruledriver.h>
   void print_error_and_exit(std::string error){
      std::cerr << "              " << error << std::endl;
      exit(1);
   }

#undef yylex
#define yylex scanner.yylex
}

%define api.value.type variant
%define parse.assert

%token               END       0  "end of file"
%token <std::string> UPPERWORD
%token <std::string> LOWERWORD
%token <std::string> WORD 
%token               LEFTPAR
%token               RIGHTPAR
%token               COMMA
%token               ARROW
%token               NEWLINE
%token               NEGATE
%token <std::string> IRI

%type <MC::RuleAST*> list_of_rules
%type <MC::RuleAST*> rule
%type <MC::RuleAST*> head_literals
%type <MC::RuleAST*> body_literals
%type <MC::RuleAST*> literal
%type <MC::RuleAST*> term
%type <MC::RuleAST*> list_of_terms


%locations

%%
list_of_rules: rule                                 { $$ = new MC::RuleAST("LISTOFRULES",    "", $1,   NULL); driver.set_root($$); }
             | rule NEWLINE                         { $$ = new MC::RuleAST("LISTOFRULES",    "", $1,   NULL); driver.set_root($$); }
             | rule NEWLINE list_of_rules           { $$ = new MC::RuleAST("LISTOFRULES",    "", $1,   $3  ); driver.set_root($$); }
             | error                                { yyerrok;  print_error_and_exit("Empty rule set.");           };
               

rule : head_literals ARROW body_literals            { $$ = new MC::RuleAST("RULE",        "", $1,   $3  ); }
     | error                                        { yyerrok;  print_error_and_exit("Not valid rule.");           };

head_literals : literal                             { $$ = new MC::RuleAST("LISTOFLITERALS", "", $1,   NULL); }
              | literal COMMA head_literals         { $$ = new MC::RuleAST("LISTOFLITERALS", "", $1,   $3  ); }
              | error                               { yyerrok;  print_error_and_exit("Not valid list of head literals."); };

body_literals : literal                             { $$ = new MC::RuleAST("LISTOFLITERALS", "", $1,   NULL); }
              | literal COMMA body_literals         { $$ = new MC::RuleAST("LISTOFLITERALS", "", $1,   $3  ); }
              | error                               { yyerrok;  print_error_and_exit("Not valid list of body literals."); };

literal : UPPERWORD LEFTPAR list_of_terms RIGHTPAR  { $$ = new MC::RuleAST("POSITIVELITERAL",     $1, $3,   NULL); }
        | LOWERWORD LEFTPAR list_of_terms RIGHTPAR  { $$ = new MC::RuleAST("POSITIVELITERAL",     $1, $3,   NULL); }
        | NEGATE UPPERWORD LEFTPAR list_of_terms RIGHTPAR  { $$ = new MC::RuleAST("NEGATEDLITERAL",     $2, $4,   NULL); }
        | NEGATE LOWERWORD LEFTPAR list_of_terms RIGHTPAR  { $$ = new MC::RuleAST("NEGATEDLITERAL",     $2, $4,   NULL); }
        | error                                     { yyerrok;  print_error_and_exit("Not valid literal."); };

list_of_terms : term                                { $$ = new MC::RuleAST("LISTOFTERMS",    "", $1, NULL); }
              | term COMMA list_of_terms            { $$ = new MC::RuleAST("LISTOFTERMS",    "", $1, $3); }
              | error                               { yyerrok;  print_error_and_exit("Not valid list of terms."); };

term : UPPERWORD                           { $$ = new MC::RuleAST("VARIABLE",    $1, NULL, NULL); }
     | LOWERWORD                           { $$ = new MC::RuleAST("CONSTANT",    $1, NULL, NULL); }
     | WORD                                { $$ = new MC::RuleAST("CONSTANT",    $1, NULL, NULL); }
     | IRI                                 { $$ = new MC::RuleAST("CONSTANT",    $1, NULL, NULL); };


%%


/*There is a bug in the line number.*/
void MC::RuleParser::error( const location_type &l, const std::string &err_message ) {
   //std::cerr << "Parser Error: " << err_message << " at " << l << "\n";
   std::cerr << "Parser Error: " << std::endl;
}

