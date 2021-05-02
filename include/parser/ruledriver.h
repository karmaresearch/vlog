#ifndef __MCDRIVER_HPP__
#define __MCDRIVER_HPP__ 1

#include <string>
#include <cstddef>
#include <istream>
#include <sstream>

#include "rulescanner.h"
#include "ruleparser.tab.hh"

namespace MC{

    class RuleAST{
        public:
            RuleAST(std::string type="",
                std::string value="",
                RuleAST *first=NULL,
                RuleAST *second=NULL):
                    type(type),
                    value(value),
                    first(first),
                    second(second){}
            void print(int sep=0);
            ~RuleAST();
            std::string getType();
            std::string getValue();
            MC::RuleAST *getFirst();
            MC::RuleAST *getSecond();
        private:
            std::string type;
            std::string value;
            RuleAST *first;
            RuleAST *second;
            void print_indented(std::string to_indent, int spaces);
    };


    class RuleDriver{
    private:
        MC::RuleAST *root = nullptr;
        MC::RuleParser  *parser  = nullptr;
        MC::RuleScanner *scanner = nullptr;
        void parse_helper( std::istream &stream );
    public:
        RuleDriver() = default;
        ~RuleDriver(){
            delete(scanner);
            scanner = nullptr;
            delete(parser);
            parser = nullptr;
            delete(root);
            root = nullptr;
        };

        //! parse - parse from a file
        void parse( std::string filename );
        //! parse - parse from a file
        void parse( const char *filename );
        //! parse - parse from a c++ input stream
        void parse( std::istream &iss );
        void set_root(RuleAST *newroot){ root = newroot;}
        RuleAST *get_root(){ return root;}
    };
} /* end namespace MC */
#endif /* END __MCDRIVER_HPP__ */

