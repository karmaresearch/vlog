#ifndef SCHEMA_H
#define SCHEMA_H

#include <vector>
#include <string>

class Schema {
public:
    static std::vector<std::string> getAllSchemaTerms() {
        std::vector<std::string> output;

        output.push_back("<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>");
        output.push_back("<http://www.w3.org/1999/02/22-rdf-syntax-ns#List>");
        output.push_back("<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>");
        output.push_back("<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>");
        output.push_back("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
        output.push_back("<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#range>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#domain>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#member>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#Literal>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#Datatype>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#Class>");
        output.push_back("<http://www.w3.org/2000/01/rdf-schema#Resource>");
        output.push_back("<http://www.w3.org/2002/07/owl#Class>");
        output.push_back("<http://www.w3.org/2002/07/owl#FunctionalProperty>");
        output.push_back("<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>");
        output.push_back("<http://www.w3.org/2002/07/owl#SymmetricProperty>");
        output.push_back("<http://www.w3.org/2002/07/owl#TransitiveProperty>");
        output.push_back("<http://www.w3.org/2002/07/owl#sameAs>");
        output.push_back("<http://www.w3.org/2002/07/owl#inverseOf>");
        output.push_back("<http://www.w3.org/2002/07/owl#equivalentClass>");
        output.push_back("<http://www.w3.org/2002/07/owl#equivalentProperty>");
        output.push_back("<http://www.w3.org/2002/07/owl#hasValue>");
        output.push_back("<http://www.w3.org/2002/07/owl#onProperty>");
        output.push_back("<http://www.w3.org/2002/07/owl#someValuesFrom>");
        output.push_back("<http://www.w3.org/2002/07/owl#allValuesFrom>");
        output.push_back("<http://www.w3.org/2002/07/owl#propertyChainAxiom>");
        output.push_back("<http://www.w3.org/2002/07/owl#hasKey>");
        output.push_back("<http://www.w3.org/2002/07/owl#intersectionOf>");
        output.push_back("<http://www.w3.org/2002/07/owl#unionOf>");
        output.push_back("<http://www.w3.org/2002/07/owl#oneOf>");
        output.push_back("<http://www.w3.org/2002/07/owl#Thing>");
        output.push_back("\"1\"^^http://www.w3.org/2001/XMLSchema#nonNegativeInteger");
        output.push_back("<http://www.w3.org/2002/07/owl#maxCardinality>");
        output.push_back("<http://www.w3.org/2002/07/owl#maxQualifiedCardinality>");
        output.push_back("<http://www.w3.org/2002/07/owl#onClass>");
        output.push_back("<http://www.w3.org/2002/07/owl#Nothing>");
        output.push_back("<http://www.w3.org/2002/07/owl#DatatypeProperty>");
        output.push_back("<http://www.w3.org/2002/07/owl#ObjectProperty>");
        output.push_back("<http://ruliee/contradiction>");

        return output;
    }
};

#endif
