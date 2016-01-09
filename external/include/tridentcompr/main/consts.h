/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Trident.

   Trident is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Trident is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Trident.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef _COMPR_CONSTS
#define _COMPR_CONSTS

#include <stdint.h>

#define PARSE_SAMPLE 0
#define PARSE_COUNTMIN 1
#define PARSE_MGCS 2
#define PARSE_COUNTMIN_MGCS 3

#define MGCS_HEAP_SIZE 1000
#define MGCS_HASH_TABLES 25

#define DICT_HASH "hash"
#define DICT_HEURISTICS "heuristics"
#define DICT_SMART "smart"

#define MAX_TERM_SIZE 16384

//Used in the compressed file
#define BLOCK_SIZE 65536
#define BLOCK_MIN_SIZE 16

#define BLOCK_SUPPORT_BUFFER_COMPR 64000000
#define MIN_MEM_SORT_TRIPLES 128000000

//Used during the parsing to detect early duplicates
#define SIZE_DUPLICATE_CACHE 500000

//Used in the schema extractor
#define SC_SIZE_SUPPORT_BUFFER 512*1024

typedef unsigned char tTerm;
typedef uint64_t nTerm;

#define S_RDF_TYPE "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
#define S_RDFS_SUBCLASS "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
#define S_RDFS_CLASS "<http://www.w3.org/2000/01/rdf-schema#Class>"
#define S_RDFS_RANGE "<http://www.w3.org/2000/01/rdf-schema#range>"
#define S_RDFS_DOMAIN "<http://www.w3.org/2000/01/rdf-schema#domain>"

//#define RDF_TYPE 0
//#define RDF_PROPERTY 1
//#define RDF_NIL 28
//#define RDF_LIST 27
//#define RDF_FIRST 26
//#define RDF_REST 25
//#define RDFS_RANGE 2
//#define RDFS_DOMAIN 3
//#define RDFS_SUBPROPERTY 4
//#define RDFS_SUBCLASS 5
//#define RDFS_MEMBER 19
//#define RDFS_LITERAL 20
//#define RDFS_CONTAINER_MEMBERSHIP_PROPERTY 21
//#define RDFS_DATATYPE 22
//#define RDFS_CLASS 23
//#define RDFS_RESOURCE 24
//#define OWL_CLASS 6
//#define OWL_FUNCTIONAL_PROPERTY 7
//#define OWL_INVERSE_FUNCTIONAL_PROPERTY 8
//#define OWL_SYMMETRIC_PROPERTY 9
//#define OWL_TRANSITIVE_PROPERTY 10
//#define OWL_SAME_AS 11
//#define OWL_INVERSE_OF 12
//#define OWL_EQUIVALENT_CLASS 13
//#define OWL_EQUIVALENT_PROPERTY 14
//#define OWL_HAS_VALUE 15
//#define OWL_ON_PROPERTY 16
//#define OWL_SOME_VALUES_FROM 17
//#define OWL_ALL_VALUES_FROM 18
//#define OWL2_PROPERTY_CHAIN_AXIOM 29
//#define OWL2_HAS_KEY 30
//#define OWL2_INTERSECTION_OF 31
//#define OWL2_UNION_OF 32
//#define OWL2_ONE_OF 33
//#define OWL2_THING 34
//#define OWL2_1 35
//#define OWL2_MAX_CARD 36
//#define OWL2_MAX_Q_CARD 37
//#define OWL2_ON_CLASS 38
//#define OWL2_NOTHING 39
//#define OWL2_DATATYPE_PROP 40
//#define OWL2_OBJECT_PROP 41
//
//#define S_RDF_NIL "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
//#define S_RDF_LIST "<http://www.w3.org/1999/02/22-rdf-syntax-ns#List>"
//#define S_RDF_FIRST "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"
//#define S_RDF_REST "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>"
//#define S_RDF_PROPERTY "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>"
//#define S_RDFS_RANGE "<http://www.w3.org/2000/01/rdf-schema#range>"
//#define S_RDFS_DOMAIN "<http://www.w3.org/2000/01/rdf-schema#domain>"
//#define S_RDFS_SUBPROPERTY "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>"
//#define S_RDFS_MEMBER "<http://www.w3.org/2000/01/rdf-schema#member>"
//#define S_RDFS_LITERAL "<http://www.w3.org/2000/01/rdf-schema#Literal>"
//#define S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY "<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty>"
//#define S_RDFS_DATATYPE "<http://www.w3.org/2000/01/rdf-schema#Datatype>"
//#define S_RDFS_RESOURCE "<http://www.w3.org/2000/01/rdf-schema#Resource>"
//#define S_OWL_CLASS "<http://www.w3.org/2002/07/owl#Class>"
//#define S_OWL_FUNCTIONAL_PROPERTY "<http://www.w3.org/2002/07/owl#FunctionalProperty>"
//#define S_OWL_INVERSE_FUNCTIONAL_PROPERTY "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>"
//#define S_OWL_SYMMETRIC_PROPERTY "<http://www.w3.org/2002/07/owl#SymmetricProperty>"
//#define S_OWL_TRANSITIVE_PROPERTY "<http://www.w3.org/2002/07/owl#TransitiveProperty>"
//#define S_OWL_SAME_AS "<http://www.w3.org/2002/07/owl#sameAs>"
//#define S_OWL_INVERSE_OF "<http://www.w3.org/2002/07/owl#inverseOf>"
//#define S_OWL_EQUIVALENT_CLASS "<http://www.w3.org/2002/07/owl#equivalentClass>"
//#define S_OWL_EQUIVALENT_PROPERTY "<http://www.w3.org/2002/07/owl#equivalentProperty>"
//#define S_OWL_HAS_VALUE "<http://www.w3.org/2002/07/owl#hasValue>"
//#define S_OWL_ON_PROPERTY "<http://www.w3.org/2002/07/owl#onProperty>"
//#define S_OWL_SOME_VALUES_FROM "<http://www.w3.org/2002/07/owl#someValuesFrom>"
//#define S_OWL_ALL_VALUES_FROM "<http://www.w3.org/2002/07/owl#allValuesFrom>"
//#define S_OWL2_PROPERTY_CHAIN_AXIOM "<http://www.w3.org/2002/07/owl#propertyChainAxiom>"
//#define S_OWL2_HAS_KEY "<http://www.w3.org/2002/07/owl#hasKey>"
//#define S_OWL2_INTERSECTION_OF "<http://www.w3.org/2002/07/owl#intersectionOf>"
//#define S_OWL2_UNION_OF "<http://www.w3.org/2002/07/owl#unionOf>"
//#define S_OWL2_ONE_OF "<http://www.w3.org/2002/07/owl#oneOf>"
//#define S_OWL2_THING "<http://www.w3.org/2002/07/owl#Thing>"
//#define S_OWL2_1 "\"1\"^^http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
//#define S_OWL2_MAX_CARD "<http://www.w3.org/2002/07/owl#maxCardinality>"
//#define S_OWL2_MAX_Q_CARD "<http://www.w3.org/2002/07/owl#maxQualifiedCardinality>"
//#define S_OWL2_ON_CLASS "<http://www.w3.org/2002/07/owl#onClass>"
//#define S_OWL2_NOTHING "<http://www.w3.org/2002/07/owl#Nothing>"
//#define S_OWL2_DATATYPE_PROP "<http://www.w3.org/2002/07/owl#DatatypeProperty>"
//#define S_OWL2_OBJECT_PROP "<http://www.w3.org/2002/07/owl#ObjectProperty>"

#endif
