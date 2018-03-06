#include <jni.h>

#include "vlog_native.h"

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/seminaiver.h>
#include <vlog/reasoner.h>
#include <kognac/utils.h>
#include <kognac/logs.h>

#include <iostream>
#include <cstring>

static SemiNaiver *sn = NULL;
static Program *program = NULL;
static EDBLayer *layer = NULL;
static bool logLevelSet = false;

// Utility method to convert java string to c++ string
std::string jstring2string(JNIEnv *env, jstring jstr) {
    const char *cstr = env->GetStringUTFChars(jstr, NULL);
    std::string str = std::string(cstr);
    env->ReleaseStringUTFChars(jstr, cstr);
    return str;
}

// Utility method to throw an exception
void throwException(JNIEnv *env, const char *exceptionName, const char *message) {

    jclass exClass = env->FindClass(exceptionName);
    if (exClass == NULL) {
	// Should throw an exception.
        return;
    }

    env->ThrowNew(exClass, message);
}

void throwNotStartedException(JNIEnv *env, const char *message) {
    throwException(env, "karmaresearch/vlog/NotStartedException", message);
}

void throwIOException(JNIEnv *env, const char *message) {
    throwException(env, "java/io/IOException", message);
}

void throwAlreadyStartedException(JNIEnv *env, const char *message) {
    throwException(env, "karmaresearch/vlog/AlreadyStartedException", message);
}

void throwEDBConfigurationException(JNIEnv *env, const char *message) {
    throwException(env, "karmaresearch/vlog/EDBConfigurationException", message);
}

// Converts a vector of Atoms into VLog representation.
std::vector<Literal> getVectorLiteral(JNIEnv *env, jobjectArray h, Dictionary &dict) {
    std::vector<Literal> result;
    jsize sz = env->GetArrayLength(h);
    // For all atoms:
    for (int i = 0; i < sz; i++) {
	// First, get the atom.
	jobject atom = env->GetObjectArrayElement(h, (jsize) i);
	jclass cls = env->GetObjectClass(atom);

	// Get the predicate
	jmethodID getPredicateMethod = env->GetMethodID(cls, "getPredicate", "()Ljava/lang/String;");
	LOG(DEBUGL) << "getPredicateMethod " << (long) getPredicateMethod;
	jstring jpred = (jstring) env->CallObjectMethod(atom, getPredicateMethod);
	std::string predicate = jstring2string(env, jpred);

	// Get the terms
	jmethodID getTermsMethod = env->GetMethodID(cls, "getTerms", "()[Lkarmaresearch/vlog/Term;");
	LOG(DEBUGL) << "getTermsMethod " << (long) getTermsMethod;
	jobjectArray jterms = (jobjectArray) env->CallObjectMethod(atom, getTermsMethod);
	jsize vtuplesz = env->GetArrayLength(jterms);

	// Collect conversions from terms
	VTuple tuple((uint8_t) vtuplesz);
	std::vector<VTerm> t;

	// For each term:
	for (int j = 0; j < vtuplesz; j++) {
	    // First, get the term
	    jobject jterm = env->GetObjectArrayElement(jterms, (jsize) j);
	    jclass termcls = env->GetObjectClass(jterm);

	    // Get the name
	    jmethodID getNameMethod = env->GetMethodID(termcls, "getName", "()Ljava/lang/String;");
	    LOG(DEBUGL) << "getNameMethod " << (long) getNameMethod;
	    jstring jname = (jstring) env->CallObjectMethod(jterm, getNameMethod);
	    std::string name = jstring2string(env, jname);

	    // Get the type: constant or variable
	    jmethodID getTypeMethod = env->GetMethodID(termcls, "getTermType", "()Lkarmaresearch/vlog/Term$TermType;");
	    LOG(DEBUGL) << "getTypeMethod " << (long) getTypeMethod;
	    jobject jtype = env->CallObjectMethod(jterm, getTypeMethod);
	    jclass enumClass = env->GetObjectClass(jtype);
	    jmethodID getOrdinalMethod = env->GetMethodID(enumClass, "ordinal", "()I");
	    LOG(DEBUGL) << "getOrdinalMethod " << (long) getOrdinalMethod;
	    jint type = (jint) env->CallIntMethod(jtype, getOrdinalMethod);

	    // For now, we only have two choices.
	    if (type != 0) {
		// Variable
		t.push_back(VTerm((uint8_t) dict.getOrAdd(name), 0));
	    } else {
		// Constant
		// name = program->rewriteRDFOWLConstants(name);
		uint64_t dictTerm;
		if (!layer->getDictNumber(name.c_str(), name.size(), dictTerm)) {
		    //Get an ID from the temporary dictionary
		    dictTerm = program->getOrAddToAdditional(name);
		}
		t.push_back(VTerm(0, dictTerm));
	    }
	}
	int pos = 0;
	for (std::vector<VTerm>::iterator itr = t.begin(); itr != t.end(); ++itr) {
	    tuple.set(*itr, pos++);
	}

	uint8_t adornment = Predicate::calculateAdornment(tuple);

	int64_t predid = program->getOrAddPredicate(predicate, (uint8_t) vtuplesz);
	if (predid < 0) {
	    // TODO: throw something
	}

	Predicate pred((PredId_t) predid, adornment, layer->doesPredExists((PredId_t) predid) ? EDB : IDB, (uint8_t) vtuplesz);

	Literal literal(pred, tuple);
	result.push_back(literal);
    }
    return result;
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    setLogLevel
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_setLogLevel(JNIEnv *env, jobject obj, jstring level) {
    std::string ll = jstring2string(env, level);
    if (ll == "debug") {
        Logger::setMinLevel(DEBUGL);
    } else if (ll == "info") {
        Logger::setMinLevel(INFOL);
    } else if (ll == "warning") {
        Logger::setMinLevel(WARNL);
    } else if (ll == "error") {
        Logger::setMinLevel(ERRORL);
    } else {
        Logger::setMinLevel(INFOL);
    }
    logLevelSet = true;
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    start
 * Signature: (Ljava/lang/String;Z)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_start(JNIEnv *env, jobject obj, jstring rawconf, jboolean isfile) {
    if (layer != NULL) {
	throwAlreadyStartedException(env, "VLog is already started");
	return;
    }

    if (! logLevelSet) {
	Logger::setMinLevel(INFOL);
    }

    std::string crawconf = jstring2string(env, rawconf);
    if (isfile) {
	if (! Utils::exists(crawconf)) {
	    throwIOException(env, ("File " + crawconf + " does not exist").c_str());
	    return;
	}
    }

    try {
	EDBConf conf(crawconf.c_str(), isfile);

	layer = new EDBLayer(conf, false);
	program = new Program(layer->getNTerms(), layer);
    } catch(std::string s) {
	throwEDBConfigurationException(env, s.c_str());
    }
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    stop
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_stop(JNIEnv *env, jobject obj) {
    if (program != NULL) {
	delete layer;
	delete program;
	program = NULL;
	layer = NULL;
    }
    if (sn != NULL) {
	delete sn;
	sn = NULL;
    }
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    getPredicateId
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_karmaresearch_vlog_VLog_getPredicateId(JNIEnv *env, jobject obj, jstring p) {

    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return (jint) -1;
    }

    //Transform the string into a C++ string
    std::string predName = jstring2string(env, p);

    // TODO: fix this: this might create a new predicate if it does not exist.
    // There should be a way to just do a lookup???
    Predicate pred = program->getPredicate(predName);

    return (jint) pred.getId();
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    getPredicate
 * Signature: (I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_karmaresearch_vlog_VLog_getPredicate(JNIEnv *env, jobject obj, jint predid) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return NULL;
    }
    std::string pred = program->getPredicateName((PredId_t) predid);
    if (pred == std::string("")) {
	return NULL;
    }
    return env->NewStringUTF(pred.c_str());
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    getConstantId
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_karmaresearch_vlog_VLog_getConstantId(JNIEnv *env, jobject obj, jstring literal) {

    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return -1;
    }

    const char *cliteral = env->GetStringUTFChars(literal, 0);
    uint64_t value;
    jlong retval = -1;
    if (layer->getDictNumber(cliteral, strlen(cliteral), value)) {
	retval = value;
    }
    // TODO: lookup in additional, and deal with results of chases.
    env->ReleaseStringUTFChars(literal, cliteral);
    return retval;
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    getConstant
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_karmaresearch_vlog_VLog_getConstant(JNIEnv *env, jobject obj, jlong literalid) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return NULL;
    }
    char supportText[MAX_TERM_SIZE];
    if (!layer->getDictText((uint64_t) literalid, supportText)) {
	std::string s = program->getFromAdditional((Term_t) literalid);
	if (s == std::string("")) {
	    s = "" + std::to_string(literalid >> 40) + "_"
		    + std::to_string((literalid >> 32) & 0377) + "_"
		    + std::to_string(literalid & 0xffffffff);
	}

	return env->NewStringUTF(s.c_str());
    }
    return env->NewStringUTF(supportText);
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    query
 * Signature: (I[J)Lkarmaresearch/vlog/QueryResultEnumeration;
 */
JNIEXPORT jobject JNICALL Java_karmaresearch_vlog_VLog_query(JNIEnv * env, jobject obj, jint p, jlongArray els ) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return NULL;
    }

    // Create a VLog query from the parameters.
    Predicate pred = program->getPredicate((PredId_t) p);
    jsize sz = env->GetArrayLength(els);
    VTuple tuple((uint8_t) sz);
    jlong *e = env->GetLongArrayElements(els, NULL);
    for (int i = 0; i < sz; i++) {
	jlong v = e[i];
	uint8_t varId = 0;
	uint64_t val = 0;
	if (v < 0) {
	    varId = (uint8_t) -v;
	} else {
	    val = v;
	}
	VTerm vterm(varId, val);
	tuple.set(vterm, i);
    }
    env->ReleaseLongArrayElements(els, e, JNI_ABORT);
    Literal query(pred, tuple);

    // Now create an iterator over the query result.
    TupleIterator *iter = NULL;
    Reasoner r((uint64_t) 0);
    if (pred.getType() == EDB) {
	iter = r.getEDBIterator(query, NULL, NULL, *layer, false, NULL);
    } else if (sn != NULL) {
	iter = r.getIteratorWithMaterialization(sn, query, false, NULL);
    } else {
	// No materialization yet, but non-EDB predicate ... so, empty.
	TupleTable *table = new TupleTable(sz);
	std::shared_ptr<TupleTable> pt(table);
	iter = new TupleTableItr(pt);
    }

    // Now we have the tuple iterator. Encapsulate it with a QueryResultEnumeration.
    jclass jcls=env->FindClass("karmaresearch/vlog/QueryResultEnumeration");
    jmethodID mID = env->GetMethodID(jcls, "<init>", "(J)V");
    jobject jobj = env->NewObject(jcls, mID, (jlong) iter);

    return jobj;
}


/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    setRules
 * Signature: ([Lkarmaresearch/vlog/Rule;Lkarmaresearch/vlog/VLog/RuleRewriteStrategy;)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_setRules(JNIEnv *env, jobject obj, jobjectArray rules, jobject rewriteHeads) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return;
    }
    if (rules != NULL) {
	// Create a new program, to remove any left-overs from old rule stuff
	delete program;
	program = new Program(layer->getNTerms(), layer);

	// Get rewrite flag
	jclass enumClass = env->GetObjectClass(rewriteHeads);
	jmethodID getOrdinalMethod = env->GetMethodID(enumClass, "ordinal", "()I");
	jint rewrite = (jint) env->CallIntMethod(rewriteHeads, getOrdinalMethod);

	// Get number of rules
	jsize sz = env->GetArrayLength(rules);

	// For each rule:
	for (int i = 0; i < sz; i++) {
	    Dictionary dictVariables; // temporary dictionary for variables.

	    jobject rule = env->GetObjectArrayElement(rules, (jsize) i);
	    jclass cls = env->GetObjectClass(rule);
	    jmethodID getHeadMethod = env->GetMethodID(cls, "getHead", "()[Lkarmaresearch/vlog/Atom;");
	    jmethodID getBodyMethod = env->GetMethodID(cls, "getBody", "()[Lkarmaresearch/vlog/Atom;");

	    // Get fields: head and body.
	    jobjectArray head = (jobjectArray) env->CallObjectMethod(rule, getHeadMethod); 
	    jobjectArray body = (jobjectArray) env->CallObjectMethod(rule, getBodyMethod); 

	    // Convert them into internal VLog format.
	    std::vector<Literal> vhead = getVectorLiteral(env, head, dictVariables);
	    std::vector<Literal> vbody = getVectorLiteral(env, body, dictVariables);

	    // And add the rule.
	    program->addRule(vhead, vbody, rewrite != 0);
	}
    }
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    setRulesFile
 * Signature: (Ljava/lang/String;Lkarmaresearch/vlog/VLog/RuleRewriteStrategy;)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_setRulesFile(JNIEnv *env, jobject obj, jstring f, jobject rewriteHeads) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return;
    }

    delete program;
    program = new Program(layer->getNTerms(), layer);

    // Get rewrite flag
    jclass enumClass = env->GetObjectClass(rewriteHeads);
    jmethodID getOrdinalMethod = env->GetMethodID(enumClass, "ordinal", "()I");
    jint rewrite = (jint) env->CallIntMethod(rewriteHeads, getOrdinalMethod);

    //Transform the string into a C++ string
    std::string fileName = jstring2string(env, f);

    if (! Utils::exists(fileName)) {
	throwIOException(env, ("File " + fileName + " does not exist").c_str());
	return;
    }
    program->readFromFile(fileName, rewrite != 0);
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    materialize
 * Signature: (Z)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_materialize(JNIEnv *env, jobject obj, jboolean skolem) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return;
    }

    if (sn != NULL) {
	delete sn;
    }

    sn = new SemiNaiver(program->getAllRules(), *layer, program, true, true, false, ! (bool) skolem, -1, false);
    LOG(INFOL) << "Starting full materialization";
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    sn->run();
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Runtime materialization = " << sec.count() * 1000 << " milliseconds";
    sn->printCountAllIDBs("");
}

/*
 * Class:     karmaresearch_vlog_QueryResultEnumeration
 * Method:    hasMoreElements
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_karmaresearch_vlog_QueryResultEnumeration_hasMoreElements(JNIEnv *env, jobject obj, jlong ref) {
    TupleIterator *iter = (TupleIterator *) ref;
    return (jboolean) iter->hasNext();
}

/*
 * Class:     karmaresearch_vlog_QueryResultEnumeration
 * Method:    nextElement
 * Signature: (J)[J
 */
JNIEXPORT jlongArray JNICALL Java_karmaresearch_vlog_QueryResultEnumeration_nextElement(JNIEnv *env, jobject obj, jlong ref) {
    TupleIterator *iter = (TupleIterator *) ref;
    size_t sz = iter->getTupleSize();
    iter->next();
    jlong res[16];
    for (int i = 0; i < sz; i++) {
	res[i] = iter->getElementAt(i);
    }
    jlongArray outJNIArray = env->NewLongArray(sz);
    if (NULL == outJNIArray) return NULL;
    env->SetLongArrayRegion(outJNIArray, 0 , sz, res);
    return outJNIArray;
}

/*
 * Class:     karmaresearch_vlog_QueryResultEnumeration
 * Method:    cleanup
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_QueryResultEnumeration_cleanup(JNIEnv *env, jobject obj, jlong ref) {
    TupleIterator *iter = (TupleIterator *) ref;
    delete iter;
}


#ifdef __cplusplus
}
#endif
