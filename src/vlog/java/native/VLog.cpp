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
    if (program != NULL) {
	throwAlreadyStartedException(env, "VLog is already started");
	return;
    }

    if (! logLevelSet) {
	Logger::setMinLevel(INFOL);
    }

    const char *crawconf = env->GetStringUTFChars(rawconf, NULL);
    EDBConf conf(crawconf, isfile);
    env->ReleaseStringUTFChars(rawconf, crawconf);

    EDBLayer *layer = new EDBLayer(conf, false);
    program = new Program(layer->getNTerms(), layer);
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    startCSV
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_startCSV(JNIEnv *env, jobject obj, jstring dir) {
    if (program != NULL) {
	throwAlreadyStartedException(env, "VLog is already started");
	return;
    }
     
    if (! logLevelSet) {
	Logger::setMinLevel(INFOL);
    }
    
    //Transform the string into a C++ string
    std::string dirName = jstring2string(env, dir);

    if (! Utils::exists(dirName)) {
	throwIOException(env, ("Directory " + dirName + " does not exist").c_str());
	return;
    }
    std::vector<std::string> files = Utils::getFilesWithSuffix(dirName, ".csv");
    std::stringstream edbconf;
    for (int i = 0; i < files.size(); i++) {
	std::string fn = Utils::removeExtension(Utils::filename(files[i]));
	edbconf << "EDB" << i << "_predname=" << fn << "\n";
	edbconf << "EDB" << i << "_type=INMEMORY\n";
	edbconf << "EDB" << i << "_param0=" << dirName << "\n";
	edbconf << "EDB" << i << "_param1=" << fn << "\n";
    }
    EDBConf conf(edbconf.str().c_str(), false);
    EDBLayer *layer = new EDBLayer(conf, false);
    program = new Program(layer->getNTerms(), layer);
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    stop
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_stop(JNIEnv *env, jobject obj) {
    if (program != NULL) {
	delete program->getKB();
	delete program;
	program = NULL;
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

    // TODO: fix this: this might create a new predicate if it does not exists.
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
    if (program->getKB()->getDictNumber(cliteral, strlen(cliteral), value)) {
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
    if (!program->getKB()->getDictText((uint64_t) literalid, supportText)) {
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
	iter = r.getEDBIterator(query, NULL, NULL, *(program->getKB()), false, NULL);
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
 * Method:    addRules
 * Signature: ([Ljava/lang/String;Z)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_addRules(JNIEnv *env, jobject obj, jobjectArray rules, jboolean rewriteHeads) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return;
    }
    if (rules != NULL) {
	jsize sz = env->GetArrayLength(rules);
	std::stringstream ss;
	for (int i = 0; i < sz; i++) {
	    jstring o = (jstring) env->GetObjectArrayElement(rules, (jsize) i);
	    ss << jstring2string(env, o) << "\n";
	}

	program->readFromString(ss.str(), (bool) rewriteHeads);
    }
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    addRulesFile
 * Signature: (Ljava/lang/String;Z)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_addRulesFile(JNIEnv *env, jobject obj, jstring f, jboolean rewriteHeads) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return;
    }

    //Transform the string into a C++ string
    std::string fileName = jstring2string(env, f);

    if (! Utils::exists(fileName)) {
	throwIOException(env, ("File " + fileName + " does not exist").c_str());
	return;
    }
    program->readFromFile(fileName, (bool) rewriteHeads);
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

    sn = new SemiNaiver(program->getAllRules(), *(program->getKB()), program, true, true, false, ! (bool) skolem, -1, false);
    sn->run();
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
