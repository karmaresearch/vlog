#include <jni.h>

#include "vlog_native.h"

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <kognac/utils.h>

#include <iostream>
#include <cstring>

#ifdef __cplusplus
extern "C" {
#endif
static Program *program = NULL;

void throwNotStartedException(JNIEnv *env, const char *message) {
    jclass exClass;
    const char *className = "karmaresearch/vlog/NotStartedException";

    exClass = env->FindClass(className);
    if (exClass == NULL) {
	// Should throw an exception.
        return;
    }

    env->ThrowNew(exClass, message);
}

void throwIOException(JNIEnv *env, const char *message) {
    jclass exClass;
    const char *className = "java/io/IOException";

    exClass = env->FindClass(className);
    if (exClass == NULL) {
	// Should throw an exception.
        return;
    }

    env->ThrowNew(exClass, message);
}

void throwAlreadyStartedException(JNIEnv *env, const char *message) {
    jclass exClass;
    const char *className = "karmaresearch/vlog/AlreadyStartedException";

    exClass = env->FindClass(className);
    if (exClass == NULL) {
	// Should throw an exception.
        return;
    }

    env->ThrowNew(exClass, message);
}

JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_start(JNIEnv *env, jobject obj, jstring rawconf, jboolean isfile) {
    //Transform the string into a C++ string
    if (program != NULL) {
	throwAlreadyStartedException(env, "VLog is already started");
	return;
    }
    const char *crawconf = env->GetStringUTFChars(rawconf, 0);
    EDBConf conf(crawconf, isfile);
    env->ReleaseStringUTFChars(rawconf, crawconf);

    EDBLayer *layer = new EDBLayer(conf, false);
    program = new Program(layer->getNTerms(), layer);
}

JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_startCSV(JNIEnv *env, jobject obj, jstring dir) {
    if (program != NULL) {
	throwAlreadyStartedException(env, "VLog is already started");
	return;
    }
     
    //Transform the string into a C++ string
    const char *cdir = env->GetStringUTFChars(dir, 0);
    std::string dirName(cdir);
    env->ReleaseStringUTFChars(dir, cdir);

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


JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_stop(JNIEnv *env, jobject obj) {
    if (program != NULL) {
	delete program->getKB();
	delete program;
	program = NULL;
    }
}

JNIEXPORT jint JNICALL Java_karmaresearch_vlog_VLog_getPredicateId(JNIEnv *env, jobject obj, jstring p) {

    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return (jint) -1;
    }

    //Transform the string into a C++ string
    const char *cpred = env->GetStringUTFChars(p, 0);
    std::string predName(cpred);
    env->ReleaseStringUTFChars(p, cpred);

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
 * Method:    getLiteralId
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_karmaresearch_vlog_VLog_getLiteralId(JNIEnv *env, jobject obj, jstring literal) {

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
    env->ReleaseStringUTFChars(literal, cliteral);
    return retval;
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    getLiteral
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_karmaresearch_vlog_VLog_getLiteral(JNIEnv *env, jobject obj, jlong literalid) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return NULL;
    }
    char supportText[MAX_TERM_SIZE];
    if (!program->getKB()->getDictText((uint64_t) literalid, supportText)) {
        return NULL;
    }
    return env->NewStringUTF(supportText);
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    query
 * Signature: (I[J)Lkarmaresearch/vlog/QueryResultEnumeration;
 */
JNIEXPORT jobject JNICALL Java_karmaresearch_vlog_VLog_query(JNIEnv * env, jobject obj, jint pred, jlongArray literals) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return NULL;
    }
    // TODO
    return NULL;
}

/*
 * Class:     karmaresearch_vlog_VLog
 * Method:    materialize
 * Signature: ([Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_materialize(JNIEnv *env, jobject obj, jobjectArray rules) {
    if (program == NULL) {
	throwNotStartedException(env, "VLog is not started yet");
	return NULL;
    }
    // TODO
}

/*
 * Class:     karmaresearch_vlog_QueryResultEnumeration
 * Method:    hasMoreElements
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_karmaresearch_vlog_QueryResultEnumeration_hasMoreElements(JNIEnv *env, jobject obj, jlong ref) {
    // TODO
    return false;
}

/*
 * Class:     karmaresearch_vlog_QueryResultEnumeration
 * Method:    nextElement
 * Signature: (J)[J
 */
JNIEXPORT jlongArray JNICALL Java_karmaresearch_vlog_QueryResultEnumeration_nextElement(JNIEnv *env, jobject obj, jlong ref) {
    // TODO
    return NULL;
}

#ifdef __cplusplus
}
#endif
