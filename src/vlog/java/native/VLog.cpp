#include <jni.h>

#include "vlog_native.h"

#include <vlog/edb.h>

#include <iostream>

#ifdef __cplusplus
extern "C" {
#endif
static EDBLayer *layer = NULL;

JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_start(JNIEnv *env, jobject obj, jstring rawconf, jboolean isfile) {
    //Transform the string into a C++ string
    const char *crawconf = env->GetStringUTFChars(rawconf, 0);
    EDBConf conf(crawconf, false);
    env->ReleaseStringUTFChars(rawconf, crawconf);

    layer = new EDBLayer(conf, isfile);
}

JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_startCSV(JNIEnv *env, jobject obj, jstring dir) {
    //Transform the string into a C++ string
    const char *dirName = env->GetStringUTFChars(dir, 0);
      
}


JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_stop(JNIEnv *env, jobject obj) {
    if (layer != NULL) {
	delete layer;
	layer = NULL;
    }
}
#ifdef __cplusplus
}
#endif
