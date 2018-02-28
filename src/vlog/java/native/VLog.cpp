#include <jni.h>

#include "karmaresearch_vlog_VLog.h"

#include <vlog/edb.h>

#include <iostream>

JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_start
(JNIEnv *env, jobject obj, jstring rawconf) {
    //Transform the string into a C++ string
    const char *crawconf = env->GetStringUTFChars(rawconf, 0);
    EDBConf conf(crawconf, false);
    env->ReleaseStringUTFChars(rawconf, crawconf);

    EDBLayer *layer = new EDBLayer(conf, false);
    delete layer;
}
