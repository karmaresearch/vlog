#ifndef _CONSTS_H
#define _CONSTS_H

#include <trident/kb/consts.h>

#if defined(_WIN32)
#if VLOG_SHARED_LIB
#define VLIBEXP DDLEXPORT
#else
#define VLIBEXP DDLIMPORT
#endif
#else
#define VLIBEXP
#endif

#endif
