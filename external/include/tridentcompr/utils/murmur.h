/**
 * `murmurhash.h' - murmurhash
 *
 * copyright (c) 2014 joseph werle <joseph.werle@gmail.com>
 * https://github.com/jwerle/murmurhash.c/blob/master/murmurhash.h
 */

#ifndef MURMURHASH_H
#define MURMURHASH_H 1

#include <stdint.h>

#define MURMURHASH_VERSION "0.0.3"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Returns a murmur hash of `key' based on `seed'
 * using the MurmurHash3 algorithm
 */

void
MurmurHash (const char *, uint32_t, uint32_t, uint32_t*);

#ifdef __cplusplus
}
#endif

#endif
