/*
 * utils.h
 *
 *  Created on: Oct 1, 2013
 *      Author: jacopo
 */

#ifndef UTILS_H_
#define UTILS_H_

#include "../main/consts.h"
#include "triple.h"
#include "hashfunctions.h"
#include <vector>
#include <string>
#include <iostream>
#include <boost/log/trivial.hpp>

using namespace std;

class Utils {
private:
public:
    static int numberOfLeadingZeros(unsigned int number) {
        if (number == 0)
            return 32;
        unsigned int n = 1;
        if (number >> 16 == 0) {
            n += 16;
            number <<= 16;
        }
        if (number >> 24 == 0) {
            n += 8;
            number <<= 8;
        }
        if (number >> 28 == 0) {
            n += 4;
            number <<= 4;
        }
        if (number >> 30 == 0) {
            n += 2;
            number <<= 2;
        }
        n -= number >> 31;
        return n;
    }

    static int numberOfLeadingZeros(unsigned long i) {
        if (i == 0)
            return 64;
        int n = 1;
        unsigned int x = (int) (i >> 32);
        if (x == 0) {
            n += 32;
            x = (int) i;
        }
        if (x >> 16 == 0) {
            n += 16;
            x <<= 16;
        }
        if (x >> 24 == 0) {
            n += 8;
            x <<= 8;
        }
        if (x >> 28 == 0) {
            n += 4;
            x <<= 4;
        }
        if (x >> 30 == 0) {
            n += 2;
            x <<= 2;
        }
        n -= x >> 31;
        return n;
    }

    static short decode_short(const char* buffer, int offset);

    static short decode_short(const char* buffer) {
        return (short) (((buffer[0] & 0xFF) << 8) + (buffer[1] & 0xFF));
    }

    static void encode_short(char* buffer, int offset, int n);
    static void encode_short(char* buffer, int n);

    static int decode_int(char* buffer, int offset);
    static int decode_int(const char* buffer);
    static void encode_int(char* buffer, int offset, int n);
    static int decode_intLE(char* buffer, int offset);
    static void encode_intLE(char* buffer, int offset, int n);

    static long decode_long(char* buffer, int offset);
    static long decode_long(const char* buffer);
    static long decode_longFixedBytes(const char* buffer, const uint8_t nbytes);

    static void encode_long(char* buffer, int offset, long n);
    static void encode_longNBytes(char* buffer, const uint8_t nbytes,
                                  const uint64_t n);

    static long decode_longWithHeader(char* buffer);
    static void encode_longWithHeader0(char* buffer, long n);
    static void encode_longWithHeader1(char* buffer, long n);

    static long decode_vlong(char* buffer, int *offset);
    static int encode_vlong(char* buffer, int offset, long n);
    static int numBytes(long number);

    static int numBytesFixedLength(long number);

    static int decode_vint2(char* buffer, int *offset);
    static int encode_vint2(char* buffer, int offset, int n);

    static long decode_vlong2(const char* buffer, int *offset);

    static int encode_vlong2_fast(uint8_t *out, uint64_t x);
    static uint64_t decode_vlong2_fast(uint8_t *out);

    static int encode_vlong2(char* buffer, int offset, long n);
    static int numBytes2(long number);

    static long decode_vlongWithHeader0(char* buffer, const int end, int *pos);
    static long decode_vlongWithHeader1(char* buffer, const int end, int *pos);
    static int encode_vlongWithHeader0(char* buffer, long n);
    static int encode_vlongWithHeader1(char* buffer, long n);

    static int compare(const char* string1, int s1, int e1, const char* string2,
                       int s2, int e2);

    static int compare(const char* o1, const int l1, const char* o2,
                       const int l2) {
        for (int i = 0; i < l1 && i < l2; i++) {
            if (o1[i] != o2[i]) {
                return (o1[i] & 0xff) - (o2[i] & 0xff);
            }
        }
        return l1 - l2;
    }

    static int prefixEquals(char* string1, int len, char* string2);

    static int prefixEquals(char* o1, int len1, char* o2, int len2);

    static int commonPrefix(tTerm *o1, int s1, int e1, tTerm *o2, int s2,
                            int e2);

    static double get_max_mem();

    static long getSystemMemory();

    static long getUsedMemory();

    static long getIOReadBytes();

    static long getIOReadChars();

    static int getNumberPhysicalCores();

    static long quickSelect(long *vector, int size, int k);

    static vector<string> getFilesWithPrefix(string dir, string prefix);

    static vector<string> getFiles(string dir);

    static long getNBytes(std::string input);

    static bool isCompressed(std::string input);

    static long long unsigned getCPUCounter();

    static int getPartition(const char *key, const int size,
                            const int partitions) {
        return abs(Hashes::dbj2s(key, size) % partitions);
    }
};
#endif /* UTILS_H_ */
