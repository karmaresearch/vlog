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

#include <tridentcompr/utils/utils.h>
#include <tridentcompr/utils/lz4io.h>

/**** MEMORY STATISTICS ****/
#if defined(_WIN32)
#include <windows.h>
#include <psapi.h>
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <unistd.h>
#include <sys/resource.h>

#if defined(__APPLE__) && defined(__MACH__)
#include <mach/mach.h>
#include <sys/types.h>
#include <sys/sysctl.h>

#elif (defined(_AIX) || defined(__TOS__AIX__)) || (defined(__sun__) || defined(__sun) || defined(sun) && (defined(__SVR4) || defined(__svr4__)))
#include <fcntl.h>
#include <procfs.h>

#elif defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
#include <stdio.h>
#endif
#else
#error "I don't know which OS it is being used. Cannot optimize the code..."
#endif

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/log/trivial.hpp>

#include <algorithm>
#include <vector>
#include <string>

namespace fs = boost::filesystem;

using namespace std;

int Utils::numBytes(long number) {
    long max = 32;
    if (number < 0) {
        BOOST_LOG_TRIVIAL(error) << "Negative number " << number;
    }
    for (int i = 1; i <= 8; i++) {
        if (number < max) {
            return i;
        }
        max *= 256;
    }
    BOOST_LOG_TRIVIAL(error) << "Number is too large: " << number;
    return -1;
}

int Utils::numBytes2(long number) {
    int nbytes = 0;
    do {
        number >>= 7;
        nbytes++;
    } while (number > 0);
    return nbytes;
}

int Utils::decode_int(char* buffer, int offset) {
    int n = (buffer[offset++] & 0xFF) << 24;
    n += (buffer[offset++] & 0xFF) << 16;
    n += (buffer[offset++] & 0xFF) << 8;
    n += buffer[offset] & 0xFF;
    return n;
}

void Utils::encode_int(char* buffer, int offset, int n) {
    buffer[offset++] = (n >> 24) & 0xFF;
    buffer[offset++] = (n >> 16) & 0xFF;
    buffer[offset++] = (n >> 8) & 0xFF;
    buffer[offset++] = n & 0xFF;
}

int Utils::decode_intLE(char* buffer, int offset) {
    int n = buffer[offset++] & 0xFF;
    n += (buffer[offset++] & 0xFF) << 8;
    n += (buffer[offset++] & 0xFF) << 16;
    n += (buffer[offset] & 0xFF) << 24;
    return n;
}

void Utils::encode_intLE(char* buffer, int offset, int n) {
    buffer[offset++] = n & 0xFF;
    buffer[offset++] = (n >> 8) & 0xFF;
    buffer[offset++] = (n >> 16) & 0xFF;
    buffer[offset++] = (n >> 24) & 0xFF;
}

long Utils::decode_long(char* buffer, int offset) {
    long n = (long) (buffer[offset++]) << 56;
    n += (long) (buffer[offset++] & 0xFF) << 48;
    n += (long) (buffer[offset++] & 0xFF) << 40;
    n += (long) (buffer[offset++] & 0xFF) << 32;
    n += (long) (buffer[offset++] & 0xFF) << 24;
    n += (buffer[offset++] & 0xFF) << 16;
    n += (buffer[offset++] & 0xFF) << 8;
    n += buffer[offset] & 0xFF;
    return n;
}

void Utils::encode_long(char* buffer, int offset, long n) {
    buffer[offset++] = (n >> 56) & 0xFF;
    buffer[offset++] = (n >> 48) & 0xFF;
    buffer[offset++] = (n >> 40) & 0xFF;
    buffer[offset++] = (n >> 32) & 0xFF;
    buffer[offset++] = (n >> 24) & 0xFF;
    buffer[offset++] = (n >> 16) & 0xFF;
    buffer[offset++] = (n >> 8) & 0xFF;
    buffer[offset++] = n & 0xFF;
}

long Utils::decode_longWithHeader(char* buffer) {
    long n = (long) (buffer[0] & 0x7F) << 49;
    n += (long) (buffer[1] & 0x7F) << 42;
    n += (long) (buffer[2] & 0x7F) << 35;
    n += (long) (buffer[3] & 0x7F) << 28;
    n += (long) (buffer[4] & 0x7F) << 21;
    n += (buffer[5] & 0x7F) << 14;
    n += (buffer[6] & 0x7F) << 7;
    n += buffer[7] & 0x7F;
    return n;
}

void Utils::encode_longWithHeader0(char* buffer, long n) {

    if (n < 0) {
        BOOST_LOG_TRIVIAL(error) << "Number is negative";
        exit(1);
    }

    buffer[0] = (n >> 49) & 0x7F;
    buffer[1] = (n >> 42) & 0x7F;
    buffer[2] = (n >> 35) & 0x7F;
    buffer[3] = (n >> 28) & 0x7F;
    buffer[4] = (n >> 21) & 0x7F;
    buffer[5] = (n >> 14) & 0x7F;
    buffer[6] = (n >> 7) & 0x7F;
    buffer[7] = n & 0x7F;
}

void Utils::encode_longWithHeader1(char* buffer, long n) {

    if (n < 0) {
        BOOST_LOG_TRIVIAL(error) << "Number is negative";
        exit(1);
    }

    buffer[0] = ((n >> 49) | 0x80) & 0xFF;
    buffer[1] = ((n >> 42) | 0x80) & 0xFF;
    buffer[2] = ((n >> 35) | 0x80) & 0xFF;
    buffer[3] = ((n >> 28) | 0x80) & 0xFF;
    buffer[4] = ((n >> 21) | 0x80) & 0xFF;
    buffer[5] = ((n >> 14) | 0x80) & 0xFF;
    buffer[6] = ((n >> 7) | 0x80) & 0xFF;
    buffer[7] = (n | 0x80) & 0xFF;
}

//long Utils::decode_long(char* buffer, int offset, const char nbytes) {
//  long n = 0;
//  for (int i = nbytes - 1; i >= 0; i--) {
//      n += (long) (buffer[offset++] & 0xFF) << i * 8;
//  }
//  return n;
//}

short Utils::decode_short(const char* buffer, int offset) {
    return (short) (((buffer[offset] & 0xFF) << 8) + (buffer[offset + 1] & 0xFF));
}

void Utils::encode_short(char* buffer, int offset, int n) {
    buffer[offset++] = (n >> 8) & 0xFF;
    buffer[offset++] = n & 0xFF;
}

void Utils::encode_short(char* buffer, int n) {
    buffer[0] = (n >> 8) & 0xFF;
    buffer[1] = n & 0xFF;
}

long Utils::decode_vlong(char* buffer, int *offset) {
    int pos = *offset;
    int first = buffer[pos++];
    int nbytes = ((first & 255) >> 5) + 1;
    long retval = (first & 31);

    switch (nbytes) {
    case 2:
        retval += (buffer[pos++] & 255) << 5;
        break;
    case 3:
        retval += (buffer[pos++] & 255) << 5;
        retval += (buffer[pos++] & 255) << 13;
        break;
    case 4:
        retval += (buffer[pos++] & 255) << 5;
        retval += (buffer[pos++] & 255) << 13;
        retval += (buffer[pos++] & 255) << 21;
        break;
    case 5:
        retval += (buffer[pos++] & 255) << 5;
        retval += (buffer[pos++] & 255) << 13;
        retval += (buffer[pos++] & 255) << 21;
        retval += (long) (buffer[pos++] & 255) << 29;
        break;
    case 6:
        retval += (buffer[pos++] & 255) << 5;
        retval += (buffer[pos++] & 255) << 13;
        retval += (buffer[pos++] & 255) << 21;
        retval += (long) (buffer[pos++] & 255) << 29;
        retval += (long) (buffer[pos++] & 255) << 37;
        break;
    case 7:
        retval += (buffer[pos++] & 255) << 5;
        retval += (buffer[pos++] & 255) << 13;
        retval += (buffer[pos++] & 255) << 21;
        retval += (long) (buffer[pos++] & 255) << 29;
        retval += (long) (buffer[pos++] & 255) << 37;
        retval += (long) (buffer[pos++] & 255) << 45;
        break;
    case 8:
        retval += (buffer[pos++] & 255) << 5;
        retval += (buffer[pos++] & 255) << 13;
        retval += (buffer[pos++] & 255) << 21;
        retval += (long) (buffer[pos++] & 255) << 29;
        retval += (long) (buffer[pos++] & 255) << 37;
        retval += (long) (buffer[pos++] & 255) << 45;
        retval += (long) (buffer[pos++] & 255) << 53;
        break;
    }
    *offset = pos;
    return retval;
}

int Utils::encode_vlong(char* buffer, int offset, long n) {
    int nbytes = numBytes(n);
    buffer[offset++] = (((nbytes - 1) << 5) + ((int) n & 31));
    n >>= 5;
    for (int i = 1; i < nbytes; i++) {
        buffer[offset++] = ((int) n & 255);
        n >>= 8;
    }
    return offset;
}

//short Utils::decode_vshort(char* buffer, int *offset) {
//  char n = buffer[(*offset)++];
//  if (n < 0) {
//      short return_value = (short) ((n & 127) << 8);
//      return_value += buffer[(*offset)++] & 255;
//      return return_value;
//  } else {
//      return n;
//  }
//}

//int Utils::decode_vint(char* buffer, int *offset) {
//  int n = 0;
//  int pos = *offset;
//  int b = buffer[pos++];
//  n = b & 63;
//  int nbytes = (b >> 6) & 3;
//  switch (nbytes) {
//  case 1:
//      n += (buffer[pos++] & 255) << 6;
//      break;
//  case 2:
//      n += (buffer[pos++] & 255) << 6;
//      n += (buffer[pos++] & 255) << 14;
//      break;
//  case 3:
//      n += (buffer[pos++] & 255) << 6;
//      n += (buffer[pos++] & 255) << 14;
//      n += (buffer[pos++] & 255) << 22;
//      break;
//  }
//  *offset = pos;
//  return n;
//}

int Utils::decode_vint2(char* buffer, int *offset) {
    int pos = *offset;
    int number = buffer[pos++];
    if (number < 0) {
        int longNumber = number & 127;
        int shiftBytes = 7;
        do {
            number = buffer[pos++];
            longNumber += ((number & 127) << shiftBytes);
            shiftBytes += 7;
        } while (number < 0);
        *offset = pos;
        return longNumber;
    } else {
        *offset = pos;
        return number;
    }
}

int Utils::encode_vlong2(char* buffer, int offset, long n) {
    if (n < 0) {
        BOOST_LOG_TRIVIAL(error) << "Number is negative. This is not allowed with vlong2";
        throw 10;
    }

    if (n < 128) { // One byte is enough
        buffer[offset++] = n;
        return offset;
    } else {
        int bytesToStore = 64 - numberOfLeadingZeros((unsigned long)n);
        while (bytesToStore > 7) {
            buffer[offset++] = ((n & 127) + 128);
            n >>= 7;
            bytesToStore -= 7;
        }
        buffer[offset++] = n & 127;
    }
    return offset;
}

int Utils::encode_vlong2_fast(uint8_t *out, uint64_t x) {
    int i, j;
    for (i = 9; i > 0; i--) {
        if (x & 127ULL << i * 7) break;
    }
    for (j = 0; j <= i; j++)
        out[j] = ((x >> ((i - j) * 7)) & 127) | 128;

    out[i] ^= 128;
    return i;
}

uint64_t Utils::decode_vlong2_fast(uint8_t *in) {
    uint64_t r = 0;

    do {
        r = (r << 7) | (uint64_t)(*in & 127);
    } while (*in++ & 128);

    return r;
}

long Utils::decode_vlong2(const char* buffer, int *offset) {
    int pos = *offset;
    int shift = 7;
    long n = buffer[pos] & 127;
    while (buffer[pos++] < 0) {
        n += (long) (buffer[pos] & 127) << shift;
        shift += 7;
    }
    *offset = pos;
    return n;
}

long Utils::decode_vlongWithHeader0(char* buffer, const int end, int *p) {
    int pos = 0;
    int shift = 7;
    long n = buffer[pos++] & 127;
    while (pos < end && ((buffer[pos] & 128) == 0)) {
        n += (long) (buffer[pos++] & 127) << shift;
        shift += 7;
    }
    *p = pos;
    return n;
}

long Utils::decode_vlongWithHeader1(char* buffer, const int end, int *p) {
    int pos = 0;
    int shift = 7;
    long n = buffer[pos++] & 127;
    while (pos < end && buffer[pos] < 0) {
        n += (long) (buffer[pos++] & 127) << shift;
        shift += 7;
    }
    *p = pos;
    return n;
}

int Utils::encode_vlongWithHeader0(char* buffer, long n) {
    if (n < 0) {
        BOOST_LOG_TRIVIAL(error) << "Number is negative";
        return -1;
    }

    int i = 0;
    do {
        buffer[i++] = n & 127;
        n >>= 7;
    } while (n > 0);
    return i;
}

int Utils::encode_vlongWithHeader1(char* buffer, long n) {
    if (n < 0) {
        BOOST_LOG_TRIVIAL(error) << "Number is negative";
        return -1;
    }

    int i = 0;
    do {
        buffer[i++] = ( n | 128 ) & 0xFF;
        n >>= 7;
    } while (n > 0);
    return i;
}

int Utils::encode_vint2(char* buffer, int offset, int n) {
    if (n < 128) { // One byte is enough
        buffer[offset++] = n;
        return offset;
    } else {
        int bytesToStore = 32 - numberOfLeadingZeros((unsigned int) n);
        while (bytesToStore > 7) {
            buffer[offset++] = ((n & 127) + 128);
            n >>= 7;
            bytesToStore -= 7;
        }
        buffer[offset++] = (n & 127);
    }
    return offset;
}

int Utils::commonPrefix(tTerm *o1, int s1, int e1, tTerm *o2, int s2, int e2) {
    int len = 0;
    for (int i = s1, j = s2; i < e1 && j < e2; i++, j++) {
        if (o1[i] != o2[j]) {
            return len;
        }
        ++len;
    }
    return len;
}

int Utils::compare(const char* o1, int s1, int e1, const char* o2, int s2,
                   int e2) {
    for (int i = s1, j = s2; i < e1 && j < e2; i++, j++) {
        if (o1[i] != o2[j]) {
            return ((int) o1[i] & 0xff) - ((int) o2[j] & 0xff);
        }
    }
    return (e1 - s1) - (e2 - s2);
}

int Utils::prefixEquals(char* o1, int len1, char* o2, int len2) {
    int i = 0;
    for (; i < len1 && i < len2; i++) {
        if (o1[i] != o2[i]) {
            return ((int) o1[i] & 0xff) - ((int) o2[i] & 0xff);
        }
    }
    if (i == len1) {
        return 0;
    } else {
        return 1;
    }
}

int Utils::prefixEquals(char* o1, int len, char* o2) {
    int i = 0;
    for (; i < len && o2[i] != '\0'; i++) {
        if (o1[i] != o2[i]) {
            return ((int) o1[i] & 0xff) - ((int) o2[i] & 0xff);
        }
    }
    if (i == len) {
        return 0;
    } else {
        return 1;
    }
}

double Utils::get_max_mem() {
    struct rusage rusage;
    getrusage(RUSAGE_SELF, &rusage);
    double memory = 0.0;
#if defined(__APPLE__) && defined(__MACH__)
    memory = (double) rusage.ru_maxrss / 1024 / 1024;
#else
    memory = (double)rusage.ru_maxrss / 1024;
#endif
    return memory;
}

long Utils::getSystemMemory() {
#if defined(__APPLE__) && defined(__MACH__)
    int mib[2];
    mib[0] = CTL_HW;
    mib[1] = HW_MEMSIZE;
    int64_t size = 0;
    size_t len = sizeof(size);
    sysctl(mib, 2, &size, &len, NULL, 0);
    return size;
#else
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size;
#endif
}

long Utils::getUsedMemory() {
#if defined(_WIN32)
    /* Windows -------------------------------------------------- */
    PROCESS_MEMORY_COUNTERS info;
    GetProcessMemoryInfo( GetCurrentProcess( ), &info, sizeof(info) );
    return (size_t)info.WorkingSetSize;

#elif defined(__APPLE__) && defined(__MACH__)
    /* OSX ------------------------------------------------------ */
    struct mach_task_basic_info info;
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info( mach_task_self( ), MACH_TASK_BASIC_INFO, (task_info_t) &info,
                   &infoCount) != KERN_SUCCESS)
        return (size_t) 0L; /* Can't access? */
    return (size_t) info.resident_size;

#elif defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
    /* Linux ---------------------------------------------------- */
    long rss = 0L;
    FILE* fp = NULL;
    if ( (fp = fopen( "/proc/self/statm", "r" )) == NULL )
        return (size_t)0L; /* Can't open? */
    if ( fscanf( fp, "%*s%ld", &rss ) != 1 ) {
        fclose( fp );
        return (size_t)0L; /* Can't read? */
    }
    fclose( fp );
    return (size_t)rss * (size_t)sysconf( _SC_PAGESIZE);

#else
    /* AIX, BSD, Solaris, and Unknown OS ------------------------ */
    return (size_t)0L; /* Unsupported. */
#endif
}

long Utils::getIOReadBytes() {
#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
    std::ifstream file("/proc/self/io");
    std::string line;
    while (std::getline(file, line)) {
        string::size_type loc = line.find("rchar", 0);
        if (loc != string::npos) {
            string number = line.substr(7);
            return stol(number);
        }
    }
    return (size_t)0L;
#else
    return (size_t)0L; /* Unsupported. */
#endif
}

long long unsigned Utils::getCPUCounter() {
    unsigned a, d;

    __asm__ volatile("rdtsc" : "=a" (a), "=d" (d));

    return ((unsigned long long)a) | (((unsigned long long)d) << 32);;
}

vector<string> Utils::getFilesWithPrefix(string dir, string prefix) {
    vector<string> files;
    for (fs::directory_iterator itr(dir); itr != fs::directory_iterator();
            ++itr) {
        if (fs::is_regular_file(itr->status())) {
            string s = itr->path().filename().string();
            if (boost::algorithm::starts_with(s, prefix)) {
                files.push_back(s);
            }
        }
    }
    return files;
}

vector<string> Utils::getFiles(string dir) {
    vector<string> files;
    for (fs::directory_iterator itr(dir); itr != fs::directory_iterator();
            ++itr) {
        if (fs::is_regular_file(itr->status())
                && itr->path().filename().string()[0] != '.') {
            string s = itr->path().string();
            files.push_back(s);
        }
    }
    return files;
}

int Utils::getNumberPhysicalCores() {
    return sysconf( _SC_NPROCESSORS_ONLN);
}

long Utils::quickSelect(long *vector, int size, int k) {
    std::vector<long> supportVector(vector, vector + size);
    std::nth_element(supportVector.begin(), supportVector.begin() + k,
                     supportVector.end());
    return supportVector[k];
    //  if (start == end)
//          return vector[start];
//      int j = partition(vector, start, end);
//      int length = j - start + 1;
//      if (length == k)
//          return vector[j];
//      else if (k < length)
//          return quickSelect(vector, start, j - 1, k);
//      else
//          return quickSelect(vector, j + 1, end, k - length);
}

long Utils::getNBytes(std::string input) {
    if (fs::is_directory(input)) {
        long size = 0;
        for (fs::directory_iterator itr(input); itr != fs::directory_iterator();
                ++itr) {
            if (fs::is_regular(itr->path())) {
                size += fs::file_size(itr->path());
            }
        }
        return size;
    } else {
        return fs::file_size(fs::path(input));
    }
}

bool Utils::isCompressed(std::string input) {
    if (fs::is_directory(input)) {
        bool isCompressed = false;
        for (fs::directory_iterator itr(input); itr != fs::directory_iterator();
                ++itr) {
            if (fs::is_regular(itr->path())) {
                if (itr->path().extension() == string(".gz"))
                    isCompressed = true;
            }
        }
        return isCompressed;
    } else {
        return fs::path(input).extension() == string(".gz");
    }
}

//int partition(long* input, int start, int end) {
//  int pivot = input[end];
//
//  while (start < end) {
//      while (input[start] < pivot)
//          start++;
//
//      while (input[end] > pivot)
//          end--;
//
//      if (input[start] == input[end])
//          start++;
//      else if (start < end) {
//          int tmp = input[start];
//          input[start] = input[end];
//          input[end] = tmp;
//      }
//  }
//
//  return end;
//}
