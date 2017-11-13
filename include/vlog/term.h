#ifndef TERM_H
#define TERM_H

#include <inttypes.h>

// The three defines below are settable.

// Set this to the unsigned integer type that will contain the values.
#define __TERM_TYPE uint64_t

// Set this to 1 if __TERM_TYPE is uint64_t, 0 otherwise.
#define __TERM_TYPE_IS_UINT64_T 1

// Set this to 1 if the Term_t type should be a struct, or 0 if it should be the unsigned
// integer type itself.
#define TERM_AS_STRUCT 0

#if TERM_AS_STRUCT

// Set to 0 if Term_t is not uint64_t.
#define TERM_IS_UINT64 0

struct Term_t {

    __TERM_TYPE value;

    Term_t() : value(0) {
    }

    Term_t(__TERM_TYPE v) : value(v) {
    }

    Term_t operator = (const Term_t other) {
        value = other.value;
        return *this;
    }

    Term_t operator = (const __TERM_TYPE other) {
        value = other;
        return *this;
    }

    bool operator == (Term_t other) const {
        return value == other.value;
    }

    bool operator < (Term_t other) const {
        return value < other.value;
    }

    operator __TERM_TYPE () const {
        return value;
    }
};

// Extend std::hash and std::equal_to for 'Term_t' and 'const Term_t'.
namespace std
{
    template <>
        struct hash<Term_t> {
            size_t operator()(Term_t const & x) const noexcept {
                return std::hash<__TERM_TYPE>()(x.value);
            }
        };

    template <>
        struct equal_to<Term_t> {
            bool operator() (Term_t const &x, Term_t const &y) const noexcept {
                return x == y;
            }
        };

    template <>
        struct hash<const Term_t> {
            size_t operator()(Term_t const & x) const noexcept {
                return std::hash<__TERM_TYPE>()(x.value);
            }
        };

    template <>
        struct equal_to<const Term_t> {
            bool operator() (Term_t const &x, Term_t const &y) const noexcept {
                return x == y;
            }
        };
}

#else

#define TERM_IS_UINT64 __TERM_TYPE_IS_UINT64_T

typedef __TERM_TYPE Term_t;

namespace std {
    template<>
        struct hash<std::pair<Term_t,Term_t>> {
            size_t operator()(const std::pair<Term_t,Term_t> &t) const {
                size_t seed = 0;
                std::hash<uint64_t> hasher;
                seed ^= hasher(t.first) + 0x9e3779b9 + (seed<<6) + (seed>>2);
                seed ^= hasher(t.second) + 0x9e3779b9 + (seed<<6) + (seed>>2);
                return seed;
            }
        };
}


#endif
#endif
