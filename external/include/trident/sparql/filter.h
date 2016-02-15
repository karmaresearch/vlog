#ifndef trident_filter_h
#define trident_filter_h

#include <inttypes.h>

class Filter {
public:
    virtual bool isValid(uint64_t v1, uint64_t v2, uint64_t v3) const = 0;

    virtual ~Filter() {}
};

class SameVarFilter : public Filter {
private:
    const int pos1;
    const int pos2;
    const int pos3;
public:
    SameVarFilter(int pos1, int pos2, int pos3) : pos1(pos1), pos2(pos2), pos3(pos3) {}

    bool isValid(uint64_t v1, uint64_t v2, uint64_t v3) const {
        uint64_t value = 0;
        switch (pos1) {
        case 0:
            value = v1;
            break;
        case 1:
            value = v2;
            break;
        case 2:
            value = v3;
            break;
        }

        bool ok = false;
        switch (pos2) {
        case 0:
            ok = value == v1;
            break;
        case 1:
            ok = value == v2;
            break;
        case 2:
            ok = value == v3;
            break;
        }
        if (!ok) {
            return false;
        }

        if (pos3 != -1) {
            switch (pos3) {
            case 0:
                ok = value == v1;
                break;
            case 1:
                ok = value == v2;
                break;
            case 2:
                ok = value == v3;
                break;
            }
        }

        return ok;
    }
};
#endif
