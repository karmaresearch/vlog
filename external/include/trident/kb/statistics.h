#ifndef _STATS_H
#define _STATS_H


class Stats {
private:
    long readIndexBlocks;
    long readIndexBytes;
public:

    Stats() : readIndexBlocks(0), readIndexBytes(0) {}

    void incrNReadIndexBlocks() {
        readIndexBlocks++;
    }

    void addNReadIndexBytes(const unsigned long bytes) {
        readIndexBytes += bytes;
    }

    unsigned long getNReadIndexBlocks() const {
        return readIndexBlocks;
    }

    unsigned long getNReadIndexBytes() const {
        return readIndexBytes;
    }
};

#endif
