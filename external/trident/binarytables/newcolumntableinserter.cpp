#include <trident/binarytables/newcolumntableinserter.h>
#include <tridentcompr/utils/utils.h>

void NewColumnTableInserter::startAppend() {
    tmpfirstpairs.clear();
    tmpsecondpairs.clear();
    largestGroup = largestElement1 = largestElement2 = 0;
}

void NewColumnTableInserter::append(long t1, long t2) {
    if (tmpfirstpairs.size() == 0 ||
            tmpfirstpairs.at(tmpfirstpairs.size() - 1).first != t1) {
        if (!tmpfirstpairs.empty() &&
                tmpsecondpairs.size() -
                tmpfirstpairs.back().second > largestGroup) {
            largestGroup = tmpsecondpairs.size() - tmpfirstpairs.back().second;
        }
        tmpfirstpairs.push_back(std::make_pair(t1, tmpsecondpairs.size()));
    }
    tmpsecondpairs.push_back(t2);
    if (t2 >= largestElement2) {
        largestElement2 = t2;
    }
    if (t1 >= largestElement1) {
        largestElement1 = t1;
    }
}

void NewColumnTableInserter::stopAppend() {
    //Check largest group.
    if (tmpsecondpairs.size() - tmpfirstpairs.back().second > largestGroup) {
        largestGroup = tmpsecondpairs.size() - tmpfirstpairs.back().second;
    }

    //First determine the size of the maximum element in both columns
    uint8_t bytesPerFirstEntry = Utils::numBytesFixedLength(largestElement1);
    uint8_t bytesPerSecondEntry = Utils::numBytesFixedLength(largestElement2);
    uint8_t bytesPerCount = Utils::numBytesFixedLength(largestGroup);
    uint8_t bytesPerOffset = Utils::numBytesFixedLength(tmpsecondpairs.size());
    if (bytesPerFirstEntry == 0 || bytesPerSecondEntry ==0 ||
            bytesPerCount == 0 || bytesPerOffset == 0) {
        BOOST_LOG_TRIVIAL(error) << "Bytes are incorrect";
        throw 10;
    }


    //Write the header
    uint8_t header1 = (bytesPerFirstEntry << 3) + (bytesPerSecondEntry & 7);
    writeByte(header1);
    uint8_t header2 = (bytesPerCount << 3) + (bytesPerOffset & 7);
    writeByte(header2);
    writeVLong2(tmpfirstpairs.size());
    writeVLong2(tmpsecondpairs.size());

    //Write all first elements
    for (size_t i = 0; i < tmpfirstpairs.size(); ++i) {
        std::pair<uint64_t, uint64_t> v = tmpfirstpairs[i];
        writeLong(bytesPerFirstEntry, v.first);
        if (i < tmpfirstpairs.size() - 1) {
            writeLong(bytesPerCount, tmpfirstpairs[i+1].second - v.second);
        } else {
            writeLong(bytesPerCount, tmpsecondpairs.size() - v.second);
        }
        writeLong(bytesPerOffset, v.second);
    }

    //Write all second elements
    for (const auto v : tmpsecondpairs) {
        writeLong(bytesPerSecondEntry, v);
    }
}
