#ifndef PARADIGM_PICO_PS_INCR_STORAGE_H
#define PARADIGM_PICO_PS_INCR_STORAGE_H

#include "pico-ps/storage/Storage.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class IncrStorage : public Storage {
public:
    typedef std::unordered_map<int32_t, std::unique_ptr<ShardData>> shard_table_t;
};

template <typename KEY, typename VALUE>
class IncrKVStorage : public IncrStorage {
public:
    using HASH = std::unordered_map<KEY, VALUE>;

protected:
    std::unordered_map<int, std::unique_ptr<HASH>> _storage;
};

class IncrDoubleStorage : public IncrStorage {

};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM_PICO_PS_INCR_STORAGE_H
