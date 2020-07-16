#ifndef PARADIGM4_PICO_PS_COMMON_DENSE_COMMON_H
#define PARADIGM4_PICO_PS_COMMON_DENSE_COMMON_H

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>


#include "pico-ps/common/core.h"
#include "pico-ps/common/Partitioner.h"
#include "pico-core/SerializationHelper.h"

namespace paradigm4 {
namespace pico {

typedef float dense_value_t;

struct dense_key_t {
    int64_t id;

    PICO_SERIALIZE(id);

    dense_key_t() : dense_key_t(0) {}

    dense_key_t(int64_t i) : id(i) {}

    bool operator==(const dense_key_t& b) const {
        return id == b.id;
    }

    static dense_key_t& empty_key() {
        static dense_key_t empty({-1});
        return empty;
    }

    static dense_key_t& deleted_key() {
        static dense_key_t deleted({-2});
        return deleted;
    }

    friend std::ostream& operator << (std::ostream& out, const dense_key_t& key) {
        out << key.id;
        return out;
    }
};

inline bool operator<(const dense_key_t& a, const dense_key_t& b) {
    return a.id < b.id;
}

struct dense_key_hasher_t {
    size_t operator()(const dense_key_t& key) const noexcept {
        return static_cast<size_t>(HashFunction::murmur_hash(static_cast<uint64_t>(key.id)));
    }
};

template <>
class DefaultPartitioner<dense_key_t> : public Partitioner<dense_key_t, dense_key_hasher_t> {
public:
    int32_t partition(const dense_key_t& key, int32_t num_buckets) const {

        return this->hash(key) % num_buckets;
    }
};

struct dense_slice_key_t {

    typedef dense_key_t key_type;

    dense_key_t id;
    int32_t slice_id;
    PICO_SERIALIZE(id, slice_id);

    dense_slice_key_t() : slice_id(0) {}

    dense_slice_key_t(int64_t id, int32_t slice_id) {
        SCHECK((id >= 0 && slice_id >= 0)
               || (id == dense_key_t::empty_key().id && slice_id == -1)
               || (id == dense_key_t::deleted_key().id && slice_id == -2));
        this->id = id;
        this->slice_id = slice_id;
    }

    dense_slice_key_t(key_type id, int32_t slice_id) {
        SCHECK((id.id >= 0 && slice_id >= 0)
               || (id == dense_key_t::empty_key() && slice_id == -1)
               || (id == dense_key_t::deleted_key() && slice_id == -2));
        this->id = id;
        this->slice_id = slice_id;
    }

    bool operator==(const dense_slice_key_t& b) const {
        return id == b.id && slice_id == b.slice_id;
    }

    static dense_slice_key_t& empty_key() {
        static dense_slice_key_t empty({key_type::empty_key(), -1});
        return empty;
    }

    static dense_slice_key_t& deleted_key() {
        static dense_slice_key_t deleted({key_type::deleted_key(), -2});
        return deleted;
    }

    friend std::ostream& operator << (std::ostream& out, const dense_slice_key_t& key) {
        out << key.id << ":" << key.slice_id;
        return out;
    }
};

inline bool operator<(const dense_slice_key_t& a, const dense_slice_key_t& b) {
    return (a.id < b.id) || (a.id == b.id && a.slice_id < b.slice_id);
}

struct dense_slice_key_hasher_t {
    size_t operator()(const dense_slice_key_t& x) const noexcept {
        return static_cast<size_t>(x.slice_id) ^ (dense_key_hasher_t()(x.id));
    }
};

template <>
class DefaultPartitioner<dense_slice_key_t>
    : public Partitioner<dense_slice_key_t, dense_slice_key_hasher_t> {
public:
    int32_t partition(const dense_slice_key_t& key, int32_t num_buckets) const {

        return this->hash(key) % num_buckets;
    }
};

namespace ps {

template <typename KEY, typename VALUE>
struct DenseValuePartitioner {
    typedef KEY key_type;
    typedef VALUE value_type;

    struct PartitionResult {
        std::vector<size_t> serialized_size;
        std::vector<value_type> parts;
    };

    PartitionResult partition(key_type, value_type& val) {
        return PartitionResult{
              std::vector<size_t>{1}, std::vector<value_type>{std::move(val)}};
    }
};

template <typename KEY, typename VALUE>
struct DenseValueMerger {
    typedef KEY key_type;
    typedef VALUE value_type;

    bool need_merge(key_type, value_type&) {
        return false;
    }

    void merge(key_type /* key */,
          value_type& merged,
          size_t /* part_num*/,
          value_type& slice,
          size_t /* idx */) {
        merged = slice;
    }

    size_t serialized_size(key_type, value_type&) {
        return 0;
    }
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_COMMON_DENSE_COMMON_H
