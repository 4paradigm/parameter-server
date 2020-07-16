#ifndef PARADIGM4_PICO_PS_STORAGE_KVSHARDSTORAGE_H
#define PARADIGM4_PICO_PS_STORAGE_KVSHARDSTORAGE_H

#include <type_traits>
#include <unordered_map>

#include <sparsehash/dense_hash_map>

#include "tsl/hopscotch_map.h"
#include "pico-ps/storage/Storage.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

/*!
 * \brief  Key-Value ShardStorage 类型
 */
template <typename KEY, typename VALUE>
class KVShardStorage : public ShardStorage {
public:
    typedef KEY key_type;
    typedef VALUE value_type;


protected:
    template<typename SHARD_TYPE, typename SHARD_ITERATOR_TYPE>
    ShardIterator* _get_shard_iterator_impl(int32_t shard_id, int32_t iterator_id) {
        auto it = _shard_iterators.find(iterator_id);
        if (it == _shard_iterators.end() || iterator_id == -1) {
            auto shard_it = _shards.find(shard_id);
            if (shard_it == _shards.end()) {
                return nullptr;
            }
            auto shard_ptr = boost::any_cast<SHARD_TYPE>(&shard_it->second->data);
            std::unique_ptr<ShardIterator> it_ptr = std::make_unique<SHARD_ITERATOR_TYPE>(shard_id, iterator_id, shard_ptr);
            while (it_ptr->id() == -1 || _shard_iterators.count(it_ptr->id()) > 0) {
                it_ptr->gen_id();
            }
            auto ret = it_ptr.get();
            _shard_iterators.emplace(std::make_pair(it_ptr->id(), std::move(it_ptr)));
            return ret;
        } else {
            return it->second.get();
        }
    }
};


template <typename SHARD_TYPE>
class KVShardIterator : public ShardIterator {
public:
  typedef typename SHARD_TYPE::const_iterator iterator_type;

  virtual ~KVShardIterator() {}

  KVShardIterator(int32_t shard_id, int32_t iterator_id, SHARD_TYPE* data_shard)
      : ShardIterator(shard_id, iterator_id), _data_shard(data_shard) {}

  virtual bool finished() override { return _offset >= _data_shard->size(); }

  virtual size_t iterate_batch_as_binary_archive(size_t offset, size_t batch_size, BinaryArchive* ar) override {
      if (offset >= _data_shard->size()) {
          _offset = _data_shard->size();
          return 0;
      }
      if (_offset > offset || _iterator_cache == nullptr) {
          // 需要新建 iterator cache
          _iterator_cache = std::make_unique<iterator_type>(_data_shard->begin());
          _offset = 0;
      }
      auto& iterator = *_iterator_cache;
      while (_offset < offset) {
        ++_offset;
        ++iterator;
      }
      size_t num = 0;
      while ((_offset < offset + batch_size) && iterator != _data_shard->end()) {
          ++num;
          ++_offset;
          *ar << *iterator;
          ++iterator;
      }
      return num;
  }

private:
  SHARD_TYPE* _data_shard;
  std::unique_ptr<iterator_type> _iterator_cache;
};

/*!
 * \brief  std::unordered_map 实现的Key-Value ShardStorage 类型
 */
template <typename KEY,
      typename VALUE,
      typename HASH = std::hash<KEY>,
      typename KEYEQUAL = std::equal_to<KEY>,
      typename ALLOCATOR = std::allocator<std::pair<const KEY, VALUE>>>
class UnorderedMapShardStorage : public KVShardStorage<KEY, VALUE> {
public:
    using KVShardStorage<KEY, VALUE>::_shards;
    using KVShardStorage<KEY, VALUE>::_shard_iterators;
    typedef std::unordered_map<KEY, VALUE, HASH, KEYEQUAL, ALLOCATOR> shard_type;
    typedef pico::vector<std::pair<KEY, VALUE>> vector_type;
    typedef HASH hasher_type;
    typedef KEYEQUAL key_equal_type;
    typedef ALLOCATOR alloc_type;
    typedef typename shard_type::iterator iterator_type;
    typedef typename shard_type::iterator accessor_type;
    typedef KVShardIterator<shard_type> shard_iterator_type;

    template <typename VAL, typename ALLOC>
    using map_type=std::unordered_map<KEY, VAL, HASH, KEYEQUAL, ALLOC>;

    template <typename VAL, typename ALLOC>
    static void init_map(map_type<VAL,ALLOC>& ){

    }

    template <typename VAL, typename ALLOC>
    static void clear_map(map_type<VAL,ALLOC>& map){
        map.clear();
    }

    static size_t map_memory_usage(shard_type& map) {
        return map.bucket_count() * (sizeof(KEY) + sizeof(VALUE));
    }

    explicit UnorderedMapShardStorage(const std::unordered_set<int32_t>& shard_id, const Configure&) {
        for (const auto& id : shard_id) {
            create_shard(id);
        }
    }

    void clear() override {
        for (auto& shard : _shards) {
            auto shard_ptr = boost::any_cast<shard_type>(&shard.second->data);
            SCHECK(shard_ptr != nullptr);
            clear_map(*shard_ptr);
        }
    }

    virtual bool create_shard(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        if (_shards.count(shard_id) != 0) {
            return false;
        }
        this->_mem.reshard = true;
        _shards.emplace(shard_id, std::make_unique<ShardData>());
        _shards[shard_id]->data = shard_type();
        _shards[shard_id]->data_vector = vector_type();
        init_map(boost::any_cast<shard_type&>(_shards[shard_id]->data));
        this->_mem.reshard = false;
        return true;
    }

    virtual size_t shard_size(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return shard_ptr->size();
        }
        return 0;
    }

    // TODO: calculate memory usage for nontivial value type
    virtual size_t shard_memory_usage(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return map_memory_usage(*shard_ptr);
        }
        return 0;
    }

    virtual ShardIterator* get_shard_iterator(int32_t shard_id, int32_t iterator_id) override {
        return this->template _get_shard_iterator_impl<shard_type, shard_iterator_type>(shard_id, iterator_id);
    }

};



/*!
 * \brief  std::unordered_map 实现的Key-Value ShardStorage 类型
 */
template <typename KEY,
    typename VALUE,
    typename HASH = std::hash<KEY>,
    typename KEYEQUAL = std::equal_to<KEY>,
    typename ALLOCATOR = PicoAllocator<std::pair<const KEY, VALUE>>>
class HopscotchMapShardStorage : public KVShardStorage<KEY, VALUE> {
public:
    using KVShardStorage<KEY, VALUE>::_shards;
    using KVShardStorage<KEY, VALUE>::_shard_iterators;
    typedef tsl::hopscotch_map<KEY, VALUE, HASH, KEYEQUAL, ALLOCATOR> shard_type;
    typedef pico::vector<std::pair<KEY, VALUE>> vector_type;
    typedef HASH hasher_type;
    typedef KEYEQUAL key_equal_type;
    typedef ALLOCATOR alloc_type;
    typedef typename shard_type::iterator iterator_type;
    typedef typename shard_type::iterator accessor_type;
    typedef KVShardIterator<shard_type> shard_iterator_type;

    template <typename VAL, typename ALLOC>
    using map_type=tsl::hopscotch_map<KEY, VAL, HASH, KEYEQUAL, ALLOC>;

    template <typename VAL, typename ALLOC>
    static void init_map(map_type<VAL,ALLOC>& ){

    }

    template <typename VAL, typename ALLOC>
    static void clear_map(map_type<VAL,ALLOC>& map){
        map.clear();
    }

    static size_t map_memory_usage(shard_type& map){
        return map.bucket_count() * (sizeof(KEY) + sizeof(VALUE));
    }

    explicit HopscotchMapShardStorage(const std::unordered_set<int32_t>& shard_id, const Configure&) {
        for (const auto& id : shard_id) {
            create_shard(id);
        }
    }

    void clear() override {
        for (auto& shard : _shards) {
            auto shard_ptr = boost::any_cast<shard_type>(&shard.second->data);
            SCHECK(shard_ptr != nullptr);
            clear_map(*shard_ptr);
        }
    }

    virtual bool create_shard(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        if (_shards.count(shard_id) != 0) {
            return false;
        }
        this->_mem.reshard = true;
        _shards.emplace(shard_id, std::make_unique<ShardData>());
        _shards[shard_id]->data = shard_type();
        _shards[shard_id]->data_vector = vector_type();
        init_map(boost::any_cast<shard_type&>(_shards[shard_id]->data));
        this->_mem.reshard = false;
        return true;
    }

    virtual size_t shard_size(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return shard_ptr->size();
        }
        return 0;
    }

    // TODO: calculate memory usage for nontivial value type
    virtual size_t shard_memory_usage(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return map_memory_usage(*shard_ptr);
        }
        return 0;
    }

    virtual ShardIterator* get_shard_iterator(int32_t shard_id, int32_t iterator_id) override {
        return this->template _get_shard_iterator_impl<shard_type, shard_iterator_type>(shard_id, iterator_id);
    }

};

/*!
 * \brief  pico::HashTable 实现的Key-Value ShardStorage 类型
 */
template <typename KEY,
      typename VALUE,
      typename HASH = std::hash<KEY>,
      typename KEYEQUAL = std::equal_to<KEY>,
      typename ALLOCATOR = std::allocator<core::HashTableEntry<KEY, VALUE>>>
class PicoHashTableShardStorage : public KVShardStorage<KEY, VALUE> {
public:
    using KVShardStorage<KEY, VALUE>::_shards;
    using KVShardStorage<KEY, VALUE>::_shard_iterators;
    typedef core::HashTable<KEY, VALUE, HASH, ALLOCATOR> shard_type;
    typedef pico::vector<std::pair<KEY, VALUE>> vector_type;
    typedef HASH hasher_type;
    typedef KEYEQUAL key_equal_type;
    typedef ALLOCATOR alloc_type;
    typedef typename shard_type::iterator iterator_type;
    typedef typename shard_type::iterator accessor_type;
    typedef KVShardIterator<shard_type> shard_iterator_type;

    template <typename VAL, typename ALLOC>
    using map_type = core::HashTable<KEY, VAL, HASH, ALLOC>;

    template <typename VAL, typename ALLOC>
    static void init_map(map_type<VAL,ALLOC>&) {

    }

    template <typename VAL, typename ALLOC>
    static void clear_map(map_type<VAL, ALLOC>& map) {
        map.clear();
    }

    static size_t map_memory_usage(shard_type& map){
        return map.bucket_count() * (sizeof(KEY) + sizeof(VALUE));
    }

    explicit PicoHashTableShardStorage(const std::unordered_set<int32_t>& shard_id,const Configure&) {
        for (const auto& id : shard_id) {
            create_shard(id);
        }
    }

    void clear() override {
        for (auto& shard : _shards) {
            auto shard_ptr = boost::any_cast<shard_type>(&shard.second->data);
            SCHECK(shard_ptr != nullptr);
            clear_map(*shard_ptr);
        }
    }

    virtual bool create_shard(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        if (_shards.count(shard_id) != 0) {
            return false;
        }
        this->_mem.reshard = true;
        _shards.emplace(shard_id, std::make_unique<ShardData>());
        _shards[shard_id]->data = shard_type();
        _shards[shard_id]->data_vector = vector_type();
        init_map(boost::any_cast<shard_type&>(_shards[shard_id]->data));
        this->_mem.reshard = false;
        return true;
    }

    virtual size_t shard_size(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return shard_ptr->size();
        }
        return 0;
    }

    // TODO: calculate memory usage for nontivial value type
    virtual size_t shard_memory_usage(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return map_memory_usage(*shard_ptr);
        }
        return 0;
    }

    virtual ShardIterator* get_shard_iterator(int32_t shard_id, int32_t iterator_id) override {
        return this->template _get_shard_iterator_impl<shard_type, shard_iterator_type>(shard_id, iterator_id);
    }

};

// GoogleDenseHashMap 要求设置 empty_key 和 delete_key，所以 Key 中需要这两个
// 成员函数返回这两种 key 的取值。
PICO_DEFINE_MEMBER_FUNC_CHECKER(empty_key);
PICO_DEFINE_MEMBER_FUNC_CHECKER(deleted_key);

/*!
 * \brief  google::dense_hash_map 实现的Key-Value ShardStorage 类型
 */
template <typename KEY,
      typename VALUE,
      typename HASH = std::hash<KEY>,
      typename KEYEQUAL = std::equal_to<KEY>,
      typename ALLOCATOR = ShardStorageAllocator<std::pair<KEY, VALUE>>>
class GoogleDenseHashMapShardStorage : public KVShardStorage<KEY, VALUE> {
public:
    static_assert(pico_has_member_func_empty_key<KEY>::value,
          "to use Google DenseHashMap, key type must has empty_key() field");
    static_assert(pico_has_member_func_deleted_key<KEY>::value,
          "to use Google DenseHashMap, key type must has deleted_key() field");

    using KVShardStorage<KEY, VALUE>::_shards;
    using KVShardStorage<KEY, VALUE>::_shard_iterators;
    typedef google::dense_hash_map<KEY, VALUE, HASH, KEYEQUAL, ALLOCATOR> shard_type;
    typedef pico::vector<std::pair<KEY, VALUE>> vector_type;
    typedef HASH hasher_type;
    typedef KEYEQUAL key_equal_type;
    typedef ALLOCATOR alloc_type;
    typedef typename shard_type::iterator iterator_type;
    typedef typename shard_type::iterator accessor_type;
    typedef KVShardIterator<shard_type> shard_iterator_type;

    template <typename VAL, typename ALLOC>
    using map_type=google::dense_hash_map<KEY, VAL, HASH, KEYEQUAL, ALLOC>;

    // 默认的和dense hash map一致
    double shrink = 0.2;
    double grow = 0.5;
    size_t expected_max_items = 0;

    template <typename VAL, typename ALLOC>
    static void init_map(map_type<VAL,ALLOC>& map, double shrink, double grow){
        map.set_empty_key(KEY::empty_key());
        map.set_deleted_key(KEY::deleted_key());
        map.set_resizing_parameters(shrink, grow);
    }

    template <typename VAL, typename ALLOC>
    static void clear_map(map_type<VAL, ALLOC>& map) {
        map.clear_no_resize();
    }

    static size_t map_memory_usage(shard_type& map){
        return map.bucket_count() * (sizeof(KEY) + sizeof(VALUE));
    }

    GoogleDenseHashMapShardStorage(const std::unordered_set<int32_t>& shard_id, const Configure& conf) {
        for (const auto& id : shard_id) {
            if (conf.has("google_dense_hash_map")) {
                const auto& param = conf["google_dense_hash_map"];
                if (param.has("shrink_ratio")) {
                    shrink = param.node()["shrink_ratio"].as<double>();
                }
                if (param.has("grow_ratio")) {
                    grow = param.node()["grow_ratio"].as<double>();
                }
                if (param.has("expected_max_items")) {
                    expected_max_items
                          = param.node()["expected_max_items"].as<size_t>();
                }
            }
           create_shard(id);
        }
    }

    void clear() override {
        for (auto& shard : _shards) {
            auto shard_ptr = boost::any_cast<shard_type>(&shard.second->data);
            SCHECK(shard_ptr != nullptr);
            clear_map(*shard_ptr);
        }
    }

    virtual bool create_shard(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        //LOG(INFO) << "create shard : " << shard_id << " " << shrink << " " << grow << " " << expected_max_items;
        if (_shards.count(shard_id) != 0) {
            return false;
        }
        this->_mem.reshard = true;
        _shards.emplace(shard_id, std::make_unique<ShardData>());
        _shards[shard_id]->data = shard_type(expected_max_items, HASH(), KEYEQUAL(), ALLOCATOR(this));
        _shards[shard_id]->data_vector = vector_type();
        init_map(boost::any_cast<shard_type&>(_shards[shard_id]->data), shrink, grow);
        this->_mem.reshard = false;
        return true;
    }

    virtual size_t shard_size(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return shard_ptr->size();
        }
        return 0;
    }

    // TODO: calculate memory usage for nontivial value type
    virtual size_t shard_memory_usage(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return map_memory_usage(*shard_ptr);
        }
        return 0;
    }

    virtual ShardIterator* get_shard_iterator(int32_t shard_id, int32_t iterator_id) override {
        return this->template _get_shard_iterator_impl<shard_type, shard_iterator_type>(shard_id, iterator_id);
    }

};

// Because Google DenseHashMap requires to set empty_key and deleted_key using some specific
// key, user has to make sure these two special keys cannot be used in normal case anymore.
// Please use this storage type with CAUTIONS, or use PicoHashTableShardStorage as an
// alternative.
/*!
 * \brief  google::dense_hash_map 实现的Key-Value ShardStorage 类型,
 *         当 key 为 primitive 类型时使用此实现，并且需要设置 empty_key 与 deleted_key
 * 现在应该已经不能用了
 */
template <typename KEY,
      int64_t DELETED_KEY,
      int64_t EMPTY_KEY,
      typename VALUE,
      typename HASH = std::hash<KEY>,
      typename KEYEQUAL = std::equal_to<KEY>,
      typename ALLOCATOR = std::allocator<std::pair<KEY, VALUE>>>
class IntegralKeyGoogleDenseHashMapShardStorage : public KVShardStorage<KEY, VALUE> {
public:
    static_assert(std::is_integral<KEY>::value,
          "to use this storage type, KEY type needs to be integral type.");
    using KVShardStorage<KEY, VALUE>::_shards;
    using KVShardStorage<KEY, VALUE>::_shard_iterators;
    typedef google::dense_hash_map<KEY, VALUE, HASH, KEYEQUAL, ALLOCATOR> shard_type;
    typedef pico::vector<std::pair<KEY, VALUE>> vector_type;
    typedef typename shard_type::iterator iterator_type;
    typedef typename shard_type::iterator accessor_type;
    typedef KVShardIterator<shard_type> shard_iterator_type;


    template <typename VAL, typename ALLOC>
    using map_type=google::dense_hash_map<KEY, VAL, HASH, KEYEQUAL, ALLOC>;

    template <typename VAL, typename ALLOC>
    static void init_map(map_type<VAL,ALLOC>& map, double shrink, double grow){
        map.set_empty_key(KEY(EMPTY_KEY));
        map.set_deleted_key(KEY(DELETED_KEY));
        map.set_resizing_parameters(shrink, grow);
    }

    template <typename VAL, typename ALLOC>
    static void clear_map(map_type<VAL, ALLOC>& map) {
        map.clear_no_resize();
    }

    static size_t map_memory_usage(shard_type& map){
        return map.bucket_count() * (sizeof(KEY) + sizeof(VALUE));
    }

    typedef HASH hasher_type;
    typedef KEYEQUAL key_equal_type;
    typedef ALLOCATOR alloc_type;

    // 默认的和dense hash map一致
    double shrink = 0.2;
    double grow = 0.5;
    size_t expected_max_items = 0;

    IntegralKeyGoogleDenseHashMapShardStorage(const std::unordered_set<int32_t>& shard_id, const Configure& conf) {
        for (const auto& id : shard_id) {
            // 默认的和dense hash map一致
            if (conf.has("google_dense_hash_map")) {
                const auto& param = conf["google_dense_hash_map"];
                if (param.has("shrink_ratio")) {
                    shrink = param.node()["shrink_ratio"].as<double>();
                }
                if (param.has("grow_ratio")) {
                    grow = param.node()["grow_ratio"].as<double>();
                }
                if (param.has("expected_max_items")) {
                    expected_max_items
                          = param.node()["expected_max_items"].as<size_t>();
                }
            }
            create_shard(id);
        }
    }

    void clear() override {
        for (auto& shard : _shards) {
            auto shard_ptr = boost::any_cast<shard_type>(&shard.second->data);
            SCHECK(shard_ptr != nullptr);
            clear_map(*shard_ptr);
        }
    }

    virtual bool create_shard(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        if (_shards.count(shard_id) != 0) {
            return false;
        }
        this->_mem.reshard = true;
        _shards.emplace(shard_id, std::make_unique<ShardData>());
        _shards[shard_id]->data = shard_type(expected_max_items);
        _shards[shard_id]->data_vector = vector_type();
        init_map(boost::any_cast<shard_type&>(_shards[shard_id]->data), shrink, grow);
        this->_mem.reshard = false;
        return true;
    }

    virtual size_t shard_size(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return shard_ptr->size();
        }
        return 0;
    }

    // TODO: calculate memory usage for nontivial value type
    virtual size_t shard_memory_usage(int32_t shard_id) override {
        boost::shared_lock<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (it != _shards.end()) {
            auto shard_ptr = boost::any_cast<shard_type>(&it->second->data);
            return map_memory_usage(*shard_ptr);
        }
        return 0;
    }

    virtual ShardIterator* get_shard_iterator(int32_t shard_id, int32_t iterator_id) override {
        return this->template _get_shard_iterator_impl<shard_type, shard_iterator_type>(shard_id, iterator_id);
    }
};

template <typename T>
class SizeMpscQueue {
public:
    SizeMpscQueue () {}

    SizeMpscQueue(const SizeMpscQueue& other) {
        SCHECK(other._size.load() == 0) << "blame boost any";
    }

    SizeMpscQueue& operator=(const SizeMpscQueue& other) {
        SCHECK(other._size.load() == 0) << "blame boost any";
    }

    void push(T&& v) {
        _size.fetch_add(1, std::memory_order_relaxed);
        _que.push(std::move(v));
    }

    bool pop(T& v) {
        _size.fetch_sub(1, std::memory_order_relaxed);
        return _que.pop(v);
    }

    //没有线程在push或pop时应是准确的
    size_t size() {
        return _size.load(std::memory_order_relaxed);
    }

    T* top() {
        return _que.top();
    }

private:
    core::MpscQueue<T> _que;
    std::atomic<size_t> _size = {0};
    char _cache_line_pad[64 - sizeof(size_t)];
};

template <typename KEY, typename VALUE>
class SizeMpscQueueShardStorage : public KVShardStorage<KEY, VALUE>  {
public:
    using KVShardStorage<KEY, VALUE>::_shards;
    typedef pico::vector<std::pair<KEY, VALUE>> vector_type;
    typedef SizeMpscQueue<vector_type> shard_type;
    SizeMpscQueueShardStorage(const std::unordered_set<int32_t>& shard_id, const Configure&) {
        for (const auto& id : shard_id) {
            create_shard(id);
        }
    }

    void clear() override {
        for (auto& shard : _shards) {
            auto shard_ptr = boost::any_cast<shard_type>(&shard.second->data);
            vector_type block;
            SCHECK(shard_ptr != nullptr);
            while (shard_ptr->pop(block));
        }
    }

    virtual bool create_shard(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        if (_shards.count(shard_id) != 0) {
            return false;
        }
        _shards.emplace(shard_id, std::make_unique<ShardData>());
        _shards[shard_id]->data = shard_type();
        _shards[shard_id]->data_vector = vector_type();
        return true;
    }

    //no use
    virtual size_t shard_size(int32_t) override {
        return 0;
    }

    //no use
    virtual size_t shard_memory_usage(int32_t) override {
        return 0;
    }

    virtual ShardIterator* get_shard_iterator(int32_t, int32_t) override {
        SLOG(FATAL) << "No implementation";
        return nullptr;
    }
};

template <typename Storage, typename VALUE>
class ClientStorage {
public:

    typedef typename Storage::key_type KEY;
    typedef typename Storage::template map_type<VALUE, 
          PicoAllocator<std::pair<KEY, VALUE>> > shard_type;

    explicit ClientStorage(int shard_num) : _shards(shard_num) {
        for (int i = 0; i < shard_num; ++i) {
            Storage::init_map(_shards[i]);
        }
    }

    VALUE& get(int shard, const KEY& k) {
        return _shards[shard][k];
    }

    const VALUE& get(int shard, const KEY& k) const {
        return _shards[shard][k];
    }

    shard_type& operator[](int i) {
        return _shards[i];
    }

    const shard_type& operator[](int i) const {
        return _shards[i];
    }

    size_t total_size() const {
        size_t total = 0;
        for (size_t i = 0; i < _shards.size(); ++i) {
            total += _shards[i].size();
        }
        return total;
    }

    size_t size() const {
        return _shards.size();
    }

    void clear() {
        for (size_t i = 0; i < _shards.size(); ++i) {
            Storage::clear_map(_shards[i]);
        }
    }

    int32_t shard_num() const {
        return int32_t(_shards.size());
    }


    std::vector<shard_type> _shards;

};

// HT::iterator用来排除PmemHashMap，感觉函数重载优先级有问题。
template <class HT, class KEY>
bool lock_find(HT& ht, KEY&& key, typename HT::iterator* it) {
    *it = ht.find(std::forward<KEY>(key));
    return *it != ht.end();
};

template <class HT, class KEY, class VAL>
bool safe_insert(HT& ht, KEY&& key, VAL&& val, bool& healthy, typename HT::iterator* = nullptr) {
    if (healthy) {
        try {
            return ht.insert({std::forward<KEY>(key), std::forward<VAL>(val)}).second;
        } catch(std::bad_alloc&) {
            healthy = false;
        }
    }
    return healthy;
};

template <class HT, class KEY, class VAL>
bool safe_set(HT& ht, KEY&& key, VAL&& val, bool& healthy, typename HT::iterator* = nullptr) {
    if (healthy) {
        try {
            ht[std::forward<KEY>(key)] = std::forward<VAL>(val);
        } catch(std::bad_alloc&) {
            healthy = false;
        }
    }
    return healthy;
};

template<class HT>
bool safe_shrink(HT& ht, typename HT::iterator* = nullptr) {
    try {
        auto remove_deleted = ht;
        std::iter_swap(&ht, &remove_deleted);
        return true;
    } catch(std::bad_alloc&) {
        return false;
    }
}

template<class HT, class IT>
void safe_erase(HT& ht, IT it, typename HT::iterator* = nullptr) {
    try {
        ht.erase(it++);
    } catch(std::bad_alloc&) {
        SLOG(FATAL) << "unexpected std::bad_alloc!";
    }
}

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_STORAGE_KVSHARDSTORAGE_H
