#ifndef DCPMEMORY_H
#define DCPMEMORY_H

#include <set>

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>

#include "pico-ps/service/TableDescriptor.h"
#include "pico-ps/storage/KVShardStorage.h"

namespace paradigm4 {
namespace pico {
namespace ps {

using pmem::obj::persistent_ptr;

using dcpmm_storage_id_set_type = pmem::obj::concurrent_hash_map<int32_t, bool>;

struct dcpmm_storage_meta {
    persistent_ptr<dcpmm_storage_id_set_type> storage_set;
    pmem::obj::p<int> node_id;
};

using meta_pool_t = pmem::obj::pool<dcpmm_storage_meta>;

using dcpmm_storage_shards_t = pmem::obj::concurrent_hash_map<int32_t, persistent_ptr<void>>;

struct dcpmm_storage_type {
    persistent_ptr<pmem::obj::string> storage_version_uuid;
    persistent_ptr<dcpmm_storage_shards_t> storage_shards;  // shard_id -> shard_type pptr
};

using storage_pool_t = pmem::obj::pool<dcpmm_storage_type>;

class DCPmemory {
public:
    // init
    bool initialize(std::string path, uint64_t meta_pool_size, uint64_t maximum_storage_pool_size);
    void finalize();
   
    meta_pool_t& get_meta_pool() {
        return _pm_meta_pool;
    }

    bool get_storage_pool_or_create(int32_t storage_id, storage_pool_t& out);

    // 保留传入的 storage_id，其他 dcpmm 存储内容全部删除。
    void remove_storage_exclude(const std::unordered_set<int32_t>& excluded_storage);

    // 删除传入的 storage 在 dcpmm 中的存储与 meta。
    void remove_storage(int32_t storage_id);

    static DCPmemory& singleton();

private:

    bool _create_storage_pool(int32_t storage_id, storage_pool_t& out);

    bool _open_storage_pool(int32_t storage_id, storage_pool_t& out);

    void _remove_storage_file(int32_t storage_id);

    DCPmemory() {}

protected:
    std::string _root_path;
    uint64_t _maximum_storage_pool_size;
    meta_pool_t _pm_meta_pool;
    std::unordered_map<int32_t, storage_pool_t> _storage_pool;  // 打开的 storage pool，storage_id -> storage_pool
    bool is_initialized = false;
};


template<class T>
class Pmem {
    static_assert(std::is_trivially_copyable<T>::value, "");
    static constexpr size_t n = (sizeof(T) + sizeof(uint64_t) - 1) / sizeof(uint64_t);

public:
    Pmem(const T& value) {
        uint64_t data[n];
        memcpy(data, &value, sizeof(value));
        for (size_t i = 0; i < n; ++i) {
            new(&_data[i]) pmem::obj::p<uint64_t>(data[i]);
        }
    }

    operator T()const {
        uint64_t data[n];
        for (size_t i = 0; i < n; ++i) {
            data[i] = _data[i].get_ro();
        }
        T value;
        memcpy(&value, data, sizeof(value));
        return value;
    }

    // should in transaction
    Pmem& operator=(const T& value) {
        uint64_t data[n];
        memcpy(data, &value, sizeof(value));
        for (size_t i = 0; i < n; ++i) {
            _data[i].get_rw() = data[i];
        }
        return *this;
    }
private:
    pmem::obj::p<uint64_t> _data[n];
};

template<typename Key,
      typename T,
      typename Hash = std::hash<Key>,
      typename KeyEqual = std::equal_to<Key>,
      typename PmemKey = Pmem<Key>,
      typename PmemT = Pmem<T> >
class PmemHashMapHandle {
    struct PmemHash {
        size_t operator()(const PmemKey& key) {
            return _hash(Key(key));
        }
        Hash _hash;
    };
    struct PmemKeyEqual {
        size_t operator()(const PmemKey& a, const PmemKey& b) {
            return Key(a) == Key(b);
        }
    };

public:
    using map_type = pmem::obj::concurrent_hash_map<PmemKey, PmemT, PmemHash, PmemKeyEqual>;
    struct _empty_place_holder {};

    class const_iterator {
    public:
        const_iterator() {}
        const_iterator(const typename map_type::const_iterator& it,
              const persistent_ptr<map_type>* ptr): _it(it), _ptr(ptr) {
            if (_it != (*_ptr)->end()) {
                _cache.first = _it->first;
                _cache.second = _it->second;
            }
        }
        std::pair<Key, T>* operator->()const {
            return &_cache;
        }
        std::pair<Key, T>& operator*()const {
            return _cache;
        }
        const_iterator& operator++() {
            *this = const_iterator(++_it, _ptr);
            return *this;
        }
        const_iterator operator++(int) {
            const_iterator it(*this);
            ++(*this);
            return it;
        }
        friend bool operator==(const const_iterator& a, const const_iterator& b) {
            return a._it == b._it;
        }
        friend bool operator!=(const const_iterator& a, const const_iterator& b) {
            return a._it != b._it;
        }

        typename map_type::const_iterator _it;
        const persistent_ptr<map_type>* _ptr;
        mutable std::pair<Key, T> _cache;
    };

    class accessor {
        friend PmemHashMapHandle;
    public:
        ~accessor() {
            flush();
        }
        std::pair<Key, T>* operator->()const {
            return &_cache;
        }
        std::pair<Key, T>& operator*()const {
            return _cache;
        }

        void release() {
            _it.release();
        }
    
        void flush(storage_pool_t& pool) {
            _pool = pool;
            if (!_it.empty()) {
                pmem::obj::transaction::run(_pool, [this]{
                    _it->second = _cache.second;
                });
            }
        }

        void flush() {
            flush(_pool);
        }

        void cache() {
            _cache.first =_it->first;
            _cache.second = _it->second;
        }
        typename map_type::accessor _it;
        mutable std::pair<Key, T> _cache;
        storage_pool_t _pool;
    };

    explicit PmemHashMapHandle(storage_pool_t& pool) {
        pmem::obj::transaction::run(pool, [this] {
            _ptr = pmem::obj::make_persistent<map_type>();
        });
        _pool = pool;
    }

    PmemHashMapHandle(storage_pool_t& pool, persistent_ptr<void> ptr) {
        _ptr = persistent_ptr<map_type>(ptr);
        _pool = pool;
    }

    map_type* operator->() const {
        return this->_ptr.operator->();
    }
    
    const_iterator begin() const {
        return {_ptr->begin(), &_ptr};
    }

    const_iterator end() const {
        return {_ptr->end(), &_ptr};
    }

    size_t bucket_count() const {
        return _ptr->bucket_count();
    }

    size_t size() const {
        return _ptr->size();
    }

    bool empty() const {
        return _ptr->empty();
    }

    void clear() {
        _ptr->clear();
    }

    std::pair<_empty_place_holder, bool> insert(const std::pair<const Key, T>& value) {
        bool result = _ptr->insert(value);
        return {_empty_place_holder(), result};
    }

    friend bool lock_find(PmemHashMapHandle& ht, const Key& key, accessor* it) {
        it->flush(ht._pool);
        if (ht._ptr->find(it->_it, key)) {
            it->cache();
            return true;
        } else {
            return false;
        }
    };

    friend bool safe_insert(PmemHashMapHandle& ht, const Key& key, const T& val, bool& healthy) {
        if (healthy) {
            try {
                ht.insert({key, val});
            } catch (pmem::pool_error &e) {
                SLOG(FATAL) << "To create pool error in safe_insert() " << e.what();
                healthy = false;
            } catch (std::bad_alloc &e) {
                healthy = false;
            }
        }
        return healthy;
    };

    friend bool safe_set(PmemHashMapHandle& ht, const Key& key, const T& val, bool& healthy) {
        if (healthy) {
            try {
                ht._ptr->insert_or_assign(key, val);
            } catch (pmem::pool_error &e) {
                SLOG(FATAL) << "To create pool error in safe_set() " << e.what();
                healthy = false;
            } catch (std::bad_alloc &e) {
                healthy = false;
            }
        }
        return healthy;
    };

    friend bool safe_shrink(PmemHashMapHandle&) {
        return true;
    }

    friend void safe_erase(PmemHashMapHandle& ht, const_iterator& it) {
        try {
            key_type key(it->first);
            ++it;
            ht._ptr->erase(key);
        } catch (std::exception& e) {
            SLOG(FATAL) << e.what()
                        << " To DCPMM error in class PmemHashMapShardStorage safe_erase() ";
        }
    }

    storage_pool_t _pool;
    persistent_ptr<map_type> _ptr = nullptr;
};

// add by cc
/*!
 * \brief  std::pmemobj concurrent_hash_map 实现的 Key-Value ShardStorage 类型
 */

template <typename KEY,
      typename VALUE,
      typename HASH = std::hash<KEY>,
      typename KEYEQUAL = std::equal_to<KEY>>
class PmemHashMapShardStorage : public KVShardStorage<KEY, VALUE> {
public:
    using KVShardStorage<KEY, VALUE>::_shards;
    using KVShardStorage<KEY, VALUE>::_shards_meta;
    using Storage::_use_dcpmm;
    typedef PmemHashMapHandle<KEY, VALUE, HASH, KEYEQUAL> shard_type;


    typedef pico::vector<std::pair<KEY, VALUE>> vector_type;
    typedef HASH hasher_type;
    typedef std::equal_to<KEY> key_equal_type;
    typedef typename shard_type::const_iterator iterator_type;
    typedef typename shard_type::accessor accessor_type;
    typedef typename shard_type::map_type map_type;
    typedef KVShardIterator<shard_type> shard_iterator_type;

    static void clear_map(shard_type& map) {
        map.clear();
    }

    static size_t map_memory_usage(shard_type& map){
        return map.bucket_count() * (sizeof(KEY) + sizeof(VALUE));
    }

    explicit PmemHashMapShardStorage(const std::unordered_set<int32_t>& shard_id, const Configure& conf) {
        // 假设合法storage_id >= 0，默认-1表示创建新的。
        _storage_id = -1;
        if (conf.has("pmem_create_storage")) {
            const auto& param = conf["pmem_create_storage"];
            if (param.has("storage_id")) {
                _storage_id = param.node()["storage_id"].as<int32_t>();
            }
            // 析构时是否保留 dcpmm 上的数据。非 0 值清除，0 值保留。
            // 目前测试case中使用。对于正常部署情况来说，如果pserver正常退出，应该清空数据。
            if (param.has("retain_dcpmm_data_after_deconstruction")) {
                _retain_dcpmm_data_after_deconstruction = param.node()["retain_dcpmm_data_after_deconstruction"].as<int32_t>();
            }
        }
        if (_storage_id != -1) {
            SLOG(INFO) << "in PmemHashMapShardStorage storage_id is : " << _storage_id;
        }

        SCHECK(DCPmemory::singleton().get_storage_pool_or_create(_storage_id, _storage_pool));
        
        for (const auto& id : shard_id) {
            create_shard(id);
        }
        _use_dcpmm = true;
    }

    virtual ~PmemHashMapShardStorage() {
        if (_retain_dcpmm_data_after_deconstruction) {
            SLOG(INFO) << "DCPMM storage " << _storage_id << " retain_dcpmm_data_after_deconsturction was set. Retain the data on dcpmm";
        } else {
            _shards.clear();
            DCPmemory::singleton().remove_storage(_storage_id);
        }
    }

    void clear() override {
        for (auto& shard : _shards) {
            auto shard_ptr = boost::any_cast<shard_type>(&shard.second->data);
            SCHECK(shard_ptr->_ptr != nullptr);
            // add by cc pmem remove
            shard_ptr->_ptr->defragment();
            clear_map(*shard_ptr);
        }
    }

    //virtual bool create_shard(int32_t) override {return true;}
    //bool create_shard_pmem(int32_t shard_id, int32_t storage_id){
    
    virtual bool create_shard(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        if (_shards.count(shard_id) != 0) {
            return false;
        }
        this->_mem.reshard = true;
        _shards.emplace(shard_id, std::make_unique<ShardData>());
        _shards_meta.emplace(shard_id, std::make_unique<ShardDataMeta>());

        auto& storage_shards = _storage_pool.root()->storage_shards;

        try {
            dcpmm_storage_shards_t::accessor storage_shard_acc;
            bool found = storage_shards->find(storage_shard_acc, shard_id);
            if (!found) {
                SLOG(INFO) << "storage: " << _storage_id << " not found shard " << shard_id << " on dcpmm, create new one.";
                // 新建 shard
                shard_type pptr(_storage_pool);
                storage_shards->insert({shard_id, pptr._ptr});
                _shards[shard_id]->data = pptr;
                _shards_meta[shard_id]->on_dcpmm = false;
            } else {
                SLOG(INFO) << "storage: " << _storage_id << " found shard " << shard_id << " on dcpmm.";
                // shard 从 dcpmm 中读取
                shard_type pptr(_storage_pool, storage_shard_acc->second);
                pptr._ptr->runtime_initialize();
                _shards[shard_id]->data = pptr;
                _shards_meta[shard_id]->on_dcpmm = true;
            }
            _shards[shard_id]->data_vector = vector_type();
            this->_mem.reshard = false;

        } catch (std::exception& e) {
            SLOG(FATAL) << e.what()
                        << " To DCPMM error in class PmemHashMapShardStorage create_shard_pmem() ";
            return false;
        } catch (pmem::pool_error& e) {
            SLOG(FATAL) << e.what()
                        << " To DCPMM pool error in class PmemHashMapShardStorage create_shard_pmem() ";
            return false;
        }
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

    virtual bool erase_shard(int32_t shard_id) override {
        core::lock_guard<RWSpinLock> lk(this->_mtx);
        auto it = _shards.find(shard_id);
        if (_shards.end() == it) {
            return false;
        }
        this->_shard_iterators.clear();
        this->_mem.reshard = true;
        _shards.erase(it);
        dcpmm_storage_shards_t::accessor acc;
        bool found = _storage_pool.root()->storage_shards->find(acc, shard_id);
        if (found) {
            auto pptr = persistent_ptr<map_type>(acc->second);
            pptr->free_data();
            pmem::obj::transaction::run(_storage_pool, [&] {
                pmem::obj::delete_persistent<map_type>(pptr);
            });
            acc.release();
            _storage_pool.root()->storage_shards->erase(shard_id);
        }
        this->_mem.reshard = false;
        return true;
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

    virtual bool sanity_check(int32_t storage_id, TableDescriptor& td) override {
        return this->load_version_uuid() == td.version_uuid;
    }

    virtual void store_version_uuid(const std::string& uuid_str) {
        _storage_pool.root()->storage_version_uuid->assign(uuid_str);
    } 

    virtual std::string load_version_uuid() override {
        if (_storage_pool.root()->storage_version_uuid == nullptr) {
            return "-";
        } else {
            return std::string(_storage_pool.root()->storage_version_uuid->cdata());
        }
    }

private:
    int32_t _storage_id;
    storage_pool_t _storage_pool;
    bool _retain_dcpmm_data_after_deconstruction = false;
};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif  // DCPMEMORY_H
 
