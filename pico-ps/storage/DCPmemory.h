#ifndef DCPMEMORY_H
#define DCPMEMORY_H


#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/container/simplekv.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>

#include "pico-ps/storage/KVShardStorage.h"

namespace paradigm4 {
namespace pico {
namespace ps {

using pmem::obj::persistent_ptr;

/* simple_kv is used to store the available shard in current node
 * key: storage_id + '_'+ shard_id
 * value: persistent_ptr<concurrent_hash_map>
 * 10000 is the amount of the buckets for simple_kv. currently is a magic number. 
 */
using kv_type = simple_kv<persistent_ptr<void>, 10000>; 

struct dcpmm_root {
	persistent_ptr<kv_type> kv;
    pmem::obj::p<int> node_id;
};

using pool_t = pmem::obj::pool<dcpmm_root>;

class DCPmemory {
public:
    // init
    bool initialize(std::string path, uint64_t pool_size);
    void finalize();
   
    pool_t& getPool(){
        return _pmpool;
    }

    static DCPmemory& singleton();

private:
    DCPmemory() {}
    void recovery_shard_storage();

protected:
    pool_t _pmpool;
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
    
        void flush() {
            if (!_it.empty()) {
                pmem::obj::transaction::run(DCPmemory::singleton().getPool(), [this]{
                    _it->second = _cache.second;
                });
            }
        }
        void cache() {
            _cache.first =_it->first;
            _cache.second = _it->second;
        }
        typename map_type::accessor _it;
        mutable std::pair<Key, T> _cache;
    };

    PmemHashMapHandle() {
        pmem::obj::transaction::run(DCPmemory::singleton().getPool(), [this]{
            _ptr = pmem::obj::make_persistent<map_type>();
        });
    }
    explicit PmemHashMapHandle(persistent_ptr<void> ptr) {
        _ptr = persistent_ptr<map_type>(ptr);
    }

    map_type* operator->()const {
        return this->_ptr.operator->();
    }
    
    const_iterator begin()const {
        return {_ptr->begin(), &_ptr};
    }

    const_iterator end()const {
        return {_ptr->end(), &_ptr};
    }

    size_t bucket_count()const {
        return _ptr->bucket_count();
    }

    size_t size()const {
        return _ptr->size();
    }

    bool empty()const {
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
        it->flush();
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
                accessor it;
                if (lock_find(ht, key, &it)) {
                    it->second = val;        
                } else {
                    ht.insert({key, val});
                }
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

    friend void safe_erase(PmemHashMapHandle&, const const_iterator&) {
        // TODO erase;
        return;
    }

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
    typedef PmemHashMapHandle<KEY, VALUE, HASH, KEYEQUAL> shard_type;


    typedef pico::vector<std::pair<KEY, VALUE>> vector_type;
    typedef HASH hasher_type;
    typedef std::equal_to<KEY> key_equal_type;
    typedef typename shard_type::const_iterator iterator_type;
    typedef typename shard_type::accessor accessor_type;

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
        }
        if (_storage_id != -1) {
            SLOG(INFO) << "in PmemHashMapShardStorage storage_id is : " << _storage_id;
        }
        
        for (const auto& id : shard_id) {
            create_shard(id);
        }
    }

    void clear() override {
        for (auto& shard : _shards) {
            auto shard_ptr = boost::any_cast<shard_type>(&shard.second->data);
            SCHECK(shard_ptr->_ptr != nullptr);
            // add by cc pmem remove
            shard_ptr->_ptr->defragment();
            clear_map(*shard_ptr);
            // set simple_kv ptr=nullptr
            int32_t shard_id = shard.first;
            try{
                pool_t root_pool = DCPmemory::singleton().getPool();
                std::string storage_shard_id = std::to_string(_storage_id)+"_"+std::to_string(shard_id);
                auto &pptr = root_pool.root()->kv->get(storage_shard_id); 
                if(pptr == nullptr){
                    throw std::out_of_range("root kv already deleted");
                }
                root_pool.root()->kv->put(storage_shard_id, nullptr);
            }catch(std::out_of_range &e){
                SLOG(FATAL) << e.what() 
                      << "clear pmem root kv err, storage id: " << _storage_id 
                      << " and shard id: " << shard_id 
                      << " does not exist, at class PmemHashMapShardStorage create_shard_pmem()";
            } catch (pmem::pool_error &e) {
                SLOG(FATAL) << e.what() << " clear pmem root kv err "
                      << " To DCPMM pool error at class PmemHashMapShardStorage clear()";
            } catch (std::exception &e) {
                SLOG(FATAL) << e.what()
                      << " To DCPMM error at class PmemHashMapShardStorage create_shard_pmem()";
            }
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
        
        // add by cc
        // boost::any data stores the pointer of the concurrent_hash_map
        // To gurantee data consistency if the failure happens during the allocation process, 
        // the allocation process on DCPMM has to be contained in a transaction.
        pool_t root_pool;
        std::string storage_shard_id = std::to_string(_storage_id)+"_"+std::to_string(shard_id);
        try {
            
            root_pool = DCPmemory::singleton().getPool();
            // recovery
            shard_type pptr(root_pool.root()->kv->get(storage_shard_id)); 
            if (pptr._ptr == nullptr){
                throw std::out_of_range("the old hash map has already been deleted");
            }
            // Logic when file already exists. After opening of the pool we
            // have to call runtime_initialize() function in order to
            // recalculate mask and check for consistentcy.
            pptr->runtime_initialize();
            // defragment the whole pool at the beginning
            // pptr->defragment();
            _shards[shard_id]->data = pptr;
            _shards[shard_id]->data_vector = vector_type();
            this->_mem.reshard = false;

        } catch (std::out_of_range &e) {
            try{
                // create new storage
                shard_type pptr;
                // 目前_storage_id < 0表示是临时storage，不持久化，update context中会用到。
                // TODO: 重构pmem storage id
                if (_storage_id >= 0) {
                    root_pool.root()->kv->put(storage_shard_id, pptr._ptr);
                }
                _shards[shard_id]->data = pptr;
                _shards[shard_id]->data_vector = vector_type();
                this->_mem.reshard = false;    
            } catch (std::exception &e) {
                SLOG(FATAL) << e.what()
                      << " To DCPMM error in class PmemHashMapShardStorage create_shard_pmem() ";
                return false;
            }
        } catch (pmem::pool_error &e) {
		    SLOG(FATAL) << e.what()
                  << " To DCPMM pool error in class PmemHashMapShardStorage create_shard_pmem() ";
            return false;
        } catch (std::exception &e) {
		    SLOG(FATAL) << e.what()
                  << " To DCPMM error in class PmemHashMapShardStorage create_shard_pmem() ";
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
private:
    int32_t _storage_id;
};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
