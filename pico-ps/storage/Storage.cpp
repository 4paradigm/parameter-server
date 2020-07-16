#include "pico-ps/storage/Storage.h"
#include "pico-ps/service/TableDescriptor.h"


namespace paradigm4 {
namespace pico {
namespace ps {

Storage::~Storage() {}

ShardData::~ShardData() {}

ShardStorage::~ShardStorage() {
    for (int32_t shard_id: shard_list()) {
        erase_shard(shard_id);
    }
}

ShardStorage::ShardStorage() {}

void ShardStorage::read_shard_nonlocked(int index,
      std::function<void(const boost::any&)> func) {
    boost::shared_lock<RWSpinLock> lk(_mtx);
    SCHECK(_shards.count(index) > 0) << index;
    func(_shards[index]->data);
}

void ShardStorage::read_shard(int index, std::function<void(const boost::any&)> func) {
    boost::shared_lock<RWSpinLock> lk(_mtx);
    SCHECK(_shards.count(index) > 0) << index;
    _shards[index]->lock_shared();
    read_shard_nonlocked(index, func);
    _shards[index]->unlock_shared();
}

void ShardStorage::write_shard(int index, std::function<void(boost::any&)> func) {
    boost::shared_lock<RWSpinLock> lk(_mtx);
    SCHECK(_shards.count(index) > 0) << index;
    _shards[index]->lock();
    func(_shards[index]->data);
    _shards[index]->unlock();
}

size_t ShardStorage::shard_num() {
    boost::shared_lock<RWSpinLock> lk(_mtx);
    return _shards.size();
}

std::vector<int32_t> ShardStorage::shard_list() {
    boost::shared_lock<RWSpinLock> lk(_mtx);
    std::vector<int32_t> ret;
    ret.reserve(_shards.size());
    for (const auto& it : _shards) {
        ret.push_back(it.first);
    }
    return ret;
}

bool ShardStorage::erase_shard(int32_t shard_id) {
    core::lock_guard<RWSpinLock> lk(_mtx);
    auto it = _shards.find(shard_id);
    if (_shards.end() == it) {
        return false;
    }
    _shard_iterators.clear();
    _mem.reshard = true;
    _shards.erase(it);
    _mem.reshard = false;
    return true;
}

bool ShardStorage::exist_shard(int32_t shard_id) {
    boost::shared_lock<RWSpinLock> lk(_mtx);
    return _shards.count(shard_id);
}

ShardData* ShardStorage::get(int index) {
    SCHECK(_shards.count(index) != 0) << index;
    return _shards[index].get();
}

void ShardStorage::delete_shard_iterator(int32_t, int32_t iterator_id) {
    _shard_iterators.erase(iterator_id);
}

ShardStorageMemory& ShardStorageMemory::singleton() {
    static ShardStorageMemory single;
    return single;
}
    

void ShardStorageMemory::initialize(size_t soft_limit, size_t hard_limit) { 
    std::unique_lock<std::mutex> lock(_mutex);
    _initialized = true;
    _soft_limit = soft_limit;
    _hard_limit = hard_limit;
}


void ShardStorageMemory::set_healthy(bool val) {
    if (!_initialized) {
        return;
    }
    _healthy.store(val, std::memory_order_release);
}

bool ShardStorageMemory::test_healthy() {
    if (!_initialized) {
        return true;
    }
    return _healthy.load(std::memory_order_acquire);
}

bool ShardStorageMemory::check_healthy() {
    if (!_initialized) {
        return true;
    }
    if (!_healthy.load(std::memory_order_acquire)) {
        return false;
    }
    std::unique_lock<std::mutex> lock(_mutex);
    if (pico_mem().get_used_pmem() + _reserved > _hard_limit) {
        pico_gc();
    }
    if (stable_memory() > _soft_limit) {
        pico_gc();
    }
    if (pico_mem().get_used_pmem() + _reserved > _hard_limit) {
        _healthy.store(false, std::memory_order_release);
    } else if (stable_memory() > _soft_limit) {
        _healthy.store(false, std::memory_order_release);
    } else {
        _healthy.store(true, std::memory_order_release);
    }
    return test_healthy();
}



void ShardStorageMemory::watch_memory(size_t size) {
    if (!_initialized) {
        return;
    }

    _lock.lock();
    _inserted += size;
    if (_inserted > (100 << 20)) {
        _inserted = 0;
        check_healthy();
    }
    _lock.unlock();
}

void* ShardStorageMemory::alloc(size_t n, ShardStorage* st) {
    if (!_initialized) {
        return pico_malloc(n);
    }

    std::unique_lock<std::mutex> lock(_mutex);
    char* ptr = nullptr;
    if (st->_mem.reshard) {
        _storages.insert(st);
        //no throw for create empty storage
        if (n >= (1 << 20)) {
            do {
                pico_gc();
                if (stable_memory() + n > _soft_limit) {
                    print_state();
                    SLOG(WARNING) << "allocate " << n << " failed";
                    throw_bad_alloc();
                }
            } while (!try_get_memory(lock, n));  
        }
        ptr = static_cast<char*>(newImpl<false>(n));
        SCHECK(st->_mem.current.emplace(ptr, n).second);
    } else {
        do {
            pico_gc();
            bool stable_add = false;
            size_t stable = stable_memory();
            if (st->_mem.rehashing.empty()) {
                for (auto& p: st->_mem.current) {
                    if (n > p.second) {
                        stable += n - p.second;
                        stable_add = true;
                    }
                }
                BLOG(1) << "predict next stable memory: " << (stable >> 20);
            }
            if ((stable_add && stable > _soft_limit) ||
                  stable + _reserved + n > _hard_limit) {
                print_state();
                SLOG(WARNING) << "allocate " << n << " failed";
                throw_bad_alloc();
            }
        } while (!try_get_memory(lock, n));
        ptr = static_cast<char*>(newImpl<false>(n));
        SCHECK(st->_mem.rehashing.emplace(ptr, n).second);   
    }
    for (size_t i = 0; i < n; i += 1024) {
        ptr[i] = 0;
    }
    return ptr;
}

void ShardStorageMemory::free(void* ptr, ShardStorage* st) {
    if (!_initialized) {
        pico_free(ptr);
        return;
    }

    std::unique_lock<std::mutex> lock(_mutex);
    if (st->_mem.reshard) {
        pico_free(ptr);
        SCHECK(st->_mem.current.erase(ptr));
        if (st->_mem.current.empty() && 
              st->_mem.rehashed.empty() && 
              st->_mem.rehashing.empty()) {
            _storages.erase(st);
        }
    } else {
        pico_free(ptr);
        auto it = st->_mem.current.find(ptr);
        SCHECK(it != st->_mem.current.end());
        SCHECK(st->_mem.rehashed.insert(*it).second);
        st->_mem.current.erase(it);
        while (st->_mem.rehashed.size() != st->_mem.rehashing.size()) {
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            lock.lock();
        }
        for (auto p: st->_mem.rehashing) {
            SCHECK(st->_mem.current.insert(p).second);
        }
        st->_mem.rehashed.clear();
        st->_mem.rehashing.clear();
    }
}

std::shared_ptr<void> ShardStorageMemory::reserve(size_t n) {
    if (!_initialized) {
        return nullptr;
    }
    
    std::unique_lock<std::mutex> lock(_mutex);
    do {
        if (stable_memory() + n > _hard_limit) {
            print_state();
            SLOG(WARNING) << "allocate " << n << " failed";
            throw_bad_alloc();
        }
    } while (!try_get_memory(lock, n));
    _reserved += n;
    return {this, [n, this](void*){
        std::lock_guard<std::mutex> lock(_mutex);
        _reserved -= n; 
    }};
}

void ShardStorageMemory::print_state() {
    SLOG(INFO) << "stable: " << (stable_memory() >> 20) << " "
            << "pmem: " << (pico_mem().get_used_pmem() >> 20) << " "
            << "reserved: " << (_reserved >> 20);

    for (ShardStorage* st: _storages) {
        std::string pmems = "";
        for (auto p: st->_mem.current) {
            pmems += std::to_string(p.second >> 20) + " ";
        }
        SLOG(INFO) << "current: " << pmems;

        pmems = "";
        for (auto p: st->_mem.rehashed) {
            pmems += std::to_string(p.second >> 20) + " ";
        }
        SLOG(INFO) << "rehashed: " << pmems;

        pmems = "";
        for (auto p: st->_mem.rehashing) {
            pmems += std::to_string(p.second >> 20) + " ";
        }
        SLOG(INFO) << "rehashing: " << pmems;
    }
}

size_t ShardStorageMemory::stable_memory() {
    size_t unstable = 0;
    for (auto& st: _storages) {
        std::vector<size_t> mems;
        for (auto& p: st->_mem.current) {
            mems.push_back(p.second);
        }
        sort(mems.begin(), mems.end());   
        size_t count = st->_mem.rehashing.size() - st->_mem.rehashed.size();
        for (size_t i = 0; i < count; ++i) {
            unstable += mems[i];
        }
    }
    size_t all = pico_mem().get_used_pmem();
    if (all > unstable) {
        return all - unstable;
    } else {
        SLOG(WARNING) << "memory statistics may be incorrect " << all << ' ' << unstable;
        return 0;
    }
}

bool ShardStorageMemory::try_get_memory(std::unique_lock<std::mutex>& lock, size_t n) {
    if (pico_mem().get_used_pmem() + _reserved + n > _hard_limit) {
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        lock.lock();
        return false;
    } else {
        return true;
    }
};

void ShardStorageMemory::throw_bad_alloc() {
    _healthy.store(false, std::memory_order_release);
    std::__throw_bad_alloc();
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
