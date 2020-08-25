#ifndef PARADIGM4_PICO_PS_STORAGE_STORAGE_H
#define PARADIGM4_PICO_PS_STORAGE_STORAGE_H

#include <unordered_set>

#include <boost/any.hpp>
#include <boost/thread/shared_mutex.hpp>


#include "pico-ps/common/core.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

struct TableDescriptor;

/*!
 * \brief  最基础 Storage 类型，其他类型的 Storage 均继承于此类型。
 *         定义 Storage 需要实现的接口。
 *         目前只有 ShardData 一种派生类型。
 */
class Storage : public NoncopyableObject {
public:
    /*! \brief  清除当前 Storage 的内容 */
    virtual void clear() = 0;
    virtual ~Storage();
    virtual bool sanity_check(int32_t storage_id, TableDescriptor& td) {
        return true;
    }
    virtual bool need_remote_restore() {
        return true;
    }

    virtual void store_version_uuid(const std::string&) {}

    virtual std::string load_version_uuid() {
        return "-";
    }

    bool use_dcpmm() const {
        return _use_dcpmm;
    }
protected:
    bool _use_dcpmm = false;
};

class ShardIterator {
public:
    virtual ~ShardIterator() {}

    ShardIterator(int32_t shard_id, int32_t iterator_id)
        : _shard_id(shard_id), _iterator_id(iterator_id), _offset(0) {}

    size_t current_offset() const {
        return _offset;
    }

    int32_t gen_id() {
        _iterator_id = pico_real_random() * static_cast<double>(std::numeric_limits<int32_t>::max());
        return _iterator_id;
    }

    int32_t id() const {
        return _iterator_id;
    }

    virtual bool finished() = 0;

    virtual size_t iterate_batch_as_binary_archive(size_t offset, size_t batch_size, BinaryArchive* ar) = 0;

protected:
    int32_t _shard_id;
    int32_t _iterator_id;
    size_t _offset;
};

/*!
 * \brief  每个 Shard 所存储的具体数据类。
 *         使用 boost::any 保存数据存储类型，目的是获得足够的灵活性，
 *         如果是离散型 key-value 存储，boost::any 中可保存为某种 hashmap 的类型。
 *         如果是连续型存储，boost::any 中可保存为类似 vector 的连续型类型。
 */
struct ShardData : public NoncopyableObject {
    virtual ~ShardData();

    boost::any data;
    boost::any data_vector;
    /*!
     * \brief  用于 data 成员多线程并发访问的锁，目前使用 spinlock 实现。
     *         这是由于在训练时，读写的请求比例为 1:1；而对于目前选用的 Google DenseHashMap
     *         来说， 读操作与写操作的效率相差不大，所以选用了互斥锁。
     */
    RWSpinLock _lock;

    /*! \brief  获取写锁 */
    void lock() {
        _lock.lock();
    }

    /*! \brief  释放写锁 */
    void unlock() {
        _lock.unlock();
    }

    /*! \brief  获取读锁 */
    void lock_shared() {
        _lock.lock_shared();
    }

    /*! \brief  释放读锁 */
    void unlock_shared() {
        _lock.unlock_shared();
    }

    /*! \brief  升级 */
    void upgrade() {
        _lock.upgrade();
    }

    /*! \brief  降级 */
    void downgrade() {
        _lock.downgrade();
    }

};

struct ShardMemoryState {
    bool reshard = true;
    std::unordered_map<void*, size_t> current;
    std::unordered_map<void*, size_t> rehashed;
    std::unordered_map<void*, size_t> rehashing;
};

struct ShardDataMeta {
    bool on_dcpmm;  // 此 shard 是从 dcpmm 中恢复的
};

/*! \brief ShardStorage 类型 */
class ShardStorage : public Storage {
public:
    typedef std::unordered_map<int32_t, std::unique_ptr<ShardData>> shard_table_t;
    typedef std::unordered_map<int32_t, std::unique_ptr<ShardDataMeta>> shard_table_meta_t;

    virtual ~ShardStorage();

    ShardStorage();

    ShardStorage(const ShardStorage&) = delete;
    ShardStorage(ShardStorage&&) = delete;

    ShardStorage& operator=(const ShardStorage&) = delete;
    ShardStorage& operator=(ShardStorage&&) = delete;

    /*!
     * \brief  不加锁地访问某一个 data shard
     * \param  index shard 的下标
     * \param  func 访问 shard data 的只读函数
     */
    void read_shard_nonlocked(int index, std::function<void(const boost::any&)> func);

    /*!
     * \brief  申请读锁后，访问某一个 data shard
     * \param  index shard 的下标
     * \param  func 访问 shard data 的只读函数
     */
    void read_shard(int index, std::function<void(const boost::any&)> func);

    /*!
     * \brief  申请写锁后，访问某一个 data shard
     * \param  index shard 的下标
     * \param  func 访问 shard data 的函数
     */
    void write_shard(int index, std::function<void(boost::any&)> func);

    size_t shard_num();

    std::vector<int32_t> shard_list();

    virtual bool create_shard(int32_t shard_id) = 0;

    virtual bool erase_shard(int32_t shard_id);

    bool exist_shard(int32_t shard_id);

    /*!
     * \brief  返回 shard_id 对应的 shard 的元素数目
     */
    virtual size_t shard_size(int32_t shard_id) = 0;

    /*!
     * \brief  返回 shard_id 对应的 shard 的元素内存占用统计
     */
    virtual size_t shard_memory_usage(int32_t shard_id) = 0;

    // unthread safe!
    /*!
     * \brief  返回 index  所对应的 ShardData
     * \param  index shard 的下标
     */
    ShardData* get(int index);

    /*! thread safe
     * \brief 返回 index 所对应的 ShardData, 若没有则创建
     */ 
    ShardData* operator[](int index) {
        core::lock_guard<RWSpinLock> lk(_mtx);
        auto& shard = _shards[index];
        if (shard.get() == nullptr) {
            shard = std::make_unique<ShardData>();
        }
        return shard.get();
    }

    shard_table_t& data() {
        return _shards;
    }

    void lock_shared() {
        _mtx.lock_shared();
    }

    void unlock_shared() {
        _mtx.unlock_shared();
    }

   shard_table_meta_t* shards_meta() {
       return &_shards_meta;
   }

    virtual ShardIterator* get_shard_iterator(int32_t, int32_t) {
        LOG(FATAL) << "No implement";
        return nullptr;
    }

    void delete_shard_iterator(int32_t shard_id, int32_t iterator_id);

    virtual bool need_remote_restore() override {
        bool ret = false;
        for (auto &shard_meta : _shards_meta) {
            if (!shard_meta.second->on_dcpmm) {
                SLOG(INFO) << "Shard " << shard_meta.first << " cannot restored locally";
                ret = true;
            }
        }
        return ret;
    }

protected:
    shard_table_t _shards;
    shard_table_meta_t _shards_meta;
    RWSpinLock _mtx;
    ShardMemoryState _mem;
    std::unordered_map<int32_t, std::unique_ptr<ShardIterator>> _shard_iterators;

    friend class ShardStorageMemory;
};


class ShardStorageMemory {
public:
    static ShardStorageMemory& singleton();
    void initialize(size_t soft_limit, size_t hard_limit);
    void set_healthy(bool);
    bool test_healthy();
    bool check_healthy();
    void watch_memory(size_t size);
    void* alloc(size_t n, ShardStorage* st);
    void free(void* p, ShardStorage* st);
    std::shared_ptr<void> reserve(size_t n);
    

private:
    size_t stable_memory();
    bool try_get_memory(std::unique_lock<std::mutex>& lock, size_t n);
    void print_state();
    void throw_bad_alloc();
    
    std::mutex _mutex;
    boost::detail::spinlock _lock;
    std::set<ShardStorage*> _storages;
    size_t _soft_limit = LONG_LONG_MAX;
    size_t _hard_limit = LONG_LONG_MAX;
    size_t _reserved = 0;
    size_t _inserted = 0;
    bool _initialized = false;
    std::atomic<bool> _healthy = {true};
};


// 不能对使用StorageAllocator的shard进行复制或移动！
// 因为shard已与Storage绑定，allocator需要用到parent storage的信息
template <class T>
class ShardStorageAllocator : public std::allocator<T> {
public:
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;
    typedef T value_type;
    template <class U>
    struct rebind {
        typedef ShardStorageAllocator<U> other;
    };

    ShardStorageAllocator(ShardStorage* st): _st(st) {}

    ShardStorageAllocator(const ShardStorageAllocator& other): std::allocator<T>(other), _st(other._st) {}

    template <class U>
    ShardStorageAllocator(const ShardStorageAllocator<U>& other): std::allocator<T>(other), _st(other._st) {}

    ~ShardStorageAllocator() {}

    pointer allocate(size_type num, const void* /*hint*/ = 0) {
        return static_cast<pointer>(
              ShardStorageMemory::singleton().alloc(num * sizeof(T), _st));
    }
    
    void deallocate(pointer p, size_type /*num*/) {
        ShardStorageMemory::singleton().free(p, _st);
    }

    ShardStorage* _st;
};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_STORAGE_STORAGE_H
