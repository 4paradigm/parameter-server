
#ifndef PARADIGM4_PICO_PS_COMMON_EASY_HASH_MAP_H
#define PARADIGM4_PICO_PS_COMMON_EASY_HASH_MAP_H

#include "pico-ps/common/core.h"
#include <unordered_map>

namespace paradigm4 {
namespace pico {

//很简单的hash map，功能很少，但是具有极高的性能，KEY最好是简单类型
template<
    class KEY, 
    class T, 
    class HASH = std::hash<KEY>,
    class KEY_EQUAL = std::equal_to<KEY>,
    class ALLOCATOR = PicoAllocator<std::pair<KEY, T>>>
class EasyHashMap {
    static constexpr int FACTOR = 2;
public:
    typedef KEY key_type;
    typedef T mapped_type;
    typedef std::pair<KEY, T> bucket_type;
    typedef std::pair<const KEY, T> value_type;
    typedef size_t size_type;
    typedef HASH hasher;
    typedef KEY_EQUAL key_equal;
    typedef typename ALLOCATOR::template rebind<std::pair<key_type, mapped_type> >::other allocator_type;

    class iterator {
    public:
        iterator() {}
        explicit iterator(key_type empty, value_type* ptr, value_type* end)
              : _ptr(ptr), _end(end), _empty(empty) {}
        value_type& operator*()const {
            return *_ptr;
        }

        value_type* operator->()const {
            return _ptr;
        }

        iterator& operator++() {
            ++_ptr;
            while (_ptr != _end && _key_equal(_ptr->first, _empty)) {
                ++_ptr;
            }
            return *this;
        }

        iterator operator++(int) {
            iterator it(*this);
            ++(*this);
            return it;
        }

        bool operator==(const iterator& it) const {
            return _ptr == it._ptr;
        }

        bool operator!=(const iterator& it) const {
            return _ptr != it._ptr;
        }
    private:
        value_type* _ptr = nullptr;
        value_type* _end = nullptr;
        key_type _empty;
        key_equal _key_equal;
    };

    EasyHashMap(const key_type& empty_key, size_type n = 0) {
        _empty = empty_key;
        if (n < 8) {
            n = 8;
        }
        int n2 = 0;
        while ((size_type(1) << n2) < n * FACTOR) {
            ++n2;
        }
        _size = 0;
        _max_size = (size_type(1) << n2) / FACTOR;
        _bucket_count = (size_type(1) << n2) + _max_size + 1;
        _buckets = std::unique_ptr<bucket_type[], Deleter>(
              _allocator.allocate(_bucket_count), Deleter(_empty, _bucket_count));
        _mask = (size_type(1) << n2) - 1;
        _empty = empty_key;
        for (size_type i = 0; i < _bucket_count; ++i) {
            _allocator.construct(&_buckets[i].first, _empty);
        }
    }

    size_type size() {
        return _size;
    }
    size_type bucket_count() {
        return _bucket_count;
    }
    value_type* buckets() {
        return reinterpret_cast<value_type*>(_buckets.get());
    }

    void reserve(size_type size) {
        if (size > _max_size) {
            EasyHashMap tmp(_empty, size);
            for (size_type i = 0; i < _bucket_count; ++i) {
                if (!_key_equal(_buckets[i].first, _empty)) {
                    tmp.force_emplace(_buckets[i].first, std::move(_buckets[i].second));
                }
            }
            std::iter_swap(this, &tmp);
        }
    }

    //认为下次使用时_size会和这次差不多
    void clear() {
        if (_size < _max_size / 4) {
            EasyHashMap tmp(_empty, _size);
            std::iter_swap(this, &tmp);
        } else {
            _size = 0;
            for (size_type i = 0; i < _bucket_count; ++i) {
                if (!_key_equal(_buckets[i].first, _empty)) {
                    _buckets[i].first = _empty;
                    _allocator.destroy(&_buckets[i].second);
                }
            }
        }
    }

    //无论是否重复都会插入
    template<class... ARGS>
    iterator force_emplace(const key_type& key, ARGS&&... args) {
        reserve(_size + 1);
        size_type np = 0;
        size_type p = _hash(key) & _mask;
        while (!_key_equal(_buckets[p].first, _empty)) {
            prob(p, np);
        }
        ++_size;
        _buckets[p].first = key;
        _allocator.construct(&_buckets[p].second, std::forward<ARGS>(args)...);
        return iter(p);
    }

    template<class... ARGS>
    std::pair<iterator, bool> try_emplace(const key_type& key, ARGS&&... args) {
        size_type np = 0;
        size_type p = _hash(key) & _mask;
        while (!_key_equal(_buckets[p].first, _empty)) {
            if (_key_equal(_buckets[p].first, key)) {
                return {iter(p), false};
            }
            prob(p, np);
        }
        if (_size < _max_size) {
            ++_size;
            _buckets[p].first = key;
            _allocator.construct(&_buckets[p].second, std::forward<ARGS>(args)...);
            return {iter(p), true};
        }
        reserve(_size + 1);
        return try_emplace(key, std::forward<ARGS>(args)...);
    }

    //must exist
    T& at(const key_type& key) {
        size_type np = 0;
        size_type p = _hash(key) & _mask;
        while (!_key_equal(_buckets[p].first, key)) {
            prob(p, np);
        }
        return _buckets[p].second;
    }

    iterator begin() {
        size_type p = 0;
        while (p <= _mask && _key_equal(_buckets[p].first, _empty)) {
            ++p;
        }
        return iter(p);
    }

    iterator end() {
        return iter(_mask + 1);
    }

    iterator find(const key_type& key) {
        size_type np = 0;
        size_type p = _hash(key) & _mask;
        while (!_key_equal(_buckets[p].first, _empty)) {
            if (_key_equal(_buckets[p].first, key)) {
                return iter(p);
            }
            prob(p, np);
        }
        return end();
    }

    size_type count(const key_type& key) {
        return find(key) != end();
    }

private:
    void prob(size_type& p, size_type& np) {
        ++np;
        p = (p + np) & _mask;
    }
    iterator iter(size_type p) {
        return iterator(_empty,
              reinterpret_cast<value_type*>(&_buckets[p]),
              reinterpret_cast<value_type*>(&_buckets[_mask + 1]));
    }
    class Deleter {
    public:
        Deleter() = default;
        Deleter(const key_type& empty, size_type bucket_count)
              : _empty(empty), _bucket_count(bucket_count) {}
        void operator()(bucket_type* p) {
            if (p == nullptr) {
                return;
            }
            for (size_type i = 0; i < _bucket_count; ++i) {
                if (!_key_equal(p[i].first, _empty)) {
                    _allocator.destroy(&p[i].second);
                }
                _allocator.destroy(&p[i].first);
            }
            _allocator.deallocate(p, _bucket_count);
        }
    private:
        key_type _empty;
        size_type _bucket_count;
        allocator_type _allocator;
        key_equal _key_equal;
    };
    allocator_type _allocator;
    hasher _hash;
    key_equal _key_equal;

    size_type _size = 0;
    size_type _max_size = 0;
    size_type _bucket_count = 0;
    std::unique_ptr<bucket_type[], Deleter> _buckets;

    size_type _mask = 0;
    key_type _empty;
};

}
}

#endif