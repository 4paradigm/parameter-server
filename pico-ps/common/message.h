#ifndef PARADIGM4_PICO_PS_COMMON_MESSAGE_H
#define PARADIGM4_PICO_PS_COMMON_MESSAGE_H

#include <cstdint>

#include "pico-ps/common/core.h"

#include "pico-ps/service/Context.h"
#include "pico-ps/common/common.h"

namespace paradigm4 {
namespace pico {
namespace ps {

enum class RequestType : int8_t {
    STORAGE_CREATE = 0,
    STORAGE_DELETE = 1,
    STORAGE_CLEAR = 2,
    STORAGE_STATISTIC = 3,
    HANDLER_REGISTER = 4,
    HANDLER_DEREGISTER = 5,
    OP_FOREACH = 6,
    OP_ERASEIF = 7,
    OP_DUMP = 8,
    OP_LOAD = 9,
    OP_PULL = 10,
    OP_PUSH_ASYNC = 11,
    OP_PUSH_SYNC = 12,
    OP_STORE = 13,
    OP_LOAD_LIBRARY = 14,
    EXIT = 15,
    NEW_CTX_CREATE_SHARD = 16,
    NEW_CTX_SHUFFLE = 17,
    NEW_CTX_STORE = 18,
    NEW_CTX_PURGE = 19,
    CTX_UPDATE = 20,
    OP_SYNC = 21,
    MEMORY_INFO = 22,
    OP_COORDINATED_RESTORE_ITERATE = 23,
    HEALTH_CHECK = 24,
};
PICO_ENUM_SERIALIZATION(RequestType, int32_t);

struct PSMessageMeta {
    int32_t sid;
    int32_t hid;
    int32_t ctx_ver;
    int32_t ctx_u_ver;
    RequestType req_type;
};

template <class AR>
inline Archive<AR>& operator<<(Archive<AR>& ar, const PSMessageMeta& meta) {
    ar.write_raw(&meta, sizeof(meta));
    return ar;
}

template <class AR>
inline Archive<AR>& operator>>(Archive<AR>& ar, PSMessageMeta& meta) {
    ar.read_raw(&meta, sizeof(PSMessageMeta));
    return ar;
}

class PSRequest {
public:
    PSRequest() = default;

    PSRequest(int node_id, size_t reserve_size = 0) {
        _rpc_request.head().sid = node_id;
        _rpc_request.archive().prepare_write(reserve_size);
    }

    PSRequest(const RpcRequest& rpc_request) = delete;

    PSRequest(RpcRequest&& rpc_request)
        : _rpc_request(std::move(rpc_request)) {}

    PSRequest(PSRequest&& rhs) {
        *this = std::move(rhs);
    }

    PSRequest& operator=(PSRequest&& rhs) {
        _rpc_request = std::move(rhs._rpc_request);
        return *this;
    }

    RpcRequest& rpc_request() {
        return _rpc_request;
    }

    template <class T>
    PSRequest& operator>>(T& val) {
        _rpc_request >> val;
        return *this;
    }

    template <class T>
    PSRequest& operator<<(const T& val) {
        _rpc_request << val;
        return *this;
    }

    BinaryArchive& archive() {
        return _rpc_request.archive();
    }

    LazyArchive& lazy() {
        return _rpc_request.lazy();
    }
private:
    RpcRequest _rpc_request;
};

class PSResponse {
public:
    PSResponse() = default;

    explicit PSResponse(PSRequest& req, size_t reserve_size = 0)
        : _rpc_response(req.rpc_request()) {
        _rpc_response.archive().prepare_write(reserve_size);
    }

    PSResponse(RpcResponse&& rpc_response) : _rpc_response(std::move(rpc_response)) {}

    PSResponse(const PSResponse&) = delete;

    PSResponse(PSResponse&& rhs) {
        *this = std::move(rhs);
    }

    PSResponse& operator=(PSResponse&& rhs) {
        _rpc_response = std::move(rhs._rpc_response);
        return *this;
    }

    PSResponse& operator=(const PSResponse&) = delete;

    RpcResponse& rpc_response() {
        return _rpc_response;
    }

    template <class T>
    PSResponse& operator>>(T& val) {
        _rpc_response >> val;
        return *this;
    }

    template <class T>
    PSResponse& operator<<(const T& val) {
        _rpc_response << val;
        return *this;
    }

    BinaryArchive& archive() {
        return _rpc_response.archive();
    }
    LazyArchive& lazy() {
        return _rpc_response.lazy();
    }
private:
    RpcResponse _rpc_response;
};

template<class T>
struct ps_trivially_copyable {
    static constexpr bool value = std::is_trivially_copyable<T>::value;
};

template<class T1, class T2>
struct ps_trivially_copyable<std::pair<T1, T2>> {
    static constexpr bool value = std::is_trivially_copyable<T1>::value && 
          std::is_trivially_copyable<T2>::value;
};


template<class T>
std::enable_if_t<ps_trivially_copyable<T>::value>
ps_serialize(BinaryArchive& ar, const T& value) {
    ar.prepare_write(sizeof(value));
#if defined(__GNUC__) && __GNUC__ >= 8
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
    memcpy(ar.end(), &value, sizeof(value));
#if defined(__GNUC__) && __GNUC__ >= 8
    #pragma GCC diagnostic pop
#endif
    ar.advance_end(sizeof(value));
}

template<class T>
std::enable_if_t<ps_trivially_copyable<T>::value>
ps_deserialize(BinaryArchive& ar, T& value) {
#if defined(__GNUC__) && __GNUC__ >= 8
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
    memcpy(&value, ar.cursor(), sizeof(value));
#if defined(__GNUC__) && __GNUC__ >= 8
    #pragma GCC diagnostic pop
#endif
    ar.advance_cursor(sizeof(value));
}

template<class T>
std::enable_if_t<!ps_trivially_copyable<T>::value>
ps_serialize(BinaryArchive& ar, const T& value) {
    ar << value;
}

template<class T>
std::enable_if_t<!ps_trivially_copyable<T>::value>
ps_deserialize(BinaryArchive& ar, T& value) {
    ar >> value;
}

inline void ps_deserialize(LazyArchive& lazy, CompressInfo& compress_info, BinaryArchive& msg_ar) {
    if (compress_info._enabled) {
        BinaryArchive compressed_ar;
        lazy >> compressed_ar;
        compress_info._compresser.raw_uncompress(compressed_ar, msg_ar);
    } else {
        lazy >> msg_ar;
    }
}

inline void ps_serialize(LazyArchive& lazy, CompressInfo& compress_info, BinaryArchive&& msg_ar) {
    if (compress_info._enabled) {
        BinaryArchive compressed_ar(true);
        compress_info._compresser.raw_compress(msg_ar, compressed_ar);
        lazy << std::move(compressed_ar);
    } else {
        lazy << std::move(msg_ar);
    }
}

template <class T>
class PSAllocator : public std::allocator<T> {
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
        typedef PSAllocator<U> other;
    };

    PSAllocator() : std::allocator<T>() {}

    PSAllocator(const PSAllocator& other) : std::allocator<T>(other) {}

    template <class U>
    PSAllocator(const PSAllocator<U>& other)
        : std::allocator<T>(other) {}

    ~PSAllocator() {}

    pointer allocate(size_type num, const void* /*hint*/ = 0) {
        RpcAllocator<char> allocator;
        return (pointer)allocator.allocate(num * sizeof(T));
    }

    void deallocate(pointer p, size_type num) {
        RpcAllocator<char> allocator;
        allocator.deallocate((char*)p, num * sizeof(T));
    }
};


template<class T> using RpcVector = std::vector<T, PSAllocator<T> >;

template<class T> 
std::enable_if_t<ps_trivially_copyable<T>::value, BinaryArchive>
vector_rpc_view(RpcVector<T>& vec) {
    BinaryArchive ar;
    ar.set_read_buffer(reinterpret_cast<char*>(vec.data()), vec.size() * sizeof(vec[0]), [](void*){});
    return ar;
}

template<class T> 
std::enable_if_t<!ps_trivially_copyable<T>::value, BinaryArchive>
vector_rpc_view(RpcVector<T>& vec) {
    BinaryArchive ar(true);
    for (auto& x: vec) {
        ar << x;
    }
    return ar;
}

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_COMMON_MESSAGE_H
