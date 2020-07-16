
#ifndef PARADIGM4_PICO_PS_COMMON_THREAD_SYNC_H
#define PARADIGM4_PICO_PS_COMMON_THREAD_SYNC_H

#include <cstdint>

#include <memory>
#include <vector>

#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

constexpr uint32_t GROUP_SIZE = 32;

class ThreadReduce {
    static constexpr uint64_t ONE = 1ull << GROUP_SIZE;
public:
    ThreadReduce(uint32_t max_sync_num): _max_sync_num(max_sync_num) {}
    void add(uint32_t tid) {
        if (_max_sync_num == 1) {
            return;
        }
        _flags.fetch_or(ONE << tid, std::memory_order_release);
    }
    //其实还是有可能超过max_sync_num
    uint32_t reduce(uint32_t tid, size_t sync_num /* = GROUP_SIZE */) {
        if (_max_sync_num == 1) {
            return 1u << tid;
        }
        if (sync_num > _max_sync_num) {
            sync_num = _max_sync_num;
        }
        uint64_t x = (ONE + 1u) << tid;
        uint64_t flags = _flags.fetch_xor(x, std::memory_order_acq_rel) ^ x;
        uint64_t running = flags >> GROUP_SIZE;
        uint64_t finished = flags & (ONE - 1u);
        if (__builtin_popcount(finished) >= sync_num || running == 0) {
            uint64_t getted = _flags.fetch_and(~finished, std::memory_order_acq_rel);
            return static_cast<uint32_t>(getted & finished);
        } else {
            return 0;
        }
    }

private:
    std::atomic<uint64_t> _flags = {0};
    char _pad1[64 - sizeof(std::atomic<uint64_t>)];
    uint32_t _max_sync_num = 1;
};

class ThreadBcast {
public:
    ThreadBcast(uint32_t max_sync_num): _max_sync_num(max_sync_num) {
        if (_max_sync_num != 1) {
            for (uint32_t i = 0; i < GROUP_SIZE; ++i) {
                _fd[i] = eventfd(0, EFD_SEMAPHORE);
                PSCHECK(_fd[i] >= 0) << "no fd";
            }            
        }
    }
    ~ThreadBcast() {
        if (_max_sync_num != 1) {
            for (uint32_t i = 0; i < GROUP_SIZE; ++i) {
                ::close(_fd[i]);
            }
        }
    }
    void bcast(uint32_t tid, uint32_t flags) {
        if (1u << tid == flags) {
            return;
        }
        int64_t _ = 1;
        if (flags == 0) {
            PSCHECK(::read(_fd[tid], &_, sizeof(int64_t)) == sizeof(int64_t));
        } else {
            for (uint32_t i = 0; i < GROUP_SIZE; ++i) {
                if (i != tid && (flags & (1u << i))) {
                    PSCHECK(::write(_fd[i], &_, sizeof(int64_t)) == sizeof(int64_t));
                }
            }
        }
    }
private:
    int _fd[GROUP_SIZE];
    uint32_t _max_sync_num = 1;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_HANDLER_H

