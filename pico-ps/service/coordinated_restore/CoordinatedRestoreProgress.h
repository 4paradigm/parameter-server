#ifndef PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_PROGRESS_H
#define PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_PROGRESS_H

#include <atomic>
#include <mutex>
#include <queue>

#include "pico-ps/service/TableDescriptor.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class CoordinatedRestoreProgress {
public:
    CoordinatedRestoreProgress(int32_t storage_id, TableDescriptor *td) : _storage_id(storage_id), _td(td) {}

    virtual ~CoordinatedRestoreProgress() {}

    void prepare();

    int32_t target_node(int32_t shard_id);

    int32_t storage_id() const {
        return _storage_id;
    }

    bool no_replica() const;

    bool next_shard(int32_t *shard_id);

    void set_shard_complete(int32_t shard_id);

    bool running() const { return _running; }

    void set_no_replica();

    TableDescriptor *td() { return _td; }

private:
    int32_t _storage_id;
    TableDescriptor *_td;

    bool _no_replica = false;
    std::queue<int32_t> _pending_shards;
    std::vector<int32_t> _unfinished_shards;
    std::vector<int32_t> _finished_shards;
    std::vector<int32_t> _selected_nodes;
    std::atomic<bool> _running;
    mutable std::mutex _mu;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_PROGRESS_H
