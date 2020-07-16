#include "pico-ps/service/coordinated_restore/CoordinatedRestoreProgress.h"

namespace paradigm4 {
namespace pico {
namespace ps {

void CoordinatedRestoreProgress::prepare() {
    for (auto &shard : _td->shards) {
        if (shard.second.size() == 0) {
            _no_replica = true;
        }
        _pending_shards.push(shard.first);
    }
    if (!_no_replica && _pending_shards.size() > 0) {
        _running = true;
        _selected_nodes = _td->runtime_info->pick_one_replica(PickAlgo::RANDOM);
    }
}

int32_t CoordinatedRestoreProgress::target_node(int32_t shard_id) {
    if (shard_id >= 0 && size_t(shard_id) < _selected_nodes.size() && _running) {
        return _selected_nodes[shard_id];
    } else {
        return -1;
    }
}

bool CoordinatedRestoreProgress::no_replica() const {
    std::unique_lock<std::mutex> lck(_mu);
    return _no_replica;
}

bool CoordinatedRestoreProgress::next_shard(int32_t *shard_id) {
    std::unique_lock<std::mutex> lck(_mu);
    if (_pending_shards.size() == 0 || !_running) {
        return false;
    }
    *shard_id = _pending_shards.front();
    _pending_shards.pop();
    return true;
}

void CoordinatedRestoreProgress::set_shard_complete(int32_t shard_id) {
    std::unique_lock<std::mutex> lck(_mu);
    _finished_shards.push_back(shard_id);
}

void CoordinatedRestoreProgress::set_no_replica() {
    std::unique_lock<std::mutex> lck(_mu);
    _no_replica = true;
    _running = false;
}

} // namespace ps
} // namespace pico
} // namespace paradigm4