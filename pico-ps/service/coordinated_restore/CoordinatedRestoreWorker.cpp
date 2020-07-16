#include "pico-ps/service/coordinated_restore/CoordinatedRestoreWorker.h"
#include "pico-ps/common/DistributedAsyncReturn.h"
#include "pico-ps/operator/RestoreOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

void CoordinatedRestoreWorker::start() {
    _th = std::make_unique<std::thread>(&CoordinatedRestoreWorker::_run, this);
}

void CoordinatedRestoreWorker::join() {
    _th->join();
}

void CoordinatedRestoreWorker::_run() {
    bool has_next = true;
    do {
        int32_t shard_id;
        has_next = _progress->next_shard(&shard_id);
        if (!has_next) {
            return;
        }
        _process_shard(shard_id);
    } while (has_next);
    _s2s_client.reset();
}

void CoordinatedRestoreWorker::_process_shard(int32_t shard_id) {
    int32_t node_id = _progress->target_node(shard_id);
    KVShardCoordinatedRestoreRequestItem req_item(node_id, _storage_id, -1, 0, _batch_size, shard_id);
    CoordinatedRestoreResponseItem resp_item;
    bool shard_finished = false;
    SLOG(INFO) << "Restoring storage_id: " << _storage_id << ", shard_id: " << shard_id << ", coordinated by node: " << node_id;
    while (!shard_finished && _progress->running()) {
        CoordinateRestoreDistributedAsyncReturn async_ret(_op, _st, _s2s_client.get());
        auto status = async_ret.RestoreBatch(&req_item, &resp_item);
        if (!status.ok()) {
            _progress->set_no_replica();
        }
        req_item.iterator_id = resp_item.iterator_id;
        req_item.offset = resp_item.next_offset;
        shard_finished = resp_item.finished;
    }
}

} // namespace ps
} // namespace pico
} // namespace paradigm4