#include "pico-ps/common/defs.h"

#include "pico-ps/service/coordinated_restore/CoordinatedRestoreController.h"

namespace paradigm4 {
namespace pico {
namespace ps {

void CoordinatedRestoreController::start() {
    _progress = std::make_unique<CoordinatedRestoreProgress>(_storage_id, _td);
    _progress->prepare();
    if (!_progress->no_replica()) {
        for (size_t i = 0; i < _thread_num; ++i) {
            _workers.emplace_back(
                std::make_unique<CoordinatedRestoreWorker>(_storage_id, _batch_size, _progress.get(), _rpc));
            _workers.back()->start();
        }
    }
}

void CoordinatedRestoreController::join() {
    if (_progress->running()) {
        for (auto &worker : _workers) {
            worker->join();
        }
    }
}

} // namespace ps
} // namespace pico
} // namespace paradigm4