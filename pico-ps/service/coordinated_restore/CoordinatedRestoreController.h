#ifndef PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_CONTROLLER_H
#define PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_CONTROLLER_H

#include <deque>
#include <memory>

#include "pico-ps/service/TableDescriptor.h"
#include "pico-ps/service/coordinated_restore/CoordinatedRestoreProgress.h"
#include "pico-ps/service/coordinated_restore/CoordinatedRestoreWorker.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class CoordinatedRestoreController {
public:
    CoordinatedRestoreController(int32_t storage_id, size_t batch_size, size_t thread_num, RpcService *rpc,
                                 TableDescriptor *td)
        : _storage_id(storage_id), _batch_size(batch_size), _thread_num(thread_num), _rpc(rpc), _td(td) {}

    virtual ~CoordinatedRestoreController() {}

    bool completed() { return !_progress->no_replica(); }

    void start();

    void join();

private:
    int32_t _storage_id;
    size_t _batch_size;
    size_t _thread_num;
    RpcService *_rpc;
    TableDescriptor *_td;
    std::unique_ptr<CoordinatedRestoreProgress> _progress;
    std::vector<std::unique_ptr<CoordinatedRestoreWorker>> _workers;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_CONTROLLER_H
