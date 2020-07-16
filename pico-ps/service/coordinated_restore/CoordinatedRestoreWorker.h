#ifndef PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_WORKER_H
#define PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_WORKER_H

#include <memory>
#include <thread>

#include "pico-ps/common/defs.h"
#include "pico-ps/operator/RestoreOperator.h"
#include "pico-ps/operator/StorageOperator.h"
#include "pico-ps/service/TableDescriptor.h"
#include "pico-ps/service/coordinated_restore/CoordinatedRestoreProgress.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class CoordinatedRestoreWorker {
public:
    CoordinatedRestoreWorker(int32_t storage_id, size_t batch_size, CoordinatedRestoreProgress *progress,
                             RpcService *rpc_service)
        : _storage_id(storage_id), _batch_size(batch_size), _progress(progress) {
        _td = _progress->td();
        _op = static_cast<RestoreOperator *>(static_cast<StorageOperator *>(_td->storage_op.get())->restore_op());
        _st = _td->storage.get();
        _s2s_client = rpc_service->create_client(PSERVER_S2S_RPC_NAME);
    }

    virtual ~CoordinatedRestoreWorker() {}

    void start();

    void join();

private:
    void _run();
    void _process_shard(int32_t shard_id);

    int32_t _storage_id;
    size_t _batch_size;
    CoordinatedRestoreProgress *_progress;
    std::unique_ptr<RpcClient> _s2s_client;
    TableDescriptor *_td;
    std::unique_ptr<std::thread> _th;
    size_t batch_size;
    RestoreOperator *_op;
    Storage *_st;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_SERVICE_COORDINATED_RESTORE_COORDINATED_RESTORE_WORKER_H
