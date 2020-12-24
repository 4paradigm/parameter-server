#ifndef PARADIGM4_PICO_PS_COMMON_DISTRIBUTED_ASYNC_RETURN_H
#define PARADIGM4_PICO_PS_COMMON_DISTRIBUTED_ASYNC_RETURN_H

#include <cstdint>

#include <memory>
#include <vector>

#include "pico-ps/common/core.h"

#include "pico-ps/common/Status.h"
#include "pico-ps/common/defs.h"
#include "pico-ps/common/message.h"
#include "pico-ps/operator/PushOperator.h"
#include "pico-ps/operator/StorageOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {


class DistributedAsyncReturn : public Object {
public:
    explicit DistributedAsyncReturn(RpcClient* rpc_client);
    
    virtual ~DistributedAsyncReturn() {}

    virtual Status wait();

protected:
    virtual void send(std::vector<PSRequest>&& reqs,
        const PSMessageMeta& meta,
        int timeout = -1);

    virtual void send_one_way(std::vector<PSRequest>&& reqs,
        const PSMessageMeta& meta);

    virtual Status check_resp_validity(PSResponse& response, PSMessageMeta& meta);
    
    virtual Status check_rpc_error(PSResponse& resp);
    
    virtual Status apply_response(PSResponse&, PSMessageMeta&) = 0;

    virtual void reset_dealer();

    Status _send_status;
    bool _busy = false;
    size_t _req_num = 0;
    int _timeout = -1;
    PSMessageMeta _meta;

    RpcClient* _rpc_client;
    std::shared_ptr<Dealer> _dealer;
};

class DefaultDistributedAsyncReturn : public DistributedAsyncReturn {
public:
    // DefaultDistributedAsyncReturn has the ownership of req_msgs
    DefaultDistributedAsyncReturn(RpcClient* rpc_client)
        : DistributedAsyncReturn(rpc_client) {}

    Status default_sync_rpc(std::vector<PSRequest>&& reqs,
          const PSMessageMeta& meta,
          int timeout = -1) {
        send(std::move(reqs), meta, timeout);
        return wait();
    }

    void async_send_one_way(std::vector<PSRequest>&& reqs,
          const PSMessageMeta& meta) {
        send_one_way(std::move(reqs), meta);
    }

protected:
    Status apply_response(PSResponse& resp, PSMessageMeta&) override {
        Status status;
        resp >> status;
        SCHECK(status.ok());
        return Status();
    }
};

class HealthCheckDistributedAsyncReturn : public DefaultDistributedAsyncReturn {
public:
    HealthCheckDistributedAsyncReturn(RpcClient* rpc_client)
        : DefaultDistributedAsyncReturn(rpc_client) {}

    Status health_check(std::vector<int> node_ids, int timeout = -1) {
        std::vector<PSRequest> reqs;
        for (auto& node_id : node_ids) {
            SLOG(INFO) << "Will check heath of server with node id: " << node_id;
            reqs.emplace_back(node_id);
        }
        return default_sync_rpc(std::move(reqs), {0, 0, -1, -1, RequestType::HEALTH_CHECK}, timeout);
    }

protected:
    Status apply_response(PSResponse& resp, PSMessageMeta&) override {
        Status status;
        int32_t node_id;
        resp >> status >> node_id;
        SLOG(INFO) << "Heatch check " << node_id << " OK";
        return Status();
    }

};

class StorageDistributedAsyncReturn : public DistributedAsyncReturn {
public:
    StorageDistributedAsyncReturn(StorageOperator* op, RpcClient* rpc_client)
        : DistributedAsyncReturn(rpc_client), _op(op) {}


    Status query_storage_info(std::vector<StorageStatisticInfo>* storage_stat,
          RuntimeInfo& rt,
          const PSMessageMeta& meta,
          int timeout = -1) {
        std::vector<PSRequest> reqs;
        _op->generate_query_info_request(rt, reqs);
        send(std::move(reqs), meta, timeout);
        _storage_stat = storage_stat;
        return wait();
    }

protected:
    Status apply_response(PSResponse& resp, PSMessageMeta&)override {
        _op->apply_query_info_response(resp, *_storage_stat);
        return Status();
    }

    StorageOperator* _op;
    std::vector<StorageStatisticInfo>* _storage_stat = nullptr;
};

class MemoryDistributedAsyncReturn : public DistributedAsyncReturn {
public:
    MemoryDistributedAsyncReturn(RpcClient* rpc_client)
        : DistributedAsyncReturn(rpc_client) {} 


    Status query_memory_info(int node_id, NodeMemoryInfo& memory_info, int timeout = -1) {
        std::vector<PSRequest> reqs;
        reqs.emplace_back(node_id);
        send(std::move(reqs), {0, 0, -1, -1, RequestType::MEMORY_INFO}, timeout);
        _memory_info = &memory_info;
        return wait();
    }

protected:
    virtual Status apply_response(PSResponse& resp, PSMessageMeta&)override {
        resp >> *_memory_info;
        return Status();
    }
    StorageOperator* _op;
    NodeMemoryInfo* _memory_info;
};

class DirectPushDistributedAsyncReturn : public DistributedAsyncReturn {
public:
    // PushDistributedAsyncReturn has the ownership of PushItems,
    // shares the owership of PushOperator with PushHandler.
    // Dealer should be s2s dealer
    DirectPushDistributedAsyncReturn(PushOperator* op,
          RpcClient* rpc_client): DistributedAsyncReturn(rpc_client), _op(op) {}

    Status push(core::vector<std::unique_ptr<PushItems>>&& push_items, 
          RuntimeInfo& rt,
          const PSMessageMeta& meta,
          int timeout = -1);

protected:
    virtual Status apply_response(PSResponse&, PSMessageMeta&)override;

    PushOperator* _op;
    core::vector<std::unique_ptr<PushItems>> _push_items;
    std::unique_ptr<PushRequestData> _data;
    std::vector<PushRequestData*> _request_data;
};

class CoordinateRestoreDistributedAsyncReturn : public DistributedAsyncReturn {
public:
    CoordinateRestoreDistributedAsyncReturn(RestoreOperator* op, Storage* st, RpcClient* rpc_client)
        : DistributedAsyncReturn(rpc_client), _st(st), _op(op) {}

    Status RestoreBatch(CoordinatedRestoreRequestItem* req_item, CoordinatedRestoreResponseItem* resp_item,
                        int timeout = -1);

protected:
    virtual Status apply_response(PSResponse&, PSMessageMeta&) override;
    Storage* _st;
    RestoreOperator* _op;
    CoordinatedRestoreResponseItem* _resp_item;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_COMMON_DISTRIBUTED_ASYNC_RETURN_H
