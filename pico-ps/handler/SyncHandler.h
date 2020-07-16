#ifndef PARADIGM4_PICO_PS_HANDLER_SYCN_HANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_SYCN_HANDLER_H

#include "pico-ps/handler/Handler.h"
#include "pico-ps/common/common.h"
#include "pico-ps/operator/SyncOperator.h"
#include "pico-ps/operator/PushOperator.h"
#include "pico-ps/model/Model.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class SyncHandler : public Handler {
public:
    SyncHandler(Client* client,
            const std::string& model_name,
            const std::string& table_name,
            const std::string& op_key) :
        Handler(-1, -1, nullptr, client) {
        _model_name = model_name;
        _table_name = table_name;
        _op_key = op_key;

        init_table();
    }

    using Handler::Handler;

    virtual ~SyncHandler() {}

    Status wait() override {
        return wait_no_retry();
    }
 
    void sync_pred(Storage* storage, int timeout = -1) {
        auto op = static_cast<SyncOperator*>(_op.get());
        auto sit = op->incr_iterator(storage);
        auto last_sit = sit->clone();
        std::vector<PSRequest> reqs;
        int version;

        while (true) {
            auto st = generate_request(sit.get(), version, reqs);
            if (st.IsEmpty())
                break;
            if (st.ok()) {
                send(std::move(reqs), {_storage_id, _handler_id, version, -1, RequestType::OP_SYNC}, timeout);
                _send_status = st;
                st = wait();
            }
            reqs.clear();
            if (!st.ok()) {
                if (st.IsNoReplica()) {
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    _client->handle_server_too_new_ctx(_storage_id, timeout);
                } else if (st.IsServerTooNewCtx()) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    _client->handle_server_too_new_ctx(_storage_id, timeout);
                } else if (st.IsTimeout()) {
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                    _client->handle_timeout(_storage_id, timeout);
                } else if (st.IsInvalidID()) {
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                    init_table();
                } else {
                    SLOG(FATAL) << st.ToString();
                }
                sit = last_sit->clone();
            } else {
                last_sit = sit->clone();
            }
        }
    }

protected:
    Status generate_request(StorageIterator* sit, 
            int& version,
            std::vector<PSRequest>& reqs) {
        TableDescriptorReader td;
        auto st = _client->context()->GetTableDescriptorReader(_storage_id, td);
        SCHECK(st.ok()) << st.ToString();
        version = td.table().version;
        auto op = static_cast<SyncOperator*>(_op.get());
        return op->generate_sync_request(sit, *td.table().runtime_info, reqs);
    }

    void init_table() {
        MasterClient* master_client = _client->master_client();
        Client* client = _client;
        std::string model_str;
        SCHECK(master_client->get_model(_model_name, model_str)) << "get model " << _model_name << " failed";
        ModelObject model;
        SCHECK(model.from_json_str(model_str));
        auto tables = model.all_tables();
        auto it = tables.find(_table_name);
        SCHECK(it != tables.end());
        SCHECK(it->second->is_remote_ps());
        _table = *it->second;

        _storage_id = _table.storage_id;
        client->initialize_storage(_storage_id);
        TableDescriptorReader td;
        auto st = client->context()->GetTableDescriptorReader(_storage_id, td);
        SCHECK(st.ok()) << st.ToString();

        auto hit = td.table().key_to_hdl.find(_op_key);
        SCHECK(hit != td.table().key_to_hdl.end());
        _handler_id = hit->second;

        if (_op.get() == nullptr) {
            auto opd_it = td.table().op_descs.find(_handler_id);
            SCHECK(opd_it != td.table().op_descs.end());
            Configure op_conf;
            op_conf.load(opd_it->second.config_str);
            _op = OperatorFactory::singleton().create(opd_it->second.lib_name, opd_it->second.op_name, op_conf);
        }
    }

    //void retry(size_t pid, int timeout) override {
        //SCHECK(_busy[pid]) << pid;
        //std::vector<PSRequest> reqs;
        //auto sit = _sit->clone();
        //auto st = generate_request(sit.get(), _meta[pid].ctx_ver, reqs);
        //_status[pid] = st;
        //send(pid, reqs, _meta[pid], timeout); 
    //}

protected:
    std::string _model_name;
    std::string _table_name;
    std::string _op_key;
    std::string _op_name;
    std::string _op_config;
    TableObject _table;
    //Storage* _storage;
    //std::unique_ptr<StorageIterator> _sit;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4
#endif // PARADIGM4_PICO_PS_HANDLER_SYCN_HANDLER_H
