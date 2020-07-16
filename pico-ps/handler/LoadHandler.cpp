#include "pico-ps/handler/LoadHandler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

LoadHandler::LoadHandler(int32_t storage_id,
      int32_t handler_id,
      std::shared_ptr<Operator>& op,
      Client* client)
    : Handler(storage_id, handler_id, op, client) {
}

LoadHandler::~LoadHandler() {}

void LoadHandler::load(const URIConfig& path,
      const std::string& hadoop_bin,
      size_t server_concurency,
      int timeout) {
    SCHECK(!_busy);
    _busy = true;
    TableDescriptorReader td;
    SCHECK(_client->context()->GetTableDescriptorReader(_storage_id, td).ok());
    _meta = {_storage_id, _handler_id, td.table().version, -1, RequestType::OP_LOAD};
    auto & data = _data;
    data.path = path;
    std::map<std::string, std::pair<std::string, int>> param_map
          = {{ds::DS_HADOOP_BIN, {hadoop_bin, URILVL::EXTCFG}}};
    data.path.replace_param(param_map);
    data.hadoop_bin = hadoop_bin;
    data.server_concurency = server_concurency;
    _timeout = timeout;
    data.future = std::async(std::launch::async, 
          [this] { return _load(); });
}

void LoadHandler::restore(const URIConfig& path, bool drop,
    const std::string& hadoop_bin,
    size_t server_concurency,
    int timeout) {
    SCHECK(!_busy);
    _busy = true;
    _meta = {_storage_id, _handler_id, -1, -1, RequestType::OP_LOAD};
    auto & data = _data;
    data.path = path;
    std::map<std::string, std::pair<std::string, int>> param_map
        = {{ds::DS_HADOOP_BIN, {hadoop_bin, URILVL::EXTCFG}}};
    data.path.replace_param(param_map);
    data.need_rehash = false;
    data.hadoop_bin = hadoop_bin;
    data.server_concurency = server_concurency;
    data.drop = drop;
    _timeout = timeout;
    data.future = std::async(std::launch::async, 
          [this] { return _load(); });
}

Status LoadHandler::wait_no_release() {
    SCHECK(_busy);
    _busy = false;
    return _data.future.get();
}

void LoadHandler::retry(int timeout) {
    SCHECK(!_busy);
    _busy = true;
    TableDescriptorReader td;
    SCHECK(_client->context()->GetTableDescriptorReader(_storage_id, td).ok());
    _meta.ctx_ver = td.table().version;
    _timeout = timeout;
    _data.future = std::async(std::launch::async, 
          [this] { return _load(); });
}

Status LoadHandler::_load() {
    try {
        auto& data = _data;
        std::vector<std::string> files;
        size_t expected_resp_num = 0;
        TableDescriptorReader td;
        auto st = _client->context()->GetTableDescriptorReader(_meta.sid, td);
        SCHECK(st.ok()) << st.ToString();
        if (data.path.storage_type() == FileSystemType::HDFS) {
            files = FileSystem::get_file_list(data.path.name(), data.hadoop_bin);
            for (size_t k = 0; k < data.server_concurency; ++k) {
                for (auto& node : td.table().nodes) {
                    if (files.empty()) {
                        break;
                    }
                    std::string uri = data.path.new_uri(files.back());
                    files.pop_back();
                    send_request(node.first, {uri, data.need_rehash, data.drop});
                    ++expected_resp_num;
                }
            }
        } else {
            for (auto& node : td.table().nodes) {
                std::string uri = data.path.uri();
                send_request(node.first, {uri, data.need_rehash, data.drop});
                ++expected_resp_num;
            }
        }

        while (expected_resp_num > 0) {
            RpcResponse response;
            bool ret = _dealer->recv_response(response, _timeout);
            if (!ret) {
                reset_dealer();
                return Status::Timeout("");
            }
            PSResponse resp(std::move(response));
            PSMessageMeta meta;
            auto status = check_resp_validity(resp, meta);
            if (!status.ok()) {
                reset_dealer();
                return status;
            }
            status = check_rpc_error(resp);
            if (!status.ok()) {
                reset_dealer();
                return status;
            }
            static_cast<LoadOperator*>(_op.get())->apply_load_response(resp);
            if (!files.empty()) {
                std::string uri = data.path.new_uri(files.back());
                files.pop_back();
                send_request(resp.rpc_response().head().sid, {uri, data.need_rehash, data.drop});
            } else {
                --expected_resp_num;
            }
        }
        if(data.path.storage_type() == FileSystemType::HDFS && data.drop) {
            FileSystem::rmrf(data.path);
        }
        return Status();
    } catch (...) { return Status::Fatal("Internal error"); }
}

void LoadHandler::send_request(comm_rank_t rank, const LoadArgs& args) {
    PSRequest req(rank);
    static_cast<LoadOperator*>(_op.get())->generate_load_request(args, req);
    req << _meta;
    _dealer->send_request(std::move(req.rpc_request()));
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
