#include "pico-ps/native_ps/NativeLoadHandler.h"

#include "pico-ps/operator/LoadOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

NativeLoadHandler::NativeLoadHandler(int32_t storage_id,
      std::shared_ptr<Operator>& op,
      NativePS* native_ps)
    : LoadHandler(storage_id, -1, op, nullptr) {
    TableDescriptorReader td;
    auto status = native_ps->context()->GetTableDescriptorReader(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    _st = td.table().storage.get();
    _td = &(td.table());
}

NativeLoadHandler::~NativeLoadHandler() {}

void NativeLoadHandler::load(const URIConfig& path,
      const std::string& hadoop_bin,
      size_t server_concurency,
      int) {
    auto uri = path;
    std::map<std::string, std::pair<std::string, int>> param_map
          = {{core::URI_HADOOP_BIN, {hadoop_bin, URILVL::EXTCFG}}};
    uri.replace_param(param_map);
    std::vector<std::string> files = FileSystem::get_file_list(uri.name(), hadoop_bin);
    size_t file_per_thread = (files.size() + server_concurency) / server_concurency;
    std::vector<std::thread> ths;
    for (size_t i = 0; i < server_concurency; ++i) {
        ths.emplace_back(&NativeLoadHandler::_load,
              this,
              uri,
              files,
              i * file_per_thread,
              (i + 1) * file_per_thread);
    }
    for (auto& th : ths) {
        th.join();
    }
}

void NativeLoadHandler::_load(const URIConfig& uri,
      const std::vector<std::string>& files,
      size_t begin,
      size_t end) {
    if (begin >= files.size()) {
        return;
    }
    auto op = static_cast<LoadOperator*>(_op.get());
    for (size_t i = begin; (i < end) && (i < files.size()); ++i) {
        std::string file_uri = uri.new_uri(files[i]);
        LoadArgs args({file_uri});
        PSRequest req(0);
        op->generate_load_request(args, req);
        std::shared_ptr<void> stream;
        op->create_stream(URIConfig(file_uri), stream);
        core::vector<std::unique_ptr<PushItems>> content;
        while (op->generate_push_items(stream, content) > 0) {
            std::unique_ptr<PushRequestData> data;
            op->push_operator()->generate_request_data(content, *_td->runtime_info, data);

            std::vector<PSRequest> push_reqs;
            std::vector<PushRequestData*> push_data = {data.get()};
            op->push_operator()->generate_push_request(push_data, *_td->runtime_info, push_reqs);
            for (auto& push_req : push_reqs) {
                PSResponse resp;
                op->push_operator()->apply_async_push_request(*_td->runtime_info, push_req, _st, nullptr, resp);
                op->push_operator()->apply_response(resp);
            }
        }
    }
} // namespace ps

Status NativeLoadHandler::wait_no_retry() {
    return Status();
}

Status NativeLoadHandler::wait() {
    return Status();
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
