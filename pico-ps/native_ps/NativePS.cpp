#include "pico-ps/native_ps/NativePS.h"

namespace paradigm4 {
namespace pico {
namespace ps {

NativePS::NativePS() {
    _ctx = std::make_shared<Context>();
}

NativePS::~NativePS() {}

int32_t NativePS::create_storage(const std::string& lib_name,
      const std::string& op_name,
      const Configure& conf) {
    int32_t storage_id = _storage_num++;
    SCHECK(conf.has("op_config"));
    auto op = OperatorFactory::singleton().create(lib_name, op_name, conf["op_config"]);
    SCHECK(op != nullptr);
    auto status = _ctx->CreateStorage(storage_id, op, lib_name, op_name, conf);
    SCHECK(status.ok()) << status.ToString();
    _ctx->UpdateRuntimeInfo(storage_id, 0);
    TableDescriptorReader td;
    status = _ctx->GetTableDescriptorReader(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    
    //add by cc pmem add one more para: storage_id 
    td.table().storage
          = static_cast<StorageOperator*>(op.get())->create_storage(*td.table().runtime_info, storage_id);
    return storage_id;
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
