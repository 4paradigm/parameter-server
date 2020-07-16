#ifndef PARADIGM4_PICO_PS_SERVICE_CONTEXT_H
#define PARADIGM4_PICO_PS_SERVICE_CONTEXT_H

#include <functional>
#include <memory>
#include <unordered_map>

#include <boost/thread/shared_mutex.hpp>

#include "pico-ps/common/core.h"

#include "pico-ps/operator/Operator.h"
#include "pico-ps/service/TableDescriptor.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class Context {
public:
    Status CreateStorage(int32_t storage_id,
          std::shared_ptr<Operator>& op,
          const std::string& lib_name,
          const std::string& op_name,
          const Configure& config);

    Status SetTableDescriptor(int32_t storage_id, std::unique_ptr<TableDescriptor> td);

    Status DeleteStorage(int32_t storage_id);

    Status SetContextVersion(int32_t storage_id, int32_t version);

    Status GetContextVersion(int32_t storage_id, int32_t& version);

    Status CheckDeltaStorage(int32_t storage_id, Storage** ptr);

    Status GetTableDescriptorReader(int32_t storage_id, TableDescriptorReader& reader);

    Status GetTableDescriptorWriter(int32_t storage_id, TableDescriptorWriter& writer);

    Status UpdateRuntimeInfo(int32_t storage_id, int my_node_id);

    static void update_runtime_info(TableDescriptor& td, int my_node_id);

    static void initialize_storage_op(TableDescriptor& td);

    static void update_handlers(TableDescriptor& td);

    static void update_handlers_form_str(TableDescriptor& td, const std::string& str);

private:
    std::unordered_map<int32_t, std::unique_ptr<TableDescriptor>> _tables; // storage id -> TableDescriptor
    std::unordered_map<int32_t, std::unique_ptr<RWSpinLock>> _table_locks;
    RWSpinLock _mtx;
};

PickAlgo initialize_shard_pick_algo(const Configure& config);

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_SERVICE_CONTEXT_H
