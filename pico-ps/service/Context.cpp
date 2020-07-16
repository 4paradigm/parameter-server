#include "pico-ps/service/Context.h"

namespace paradigm4 {
namespace pico {
namespace ps {

Status Context::CreateStorage(int32_t storage_id,
      std::shared_ptr<Operator>& op,
      const std::string& lib_name,
      const std::string& op_name,
      const Configure& config) {
    auto ptd = std::make_unique<TableDescriptor>();
    auto& td = *ptd;
    if (!config.has("nodes")) {
        return Status::InvalidConfig("No field \"nodes\"");
    }
    if (!config.has("op_config")) {
        return Status::InvalidConfig("No field \"op_config\"");
    }
    for (size_t i = 0; i < config["nodes"].size(); ++i) {
        td.node_descs.emplace_back(config["nodes"][i]);
    }
    td.refresh_info();
    td.storage_op = op;
    OperatorDescriptor opd("", lib_name, op_name, config["op_config"].dump());
    td.storage_op_desc = std::move(opd);
    core::lock_guard<RWSpinLock> lock(_mtx);
    if (_table_locks.count(storage_id) > 0) {
        return Status::InvalidID(format_string("storage id %d exists", storage_id));
    }
    _tables[storage_id] = std::move(ptd);
    _table_locks[storage_id].reset(new RWSpinLock());
    return Status();
}

Status Context::SetTableDescriptor(int32_t storage_id, std::unique_ptr<TableDescriptor> td) {
    core::lock_guard<RWSpinLock> lock(_mtx);
    auto& table_lock = _table_locks[storage_id];
    if (!table_lock.get()) {
        table_lock.reset(new RWSpinLock());
    }
    core::lock_guard<RWSpinLock> tlock(*table_lock);
    _tables[storage_id] = std::move(td);
    return Status();
}

Status Context::DeleteStorage(int32_t storage_id) {
    core::lock_guard<RWSpinLock> lock(_mtx);
    auto it = _tables.find(storage_id);
    if (it == _tables.end()) {
        return Status();
    }
    {
        core::lock_guard<RWSpinLock> tlock(*_table_locks[storage_id]);
        _tables.erase(it);
    }
    _table_locks.erase(storage_id);
    return Status();
}

Status Context::SetContextVersion(int32_t storage_id, int32_t version) {
    boost::shared_lock<RWSpinLock> lock(_mtx);
    auto it = _tables.find(storage_id);
    if (it == _tables.end()) {
        return Status::InvalidID(format_string("storage id not found: %d", storage_id));
    }
    core::lock_guard<RWSpinLock> table_lock(*_table_locks[storage_id]);
    it->second->version = version;
    return Status();
}

Status Context::GetContextVersion(int32_t storage_id, int32_t& version) {
    boost::shared_lock<RWSpinLock> lock(_mtx);
    auto it = _tables.find(storage_id);
    if (it == _tables.end()) {
        return Status::InvalidID(format_string("storage id not found: %d", storage_id));
    }
    boost::shared_lock<RWSpinLock> table_lock(*_table_locks[storage_id]);
    version = it->second->version;
    return Status();
}

Status Context::CheckDeltaStorage(int32_t storage_id, Storage** ptr) {
    boost::shared_lock<RWSpinLock> lock(_mtx);
    auto it = _tables.find(storage_id);
    if (it == _tables.end()) {
        return Status::InvalidID(format_string("storage id not found: %d", storage_id));
    }
    boost::shared_lock<RWSpinLock> table_lock(*_table_locks[storage_id]);
    *ptr = it->second->delta_storage.get();
    return Status();
}

Status Context::GetTableDescriptorReader(int32_t storage_id, TableDescriptorReader& reader) {
    boost::shared_lock<RWSpinLock> lock(_mtx);
    auto it = _tables.find(storage_id);
    if (it == _tables.end()) {
        return Status::InvalidID(format_string("storage id not found: %d", storage_id));
    }
    TableDescriptorReader ret(it->second.get(), *_table_locks[storage_id]);
    reader = std::move(ret);
    return Status();
}

Status Context::GetTableDescriptorWriter(int32_t storage_id, TableDescriptorWriter& writer) {
    boost::shared_lock<RWSpinLock> lock(_mtx);
    auto it = _tables.find(storage_id);
    if (it == _tables.end()) {
        return Status::InvalidID(format_string("storage id not found: %d", storage_id));
    }
    TableDescriptorWriter ret(it->second.get(), *_table_locks[storage_id]);
    writer = std::move(ret);
    return Status();
}

Status Context::UpdateRuntimeInfo(int32_t storage_id, int my_node_id) {
    TableDescriptorWriter td;
    auto status = GetTableDescriptorWriter(storage_id, td);
    if (!status.ok()) {
        return status;
    }
    update_runtime_info(td.table(), my_node_id);
    return status;
}

void Context::update_runtime_info(TableDescriptor& td, int my_node_id) {
    td.runtime_info.reset(new RuntimeInfo(td, my_node_id));
    td.new_runtime_info.reset(new RuntimeInfo(td, my_node_id, true));
}

void Context::initialize_storage_op(TableDescriptor& td) {
    if (td.storage == nullptr) {
        SCHECK(OperatorFactory::singleton().load_library(
              "lib" + td.storage_op_desc.lib_name + ".so"));
        SLOG(INFO) << "creating storage op with lib name: \"" << td.storage_op_desc.lib_name
                   << "\" op name: \"" << td.storage_op_desc.op_name << "\" config:\n"
                   << td.storage_op_desc.config_str;
        Configure config;
        config.load(td.storage_op_desc.config_str);
        td.storage_op = OperatorFactory::singleton().create(
              td.storage_op_desc.lib_name, td.storage_op_desc.op_name, config);
    }
}

void Context::update_handlers(TableDescriptor& td) {
    for (auto& op_desc : td.op_descs) {
        if (td.handlers.count(op_desc.first) == 0) {
            SLOG(INFO) << "creating new op with lib name: \"" << op_desc.second.lib_name
                       << "\" op name: \"" << op_desc.second.op_name << "\" config:\n"
                       << op_desc.second.config_str;
            Configure config;
            config.load(op_desc.second.config_str);
            auto op = OperatorFactory::singleton().create(
                  op_desc.second.lib_name, op_desc.second.op_name, config);
            td.handlers.emplace(op_desc.first, op);
            td.key_to_hdl[op_desc.second.key] = op_desc.first;
        }
    }
    for (auto it = td.handlers.begin(); it != td.handlers.end();) {
        if (td.op_descs.count(it->first) == 0) {
            td.handlers.erase(it++);
        } else {
            ++it;
        }
    }
}

void Context::update_handlers_form_str(TableDescriptor& td, const std::string& str) {
    bool updated = false;
    SCHECK(td.from_json_str(str, updated)) << str;
    if (updated) {
        update_handlers(td);
    }
}

PickAlgo initialize_shard_pick_algo(const Configure& config) {
    std::string str = "";
    if (config.has("node_selection")) {
        str = config["node_selection"].as<std::string>();
    }
    PickAlgo ret = PickAlgo::RANDOM;
    if (str != "") {
        if (str == "random") {
            ret = PickAlgo::RANDOM;
            // } else if (str == "round_robin") {
            //     ret = PickAlgo::ROUND_ROBIN;
        } else {
            SLOG(WARNING) << "unknow shard selection algorithm: \"" << str << "\"";
        }
    }
    return ret;
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
