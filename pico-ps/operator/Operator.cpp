#include "pico-ps/operator/Operator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

Operator::~Operator() {}

OperatorFactory::~OperatorFactory() {
    for (auto& h : _opened_libs) {
        dlclose(h);
    }
}

bool OperatorFactory::load_library(const std::string& /*path*/) {
    //#ifndef LEMON_WITH_OP
    //if (_opened_lib_path.count(path) > 0) {
        //return true;
    //}
    //void* dl_handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    //if (!dl_handle) {
        //SLOG(WARNING) << "Cannot load library with path: \"" << path
                      //<< "\" error: " << dlerror();
        //return false;
    //}
    //_lib_mu.lock();
    //_opened_libs.emplace_back(dl_handle);
    //_opened_lib_path.insert(path);
    //_lib_mu.unlock();
    //SLOG(INFO) << "Loaded library with path: \"" << path << "\"";
    //#else
    //(void)(path);
    //#endif
    return true;
}

std::shared_ptr<Operator> OperatorFactory::create(const std::string& lib_name,
      const std::string& op_name,
      const Configure& conf) {
    boost::shared_lock<boost::shared_mutex> lock(_producer_mu);
    auto it = _producers.find(std::make_pair(lib_name, op_name));
    if (it == _producers.end()) {
        SLOG(WARNING) << "No operator found with lib_name=\"" << lib_name << "\" op_name=\""
                   << op_name << "\"";
        return nullptr;
    } else {
        return std::shared_ptr<Operator>((it->second)(conf));
    }
}

void OperatorFactory::register_producer(const std::string& lib_name,
      const std::string& op_name,
      producer_t p) {
    boost::unique_lock<boost::shared_mutex> lock(_producer_mu);
    auto lookup_key = std::make_pair(lib_name, op_name);
    auto it = _producers.find(lookup_key);
    SCHECK(it == _producers.end());
    _producers.emplace(lookup_key, p);
    // SLOG(INFO) << "Operator of library \"" << lib_name << "\" with name \"" << op_name
    //            << "\" registered";
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
