#ifndef PARADIGM4_PICO_PS_COMMON_COMMON_H
#define PARADIGM4_PICO_PS_COMMON_COMMON_H

#include "core.h"

namespace paradigm4 {
namespace pico {
namespace ps {

struct CompressInfo {
    bool _enabled = false;
    Compress _compresser;
};

void initialize_compress_info(const Configure& config,
      const std::string& op_name,
      CompressInfo& compress_info);

std::string byte2str(const char* data, size_t num_bytes);


template<typename T>
void add_base64_config(Configure& conf, const std::string& key, T& value) {
    std::string raw, out;
    Compress c("snappy");
    c.compress(value, raw);
    Base64::singleton().base64_encode(raw.c_str(), raw.length(), out);
    conf.node()[key] = out; 
}

template<typename T>
void get_base64_config(const Configure& conf, const std::string& key, T& value) {
    std::string in = conf.node()[key].as<std::string>();
    Compress c("snappy");
    size_t len = 0;
    BinaryArchive ar;
    ar.resize(in.length() * 2);
    Base64::singleton().base64_decode(in, ar.buffer(), len);
    ar.resize(len);
    c.uncompress(ar, value);
}


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_COMMON_COMMON_H
