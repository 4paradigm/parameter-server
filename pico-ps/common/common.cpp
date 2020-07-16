#include "pico-ps/common/common.h"

#include <cstdint>

#include <iomanip>
#include <sstream>



namespace paradigm4 {
namespace pico {
namespace ps {

void initialize_compress_info(const Configure& config,
      const std::string&,
      CompressInfo& compress_info) {
    std::string compress_algo = "";
    if (config.has("compress_algorithm")) {
        compress_algo = config["compress_algorithm"].as<std::string>();
    }
    if (compress_algo != "") {
        compress_info._enabled = true;
        compress_info._compresser = pico_compress(compress_algo);
        // SLOG(INFO) << op_name << " using compression algorithm \"" << compress_algo <<
        // "\".";
    } else {
        // SLOG(INFO) << op_name << " not using compression algorithm.";
    }
}

std::string byte2str(const char* data, size_t num_bytes) {
    std::ostringstream stm;
    for (size_t i = 0; i < num_bytes; ++i) {
        stm << std::hex << std::setfill('0') << std::setw(2) << uint32_t(*(data + i));
    }
    return stm.str();
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
