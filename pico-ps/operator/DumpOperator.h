#ifndef PARADIGM4_PICO_PS_OPERATOR_DUMPOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_DUMPOPERATOR_H

#include <memory>
#include <string>
#include <vector>

#include "pico-ps/common/core.h"

#include "pico-ps/common/message.h"
#include "pico-ps/operator/ForEachOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

struct DumpArgs : CarriedItem {
    std::string _uri;
    size_t _file_number;

    PICO_SERIALIZATION(_uri, _file_number);

    DumpArgs() = default;

    DumpArgs(const URIConfig& uri, size_t file_number, std::string hadoop_bin) {
        SCHECK(file_number > 0) << "Invalid file number";
        _file_number = file_number;
        
        URIConfig re_uri(uri);
        std::map<std::string, std::pair<std::string, int>> param_map
            = {{core::URI_HADOOP_BIN, {hadoop_bin, URILVL::EXTCFG}}};
        re_uri.replace_param(param_map);
        _uri = re_uri.uri();
    }

    const std::string& uri() const {
        return _uri;
    }

    int32_t file_number() const {
        return _file_number;
    }
};

/*!
 * \brief   继承于 ShardStorageForEachOperator 的 ShardStorageDumpOperator，
 *          用于将 Storage 内容保存至文件系统。
 */

class ShardStorageDumpOperator : public ShardStorageForEachOperator {
public:
    ShardStorageDumpOperator(const Configure& conf) : ShardStorageForEachOperator(conf) {}

    virtual ~ShardStorageDumpOperator() {};

    ShardStorageDumpOperator(ShardStorageDumpOperator&&) = default;
    ShardStorageDumpOperator& operator=(ShardStorageDumpOperator&&) = default;

    virtual void generate_request(const CarriedItem& item,
          RuntimeInfo& rt,
          std::vector<PSRequest>& reqs) override {
        const auto& dump_args = static_cast<const DumpArgs&>(item);
        int32_t file_number = dump_args.file_number();
        int32_t request_number = rt.nodes().size() * file_number;
        reqs.reserve(request_number);

        /***
         * Encoding Scheme of the BinaryArchive of RpcRequest:
         * <|     dump_args     | file_id | vector of shard ids |>
         * <|  sizeof(DumpArgs) | 4bytes  |     variable size   |>
         ***/
        for (auto& p: rt.nodes()) {
            std::vector<std::vector<int32_t>> shard_assignment(file_number);
            for (int32_t shard_id: p.second) {
                shard_assignment[shard_id % file_number].push_back(shard_id);
            }
            for (int32_t file_id = 0; file_id < file_number; ++file_id) {
                reqs.emplace_back(p.first);
                reqs.back() << dump_args;
                reqs.back() << file_id;
                reqs.back() << shard_assignment[file_id];
            }
        }
    }

};

/*!
 *  \brief 针对 KV Text File 的 Dump 操作。
 *
 *         KV       定义了 key-value pair 以及其序列化方法的数据结构
 *         STORAGE  server 端具体保存 key-value pair 的数据结构
 *         RES      Dump 操作继承于 Foreach 操作，所以与 ForEach
 *                  操作类似，可以在遍历过程中统计信息（非必需）。
 *
 *         application developer 根据自己的需求实现如下几个函数。
 *         一般情况下，无需自己实现函数即可完成 Dump 功能，
 *         若需要统计信息，则应该实现所有三个函数。
 *
 *         * virtual std::unique_ptr<RES> init_result() {
 *             return nullptr;
 *         }
 *         初始化默认 ForEachResult 类型的函数。
 *
 *         * virtual std::unique_ptr<RES> dump_kv(DS_WRITER& writer,
 *               const key_type& key,
 *               value_type& value) {
 *             RCHECK(writer->write({key, value})) << "failed to write";
 *             return nullptr;
 *         }
 *         将 key-value 写入到 writer，并且统计信息返回 ForEachResult 的函数。
 *
 *         * virtual void merge_result(const RES& lhs, RES& rhs) {}
 *         将 ForEachResult lhs 合并至 ForEachResult rhs 的函数。
 */
template <typename DS_WRITER,
      typename KV,
      typename STORAGE,
      typename RES = ForEachResult>
class BaseKVTextFileDumpOperator : public ShardStorageDumpOperator {
public:
    typedef typename STORAGE::key_type key_type;
    typedef typename STORAGE::value_type value_type;

    static_assert(IsTextArchivable<value_type>::value, "table value is not Archivable.");

    typedef STORAGE storage_type;
    typedef typename storage_type::shard_type shard_type;

    BaseKVTextFileDumpOperator(const Configure& conf) : ShardStorageDumpOperator(conf) {}

    virtual ~BaseKVTextFileDumpOperator() {}

    BaseKVTextFileDumpOperator(BaseKVTextFileDumpOperator&&) = default;
    BaseKVTextFileDumpOperator& operator=(BaseKVTextFileDumpOperator&&) = default;

    void apply_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp_ret) override {
        DumpArgs dumpArgs;
        req >> dumpArgs;
        int32_t file_id;
        req >> file_id;
        std::vector<int32_t> shard_ids;
        req >> shard_ids;
        SCHECK(req.archive().is_exhausted());

        std::shared_ptr<void> hdfs_process;
        try {
            hdfs_process = ShardStorageMemory::singleton().reserve(200 << 20);
        } catch (std::bad_alloc&) {
            SLOG(FATAL) << "no memory for hdfs process.";
        }
    
        URIConfig file_uri(dumpArgs.uri(), format_string("/model_%d_%d", rt.node_id(), file_id));
        DS_WRITER writer;
        SCHECK(writer.open(file_uri))
              << "failed to open file for write, filename: " << file_uri.uri();
        auto shard_storage = static_cast<storage_type*>(storage);
        auto op_res = init_result_impl();
        CarriedItem carried;
        for (int32_t shard_id : shard_ids) {
            shard_storage->write_shard(
                  shard_id, [&writer, &op_res, &carried, this](boost::any& shard) {
                      auto& data = *boost::any_cast<shard_type>(&shard);
                      for (auto it = data.begin(); it != data.end(); ++it) {
                          auto res = dump_kv(writer, it->first, it->second);
                          if (op_res.get() != nullptr) {
                              merge_result_impl(*res, *op_res, carried);
                          }
                      }
                  });
        }
        writer.close();
        PSResponse resp(req);
        auto ok = Status();
        resp << ok;
        if (op_res.get() != nullptr) {
            op_res->_binary_archive_serialize_internal_(resp.archive());
        }
        resp_ret = std::move(resp);
    }

    virtual std::unique_ptr<ForEachResult> init_result_impl() {
        return init_result();
    }

    virtual void merge_result_impl(const ForEachResult& in,
          ForEachResult& out,
          const CarriedItem&) override {
        merge_result(static_cast<const RES&>(in), static_cast<RES&>(out));
    }

    virtual std::unique_ptr<RES> dump_kv(DS_WRITER&, const key_type&, value_type&) = 0;

    virtual std::unique_ptr<RES> init_result() {
        return nullptr;
    }

    virtual void merge_result(const RES&, RES&) {}
};


template <typename DS_WRITER,
      typename KV,
      typename STORAGE
      = GoogleDenseHashMapShardStorage<typename KV::key_type, typename KV::value_type>,
      typename RES = ForEachResult>
class KVTextFileDumpOperator : public BaseKVTextFileDumpOperator<DS_WRITER, KV, STORAGE, RES> {
public:
    typedef typename STORAGE::key_type key_type;
    typedef typename STORAGE::value_type value_type;

    static_assert(std::is_same<key_type, typename KV::key_type>::value,
          "KEY type of DSKV is not the same as KEY type of Storage.");
    static_assert(std::is_same<value_type, typename KV::value_type>::value,
          "VALUE type of DSKV is not the same as VALUE type of Storage.");

    KVTextFileDumpOperator(const Configure& config)
        : BaseKVTextFileDumpOperator<DS_WRITER, KV, STORAGE, RES>(config) {}

    virtual ~KVTextFileDumpOperator() {}

    KVTextFileDumpOperator(KVTextFileDumpOperator&&) = default;
    KVTextFileDumpOperator& operator=(KVTextFileDumpOperator&&) = default;

    virtual std::unique_ptr<RES> dump_kv(DS_WRITER& writer,
          const key_type& key,
          value_type& value) {
        RCHECK(writer.write({key, value})) << "failed to write";
        return nullptr;
    }
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_OPERATOR_DUMPOPERATOR_H
