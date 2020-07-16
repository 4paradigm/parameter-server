#ifndef PARADIGM4_PICO_PS_OPERATOR_LOAD_OPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_LOAD_OPERATOR_H

#include <cstdio>

#include <memory>
#include <string>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/core.h"

#include "pico-ds/builder/DsReader.h"
#include "pico-ps/common/message.h"
#include "pico-ps/operator/Operator.h"
#include "pico-ps/operator/SparsePushOperator.h"
#include "pico-ps/common/DistributedAsyncReturn.h"

namespace paradigm4 {
namespace pico {
namespace ps {

struct LoadArgs {
    std::string uri;
    bool need_rehash = true;
    bool drop = false;

    PICO_SERIALIZATION(uri, need_rehash);
};

/*!
 * \brief   用于将文件系统中的内容读取到 Storage 的 LoadOperator。
 *          流程为：
 *          1. client 读取文件列表。
 *          2. client 将文件列表中每一个文件产生一个 load_request 发送给某一个 server。
 *          3. server 按照 load_requet 中的文件 uri 读取文件，调用 push 操作 push 到对应的
 *                    storage 所在的 server。
 *
 *          因此，client 只负责分发文件列表给 server，而根据 key 将读取的内容 shuffle 到相应
 *          server 的过程发生在 parameter server 端。
 *          server 端存在两组线程，一组是处理 client2server 的 load_request,
 *          另一组处理 server2server 的 push_request。
 *
 *          client 调用 generate_load_request() 生成 load_request；
 *          c2s_server 调用 create_stream() 打开 uri 对应的文件，读取文件，
 *                     调用 generate_push_request() 将读取的内容序列化至 push_request() 中；
 *          s2s_server 调用 apply_async_push_request() 将 c2s_server push 过来的内容保存至
 *                     Storage 中，并且返回 push_response。
 */
class LoadOperator : public Operator {
public:
    LoadOperator(const Configure& conf) : Operator(conf) {}

    virtual ~LoadOperator() {}

    LoadOperator(LoadOperator&&) = default;
    LoadOperator& operator=(LoadOperator&&) = default;

    // 给定单个文件的所有的信息，生成一个req, req的dest_id已经生成好
    virtual void generate_load_request(const LoadArgs& load_uri, PSRequest& req) {
        req << load_uri;
    }

    virtual void apply_load_request(const PSMessageMeta& meta,
        RuntimeInfo & rt,
        PSRequest& req,
        Storage* storage,
        PSResponse& resp, int version, RpcClient* rpc_client){
        LoadArgs args;
        req >> args;
        URIConfig uri(args.uri);
        if(!args.need_rehash){
            restore(uri, rt, storage);
        } else {
            std::shared_ptr<void> hdfs_process;
            try {
                hdfs_process = ShardStorageMemory::singleton().reserve(200 << 20);
            } catch (std::bad_alloc&) {
                SLOG(WARNING) << "no memory for hdfs process.";
            }
            
            std::shared_ptr<void> stream;
            create_stream(uri, stream);
            SCHECK(stream.get() != nullptr);
            core::vector<std::unique_ptr<PushItems>> content;
            DirectPushDistributedAsyncReturn push(push_operator(), rpc_client);
            while (generate_push_items(stream, content) > 0) {
                auto status = push.push(std::move(content), rt,
                    {meta.sid, meta.hid, version, -1, RequestType::OP_PUSH_ASYNC}, 60000);
                if (!status.ok()) {
                    resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
                    resp << status;
                    return;
                }
            }
        }
        if(uri.storage_type() != FileSystemType::HDFS && args.drop) {
            FileSystem::rmrf(uri);
        }

        resp << Status();
    }

    virtual void apply_load_response(PSResponse&) = 0;

    virtual size_t generate_push_items(std::shared_ptr<void>& stream,
          core::vector<std::unique_ptr<PushItems>>& push_items)
          = 0;

    virtual void restore(const URIConfig& , RuntimeInfo& , Storage* ) {
        SLOG(FATAL);
    }

    virtual PushOperator* push_operator() = 0;

    // 一个message对应一个Archive

    virtual void create_stream(const URIConfig& uri, std::shared_ptr<void>& stream) = 0;

};

template <typename DS_READER,
      typename DS_KV,
      typename KEY,
      typename VALUE,
      typename PARTITIONER,
      typename STORAGE,
      typename PUSH_OP = DirectPushOperator<KEY, VALUE, PARTITIONER, STORAGE>,
      typename PUSH_VALUE = VALUE>
class BaseKVTextFileLoadOperator : public LoadOperator {
public:
    typedef typename DS_KV::key_type ds_key_type;
    typedef typename DS_KV::value_type ds_value_type;
    typedef KEY key_type;
    typedef VALUE value_type;
    typedef PUSH_VALUE push_value_type;
    typedef STORAGE storage_type;
    typedef typename STORAGE::shard_type shard_type;

    static_assert(std::is_same<key_type, typename STORAGE::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Storage.");
    static_assert(std::is_same<value_type, typename STORAGE::value_type>::value,
          "VALUE type of Operator is not the same as VALUE type of Storage.");
    static_assert(std::is_same<storage_type, typename PUSH_OP::storage_type>::value,
          "storage type of Load Operator is not the same as storage type of Push Operator.");

    BaseKVTextFileLoadOperator(const Configure& config)
        : LoadOperator(config), _push_op(config) {
        _block_size = config["block_size"].as<int32_t>();
    }

    virtual ~BaseKVTextFileLoadOperator() {}

    BaseKVTextFileLoadOperator(BaseKVTextFileLoadOperator&&) = default;
    BaseKVTextFileLoadOperator& operator=(BaseKVTextFileLoadOperator&&) = default;

    virtual void apply_load_response(PSResponse& resp) override {
        Status st;
        resp >> st;
        SCHECK(resp.archive().is_exhausted()) << resp.archive().readable_length();
        SCHECK(st.ok()) << st.ToString();
    }

//    std::enable_if_t<std::is_same<push_value_type, typename STORAGE::value_type>::value,
    virtual void restore(const URIConfig& uri, RuntimeInfo& rt, Storage* storage) {
        auto& st = *static_cast<storage_type*>(storage);
        DS_READER reader;
        std::vector<key_type> keys;
        std::vector<push_value_type> args;
        keys.reserve(_block_size);
        args.reserve(_block_size);
        auto files = FileSystem::get_file_list(uri);
        reader.open(uri.new_uri(files.back()));
        files.pop_back();
        int prev_shard = -1;
        while (true) {
            DS_KV kv;
            if (!reader.read(kv)) {
                if (files.empty())break;
                reader.open(uri.new_uri(files.back()));
                files.pop_back();
            } else {
                KEY key;
                VALUE value;
                deserialize_kv(kv, key, value);
                auto shard_id = _key_partitioner.partition(key, rt.global_shard_num());
                if (rt.local_shards().count(shard_id) > 0) {
                    if (prev_shard == -1) {
                        prev_shard = shard_id;
                    }
                    if (shard_id != prev_shard || static_cast<int>(keys.size()) >= _block_size) {
                        st.write_shard(prev_shard, [&keys, &args](boost::any& any) {
                            shard_type& ht = *boost::any_cast<shard_type>(&any);
                            for (size_t i = 0; i < keys.size(); ++i) {
                                SCHECK(ht.insert({keys[i], args[i]}).second);
                            }
                        });
                        prev_shard = shard_id;
                        keys.clear();
                        args.clear();
                    }
                    keys.push_back(key);
                    args.push_back(value);
                }
            }
        }
        if(prev_shard != -1)
            st.write_shard(prev_shard, [&keys, &args](boost::any& any) {
                shard_type& ht = *boost::any_cast<shard_type>(&any);
                for (size_t i = 0; i < keys.size(); ++i) {
                    SCHECK(ht.insert({keys[i], args[i]}).second);
                }
            });
    }

    virtual void create_stream(const URIConfig& uri, std::shared_ptr<void>& stream) override {
        if(uri.storage_type() == FileSystemType::HDFS) {
            auto reader = std::make_shared<std::vector<DS_READER>>(1);
            SCHECK((*reader)[0].open(uri));
            stream = reader;
        }else{
            auto files = FileSystem::get_file_list(uri.uri());
            auto reader = std::make_shared<std::vector<DS_READER>>(files.size());
            for(size_t i = 0; i < files.size(); ++i){
                SCHECK((*reader)[i].open(uri.new_uri(files[i])));
            }
            stream =  reader;
        }
    }

    virtual size_t generate_push_items(std::shared_ptr<void>& reader,
          core::vector<std::unique_ptr<PushItems>>& push_items) override {
        std::vector<key_type> keys;
        std::vector<push_value_type> args;
        keys.resize(_block_size);
        args.resize(_block_size);

        auto& streams = *static_cast<std::vector <DS_READER> *>(reader.get());

        int i;
        for (i = 0; i < _block_size && streams.size()>0;) {
            DS_KV kv;
            if (!streams.back().read(kv)) {
                streams.pop_back();
            } else {
                deserialize_kv(kv, keys[i], args[i]);
                ++i;
            }
        }
        keys.resize(i);
        args.resize(i);
        auto ret = keys.size();
        push_items.resize(1);
        push_items[0] = std::make_unique<SparsePushItems<key_type, push_value_type>>(
              std::move(keys), std::move(args));
        return ret;
    }

    virtual PushOperator* push_operator() override {
        return &_push_op;
    }

    virtual void deserialize_kv(DS_KV&, key_type&, push_value_type&) = 0;

protected:
    PARTITIONER _key_partitioner;
    int32_t _block_size;
    PUSH_OP _push_op;
};

template <typename DS_READER,
      typename DS_KV,
      typename PARTITIONER = DefaultPartitioner<typename DS_KV::key_type>,
      typename STORAGE
      = GoogleDenseHashMapShardStorage<typename DS_KV::key_type, typename DS_KV::value_type>,
      typename KEY = typename DS_KV::key_type,
      typename VALUE = typename DS_KV::value_type,
      typename PUSH_OP = DirectPushOperator<KEY, VALUE, PARTITIONER, STORAGE>>
class KVTextFileLoadOperator : public BaseKVTextFileLoadOperator<DS_READER,
                                              DS_KV,
                                              KEY,
                                              VALUE,
                                              PARTITIONER,
                                              STORAGE,
                                              PUSH_OP> {
public:
    KVTextFileLoadOperator(const Configure& config)
        : BaseKVTextFileLoadOperator<DS_READER,
                DS_KV,
                KEY,
                VALUE,
                PARTITIONER,
                STORAGE,
                PUSH_OP>(config) {}

    virtual ~KVTextFileLoadOperator() {}

    KVTextFileLoadOperator(KVTextFileLoadOperator&&) = default;
    KVTextFileLoadOperator& operator=(KVTextFileLoadOperator&&) = default;

    virtual void deserialize_kv(DS_KV& kv, KEY& key, VALUE& push_val) override {
        key = kv.key;
        push_val = kv.value;
    }
};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_OPERATOR_LOAD_OPERATOR_H
