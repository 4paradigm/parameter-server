#ifndef PARADIGM4_PICO_PS_OPERATOR_RESTOREOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_RESTOREOPERATOR_H

#include <cstdio>

#include <memory>
#include <string>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/Partitioner.h"
#include "pico-ps/common/core.h"

#include "pico-ps/common/message.h"
#include "pico-ps/operator/Operator.h"
#include "pico-ps/storage/KVShardStorage.h"

namespace paradigm4 {
namespace pico {
namespace ps {


struct CoordinatedRestoreRequestItem: public Object {
    CoordinatedRestoreRequestItem(int32_t node_id, int32_t storage_id, int32_t iterator_id, size_t offset,
                                 size_t batch_size)
        :  node_id(node_id), storage_id(storage_id), iterator_id(iterator_id), offset(offset), batch_size(batch_size) {}

    int32_t node_id;
    int32_t storage_id;
    int32_t iterator_id;
    size_t offset;
    size_t batch_size;
};

struct KVShardCoordinatedRestoreRequestItem: public CoordinatedRestoreRequestItem {
    KVShardCoordinatedRestoreRequestItem(int32_t node_id, int32_t storage_id, int32_t iterator_id, size_t offset,
                                        size_t batch_size, int32_t shard_id)
        : CoordinatedRestoreRequestItem(node_id, storage_id, iterator_id, offset, batch_size), shard_id(shard_id) {}

    int32_t shard_id;
};

struct CoordinatedRestoreResponseItem: public Object {
    bool finished;
    int32_t iterator_id;
    size_t next_offset;
};

class RestoreOperator : public Operator {
public:
    RestoreOperator(const Configure& conf) : Operator(conf) {}

    virtual ~RestoreOperator() {}

    RestoreOperator(RestoreOperator&&) = default;
    RestoreOperator& operator=(RestoreOperator&&) = default;
    
    virtual void generate_coordinated_restore_request(CoordinatedRestoreRequestItem*, std::vector<PSRequest>&) {
        SLOG(FATAL) << "No implement for training";
    }

    virtual void apply_coordinated_restore_request(PSRequest&, Storage*, PSResponse&) {
        SLOG(FATAL) << "No implement for training";
    }

    virtual void apply_coordinated_restore_response(PSResponse&, Storage*, CoordinatedRestoreResponseItem*) {
        SLOG(FATAL) << "No implement for training";
    }

    virtual void restore(const URIConfig&, RuntimeInfo&, Storage*) {
        SLOG(WARNING) << "no actual restore finished";
    }
};

template <typename DS_READER, typename DS_KV, typename KEY, typename VALUE, typename PARTITIONER, typename STORAGE>
class BaseKVTextFileRestoreOperator : public RestoreOperator {
public:
    typedef typename DS_KV::key_type ds_key_type;
    typedef typename DS_KV::value_type ds_value_type;
    typedef KEY key_type;
    typedef VALUE value_type;
    typedef STORAGE storage_type;
    typedef typename STORAGE::shard_type shard_type;

    BaseKVTextFileRestoreOperator(const Configure& conf) : RestoreOperator(conf) {
        initialize_compress_info(conf, "RestoreOperator", _compress_info);
    }

    virtual ~BaseKVTextFileRestoreOperator() {}

    BaseKVTextFileRestoreOperator(BaseKVTextFileRestoreOperator&&) = default;
    BaseKVTextFileRestoreOperator& operator=(BaseKVTextFileRestoreOperator&&) = default;

    virtual void generate_coordinated_restore_request(CoordinatedRestoreRequestItem* req_item, std::vector<PSRequest>& req) {
        auto& item = *static_cast<KVShardCoordinatedRestoreRequestItem*>(req_item);
        req.emplace_back(item.node_id);
        req.back() << item.storage_id << item.iterator_id << item.offset << item.batch_size << item.shard_id;
    }

    virtual void apply_coordinated_restore_request(PSRequest& req, Storage* storage, PSResponse& resp) {
        int32_t storage_id;
        int32_t iterator_id;
        size_t offset;
        size_t batch_size;
        int32_t shard_id;
        req >> storage_id >> iterator_id >> offset >> batch_size >> shard_id;

        auto& st = *static_cast<storage_type*>(storage);
        core::shared_lock_guard<storage_type> l(st);
        auto &shard = *(st.get(shard_id));
        core::lock_guard<ShardData> ls(shard);
        auto it = st.get_shard_iterator(shard_id, iterator_id);
        if (it == nullptr) {
            resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
            resp << Status::InvalidID("Invalid shard id");
            return;
        }
        BinaryArchive ar;
        size_t num = it->iterate_batch_as_binary_archive(offset, batch_size, &ar);
        resp = PSResponse(req);
        resp << storage_id << shard_id << it->finished() << it->id() << it->current_offset() << num;
        ps_serialize(resp.lazy(), _compress_info, std::move(ar));
        if (it->finished()) {
            st.delete_shard_iterator(shard_id, iterator_id);
        }
    }

    virtual void apply_coordinated_restore_response(PSResponse& resp, Storage* storage, CoordinatedRestoreResponseItem* resp_item) {
        int32_t storage_id;
        int32_t shard_id;
        resp >> storage_id >> shard_id;
        resp >> resp_item->finished >> resp_item->iterator_id >> resp_item->next_offset;
        size_t num;
        resp >> num;
        BinaryArchive ar;
        ps_deserialize(resp.lazy(), _compress_info, ar);
        auto& st = *static_cast<storage_type*>(storage);
        core::shared_lock_guard<storage_type> l(st);
        auto &shard = *(st.get(shard_id));
        core::lock_guard<ShardData> ls(shard);
        auto &ht = *boost::any_cast<shard_type>(&shard.data);
        while(num--) {
            std::pair<key_type, value_type> item;
            ar >> item;
            bool succ = ht.insert(item).second;
            if (!succ) {
                SLOG(WARNING) << "Try to insert key: \"" << item.first << "\", but already existed.";
            }
        }
    }

    virtual void restore(const URIConfig& uri, RuntimeInfo& rt, Storage* storage)override {
        auto& st = *static_cast<storage_type*>(storage);
        auto reader = std::make_shared<DS_READER>();
        key_type key;
        value_type value;
        SCHECK(reader->open(uri));
        DS_KV kv;
        while (reader->read(kv)) {
            deserialize_kv(kv, key, value);
            auto shard_id = _key_partitioner.partition(key, rt.global_shard_num());
            if (rt.local_shards().count(shard_id) > 0) {
                st.write_shard(shard_id, [&key, &value](boost::any& any) {
                    shard_type& ht = *boost::any_cast<shard_type>(&any);
                    bool succ = ht.insert({key, value}).second;
                    if (!succ) {
                        SLOG(WARNING) << "Try to insert key: \"" << key << "\", but already existed.";
                    }
                });
            }
        }
    }

    virtual void deserialize_kv(DS_KV&, key_type&, value_type&) = 0;

protected:
    CompressInfo _compress_info;
    PARTITIONER _key_partitioner;
};

template <typename DS_READER,
      typename DS_KV,
      typename PARTITIONER = DefaultPartitioner<typename DS_KV::key_type>,
      typename STORAGE
      = GoogleDenseHashMapShardStorage<typename DS_KV::key_type, typename DS_KV::value_type>,
      typename KEY = typename DS_KV::key_type,
      typename VALUE = typename DS_KV::value_type>
class KVTextFileRestoreOperator : public BaseKVTextFileRestoreOperator<DS_READER,
                              DS_KV,
                              KEY,
                              VALUE,
                              PARTITIONER,
                              STORAGE> {
public:
    virtual ~KVTextFileRestoreOperator() {}
    KVTextFileRestoreOperator(KVTextFileRestoreOperator&&) = default;
    KVTextFileRestoreOperator& operator=(KVTextFileRestoreOperator&&) = default;
    KVTextFileRestoreOperator(const Configure& conf) : BaseKVTextFileRestoreOperator<DS_READER,
                              DS_KV,
                              KEY,
                              VALUE,
                              PARTITIONER,
                              STORAGE>(conf) {}
    virtual void deserialize_kv(DS_KV& kv, KEY& key, VALUE& value) override {
        key = kv.key;
        value = kv.value;
    }
};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_OPERATOR_RESTOREOPERATOR_H
