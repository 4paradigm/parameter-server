#ifndef PARADIGM4_PICO_PS_OPERATOR_DENSEPUSHOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_DENSEPUSHOPERATOR_H

#include "pico-ps/operator/PushOperator.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

template <typename SLICE_KEY,
      typename VALUE,
      typename PUSH_ARG,
      typename PUSH_VAL,
      typename PARTITIONER,
      typename VAL_PARTITIONER,
      typename STORAGE,
      typename DELTA_STORAGE,
      typename INCR_STORAGE = STORAGE>
class DenseTablePushOperator : public PushOperator {

public:
    static_assert(std::is_same<SLICE_KEY, typename PARTITIONER::key_type>::value,
          "SLICE_KEY type of Operator is not the same as KEY type of PARTITIONER.");
    static_assert(std::is_same<PUSH_ARG, typename VAL_PARTITIONER::value_type>::value,
          "value_type of VAL_PARTITIONER is not same as PUSH_ARG");
    static_assert(std::is_same<SLICE_KEY, typename STORAGE::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Storage.");
    static_assert(std::is_same<VALUE, typename STORAGE::value_type>::value,
          "VALUE type of Operator is not the same as VALUE type of Storage.");
    static_assert(std::is_same<SLICE_KEY, typename DELTA_STORAGE::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Delta Storage.");
    static_assert(std::is_same<PUSH_VAL, typename DELTA_STORAGE::value_type>::value,
          "PUSH_VALUE type of Operator is not the same as VALUE type of Delta Storage.");

    typedef SLICE_KEY slice_key_type;
    typedef typename slice_key_type::key_type key_type;
    typedef VALUE value_type;
    typedef PUSH_ARG push_arg_type;
    typedef PUSH_VAL push_value_type;

    typedef STORAGE storage_type;
    typedef typename storage_type::shard_type shard_type;
    typedef typename storage_type::vector_type vector_type;
    typedef DELTA_STORAGE delta_storage_type;
    typedef typename delta_storage_type::shard_type delta_shard_type;
    typedef INCR_STORAGE incr_storage_type;
    typedef typename incr_storage_type::shard_type incr_shard_type;

    DenseTablePushOperator(const Configure& config) : PushOperator(config) {
        initialize_compress_info(config, "PushOperator", _compress_info);
    }

    virtual ~DenseTablePushOperator() {}

    DenseTablePushOperator(DenseTablePushOperator&&) = default;
    DenseTablePushOperator& operator=(DenseTablePushOperator&&) = default;

    virtual void generate_request_data(core::vector<std::unique_ptr<PushItems>>& push_items,
          RuntimeInfo& rt,
          std::unique_ptr<PushRequestData>& push_request_data) override {
        VTIMER(1, framework, dense push generate request partition, ms);
        if (push_request_data == nullptr) {
            push_request_data = std::make_unique<DenseTablePushRequestData>();
        }
        auto& request_data = *static_cast<DenseTablePushRequestData*>(push_request_data.get());
        SCHECK(push_items.size() == 1) << push_items.size();

        auto key = static_cast<DensePushItems<key_type, push_arg_type>*>(push_items[0].get())
                        ->key;
        auto& push_arg
              = *(static_cast<DensePushItems<key_type, push_arg_type>*>(push_items[0].get())
                        ->push_value);

        request_data.key = key;
        request_data.partitioned = _value_partitioner.partition(key, push_arg);
        request_data.init(rt.global_shard_num());
        int32_t slices_num = request_data.partitioned.parts.size();
        for (int32_t slice_id = 0; slice_id < slices_num; ++slice_id) {
            slice_key_type slice_key(key, slice_id);
            int32_t shard_id = _slice_key_partitioner.partition(
                    slice_key, rt.global_shard_num());
            request_data.shards_slices[shard_id].push_back(slice_id);
        }
    }

    virtual void generate_push_request(std::vector<PushRequestData*>& push_request_data,
          RuntimeInfo& rt,
          std::vector<PSRequest>& reqs) override {
        VTIMER(1, framework, dense push generate request serialize, ms);
        SCHECK(push_request_data.size() == 1) << push_request_data.size();
        auto& request_data = *static_cast<DenseTablePushRequestData*>(push_request_data[0]);

        for (auto p: rt.nodes()) {
            int32_t node_slices_num = 0;
            int32_t shards_num = p.second.size();
            reqs.emplace_back(p.first, sizeof(key_type) + 4 + 8 * shards_num);
            auto& req = reqs.back();
            BinaryArchive ar;
            req << request_data.key;
            req << shards_num;
            for (int32_t shard_id: p.second) {
                int32_t shard_slices_num = request_data.shards_slices[shard_id].size();
                node_slices_num += shard_slices_num;
                req << shard_id << shard_slices_num;
                for (int32_t slice_id: request_data.shards_slices[shard_id]) {
                    req << slice_id;
                    slice_key_type slice_key(request_data.key, slice_id);
                    push_value_type push_val;
                    apply_push_arg(slice_key, request_data.partitioned.parts[slice_id], push_val);
                    //lazy部分不支持压缩
                    if (_compress_info._enabled) {
                        ar << push_val;
                    } else {
                        req.lazy() << std::move(push_val);
                    }
                }
            }
            if (_compress_info._enabled) {
                ps_serialize(req.lazy(), _compress_info, std::move(ar));
            }
            if (node_slices_num == 0) {
                reqs.pop_back();
            }
        }
    }

    virtual void generate_store_request(RuntimeInfo& rt, std::vector<PSRequest>& reqs) override {
        for (const auto& p: rt.nodes()) {
            for (int32_t shard_id: p.second) {
                reqs.emplace_back(p.first);
                reqs.back() << shard_id;
            }
        }
    }

    virtual void apply_async_push_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          Storage* /*incr_storage*/,
          PSResponse& resp) override {
        VTIMER(1, framework, dense push apply async push request, ms);
        BinaryArchive ar;
        if (_compress_info._enabled) {
            ps_deserialize(req.lazy(), _compress_info, ar);
        }
        
        key_type key;
        req >> key;
        int32_t shards_num;
        req >> shards_num;
        
        size_t inserted = 0;
        bool healthy = ShardStorageMemory::singleton().test_healthy();
        auto& st = *static_cast<storage_type*>(storage);
        core::shared_lock_guard<storage_type> l(st);
        while (shards_num--) {
            int32_t shard_id, shard_slices_num;
            req >> shard_id >> shard_slices_num;  
            SCHECK(rt.local_shards().count(shard_id) != 0) 
                    << "Bad Request: invalid shard_id = " << shard_id;
            
            auto& shard = *(st.get(shard_id));
            core::lock_guard<ShardData> ls(shard);
            auto& ht = *boost::any_cast<shard_type>(&shard.data);
            auto& svec = *boost::any_cast<vector_type>(&shard.data_vector);
            if (!svec.empty()) {
                healthy = false;
            }

            while (shard_slices_num--) {
                int32_t slice_id;
                req >> slice_id;
                push_value_type push_val;

                //lazy部分不支持压缩
                if (_compress_info._enabled) {
                    ar >> push_val;
                } else {
                    req.lazy() >> push_val;
                }
                slice_key_type slice_key(key, slice_id);
                auto it = ht.find(slice_key);
                if (it == ht.end()) {
                    value_type store_val;
                    if (init_store_value(slice_key, push_val, store_val)) {
                        inserted += pico_serialized_size(store_val) + 8;
                        SCHECK(safe_insert(ht, slice_key, std::move(store_val), healthy));
                    }
                } else {
                    store_push_value(slice_key, push_val, it->second);
                }
            }
        }
        ShardStorageMemory::singleton().watch_memory(inserted);

        SCHECK(req.archive().is_exhausted());
        resp = PSResponse(req);
        resp << Status();
    }

    virtual void apply_sync_push_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp) override {
        VTIMER(1, framework, dense push apply sync push request, ms);
        BinaryArchive ar;
        if (_compress_info._enabled) {
            ps_deserialize(req.lazy(), _compress_info, ar);
        }
        
        key_type key;
        req >> key;
        int32_t shards_num;
        req >> shards_num;

        auto& st = *static_cast<delta_storage_type*>(storage);
        core::shared_lock_guard<delta_storage_type> l(st);
        while (shards_num--) {
            int32_t shard_id, shard_slices_num;
            req >> shard_id >> shard_slices_num;  
            SCHECK(rt.local_shards().count(shard_id) != 0) 
                    << "Bad Request: invalid shard_id = " << shard_id;

            auto& shard = *(st.get(shard_id));
            shard.lock();
            auto& ht = *boost::any_cast<delta_shard_type>(&shard.data);

            while (shard_slices_num--) {
                int32_t slice_id;
                req >> slice_id;
                push_value_type push_val;

                //lazy部分不支持压缩
                if (_compress_info._enabled) {
                    ar >> push_val;
                } else {
                    req.lazy() >> push_val;
                }
                slice_key_type slice_key(key, slice_id);
                auto it = ht.find(slice_key);
                if (it == ht.end()) {
                    ht.insert({slice_key, push_value_type()});
                    it = ht.find(slice_key);
                }
                merge_push_value(slice_key, push_val, it->second);
            }
            shard.unlock();
        }

        SCHECK(req.archive().is_exhausted());
        resp = PSResponse(req);
        resp << Status();
    }

    virtual void apply_store_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* delta_storage,
          Storage* storage,
          Storage* /*incr_storage*/,
          std::function<void(PSResponse&&)> send_response) override {
        VTIMER(1, framework, dense apply store request, ms);
        int32_t shard_id;
        req >> shard_id;
        SCHECK(req.archive().is_exhausted());
        SCHECK(rt.shards().count(shard_id))
            << "Bad Request: invalid shard_id = " << shard_id;

        if (delta_storage == nullptr) {
            PSResponse resp(req);
            resp << Status();
            send_response(std::move(resp));
            return;
        }

        size_t inserted = 0;
        bool healthy = ShardStorageMemory::singleton().test_healthy();

        auto& st = *static_cast<storage_type*>(storage);
        auto& delta_st = *static_cast<delta_storage_type*>(delta_storage);
        
        core::shared_lock_guard<storage_type> l(st);
        core::shared_lock_guard<delta_storage_type> dl(delta_st);

        auto& shard = *(st.get(shard_id));
        auto& delta_shard = *(delta_st.get(shard_id));

        core::lock_guard<ShardData> ls(shard);
        core::lock_guard<ShardData> dls(delta_shard);
        
        auto& ht = *boost::any_cast<shard_type>(&shard.data);
        auto& svec = *boost::any_cast<vector_type>(&shard.data_vector);
        auto& delta_shard_ht = *boost::any_cast<delta_shard_type>(&delta_shard.data);
        
        PSResponse resp(req);
        resp << Status();
        send_response(std::move(resp));

        for (auto& item: delta_shard_ht) {
            auto it = ht.find(item.first);
            if (it == ht.end()) {
                value_type store_val;
                if (init_store_value(item.first, item.second, store_val)) {
                    inserted += pico_serialized_size(store_val) + 8;
                    if (!safe_insert(ht, item.first, std::move(store_val), healthy)) {
                        svec.emplace_back(item.first, std::move(store_val));
                    }
                }
            } else {
                store_push_value(item.first, item.second, it->second);
            }
        }
        delta_storage_type::clear_map(delta_shard_ht);
        
        SCHECK(req.archive().is_exhausted());
        ShardStorageMemory::singleton().watch_memory(inserted);
    }

    virtual void apply_response(PSResponse& resp) override {
        Status st;
        resp >> st;
        SCHECK(resp.archive().is_exhausted());
        SCHECK(st.ok()) << st.ToString();
    }

    virtual std::unique_ptr<Storage> create_delta_storage(RuntimeInfo& rt) override {
        return std::make_unique<delta_storage_type>(rt.local_shards(), _config);
    }

    virtual std::unique_ptr<Storage> create_incr_storage(RuntimeInfo& /*rt*/) override {
        SLOG(FATAL);
        return nullptr;
    }

    /*! \brief   用户自定义函数，client 使用*/
    virtual void init_push_value(const slice_key_type&, push_value_type& val) {
        val = push_value_type();
    }

    /*! \brief   用户自定义函数，client 使用*/
    virtual void apply_push_arg(const slice_key_type&, const push_arg_type&, push_value_type&)
          = 0;

    /*! \brief   用户自定义函数，server 使用*/
    virtual bool init_store_value(const slice_key_type&, const push_value_type&, value_type&)
          = 0;

    /*! \brief   用户自定义函数，server 使用*/
    virtual void merge_push_value(const slice_key_type&, const push_value_type&, push_value_type&)
          = 0;

    /*! \brief   用户自定义函数，server 使用*/
    virtual void store_push_value(const slice_key_type&, const push_value_type&, value_type&)
          = 0;


protected:
    struct DenseTablePushRequestData: PushRequestData {
        key_type key;
        typename VAL_PARTITIONER::PartitionResult partitioned;
        std::vector<pico::vector<int32_t>> shards_slices;
        void init(size_t shard_num) {
            shards_slices.resize(shard_num);
            for (auto& shard_slices : shards_slices) {
                shard_slices.clear();
            }
        }
    };

    CompressInfo _compress_info;
    PARTITIONER _slice_key_partitioner;
    VAL_PARTITIONER _value_partitioner;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_OPERATOR_DENSEPUSHOPERATOR_H
