#ifndef PARADIGM4_PICO_PS_OPERATOR_SPARSEPUSHOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_SPARSEPUSHOPERATOR_H

#include "pico-ps/operator/PushOperator.h"
#include "pico-ps/common/EasyHashMap.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

/*!
 *  \brief 针对 SparseTable 的 PushOperator。
 *
 *         KEY sparsetable 中的索引。
 *         VALUE sparsetable server端存储的内容。
 *         PUSH_ARG client端计算产生的数据，例如某一个参数的梯度，PUSH_ARG 将被合并成为
 *                  PUSH_VALUE 再传输。
 *         PUSH_VALUE client 与 server 端之间传输的数据内容，是 PUSH_ARG
 *                    合并之后的内容，例如某个参数的梯度和。
 *         PARTITIONER KEY->shard_id 的映射。
 *         STORAGE server 端存储 pair<KEY,VALUE> 的数据结构。
 *         DELTA_STORAGE server 端暂存 pair<KEY,PUSH_VALUE>
 *                       的数据结构，用于同步训练模式。同步模式下 client 调用
 *                       push 传递到 server 处的 PUSH_VALUE 将被暂存于此，直到 client 调用
 *                       store 之后 DELTA_STORAGE
 *                       所存储的内容才会被更新至 STORAGE 之中。
 *
 *         appliaction developer 需要实现如下 5 个函数，其中 init_push_value 有默认实现。
 *         init_push_value(), apply_push_arg() 两个函数为 client 使用，
 *         init_store_value(), merge_push_value(), store_push_value() 三个函数为 server 使用。
 *
 *         在 async 训练模式下，server 只使用 init_store_value() 与 store_push_value()
 *         两个函数。
 *
 *         * void init_push_value(const key_type&, push_value_type& val) {
 *             val = push_value_type();
 *         }
 *         init_push_value() 在 client 端初始化某一个 key 所对应的 PUSH_VALUE 类型的数据。
 *
 *         * void apply_push_arg(const key_type&, const push_arg_type&, push_value_type&);
 *         apply_push_arg() 由 client 端调用，将 PUSH_ARG 类型的数据合并至 PUSH_VALUE
 *         类型的数据上， 例如在 mini_batch 训练时，同一个 key 在一组 mini_batch
 *         的数据中出现了多次，每出现一次将产生 一个 PUSH_ARG 类型的数据（即梯度），client
 *         调用本函数，将多个 PUSH_ARG 合并为一个 PUSH_VALUE（即梯度和）。
 *
 *         * bool init_store_value(const key_type&, const push_value_type&, value_type&);
 *         init_store_value() 由 server 调用，在将某个 PUSH_VALUE 插入至 STORAGE 时，若此 KEY
 *         并未存在，将调用此函数根据 PUSH_VALUE 的内容初始化一个 VALUE 存储在 STORAGE 中。
 *
 *         * void merge_push_value(const key_type&, const push_value_type&, push_value_type&);
 *         merge_push_value 由 server 调用，在 sync 训练模式下，将接收到的 PUSH_VALUE 合并至
 *         DELTA_STORAGE 中暂存。
 *
 *         * void store_push_value(const key_type&, const push_value_type&, value_type&);
 *         store_push_value 由 server 调用，在 async 训练模式下，接收到的 PUSH_VALUE 将直接
 *         通过调用此函数存储到 STORAGE 中的 VALUE 中；在 sync 训练模式下，在 store 阶段，
 *         server 将 DELTA_STORAGE 中暂存的 PUSH_VALUE 类型应用至 STORAGE 中的 VALUE 上。
 */
template <typename KEY,
      typename VALUE,
      typename PUSH_ARG,
      typename PUSH_VALUE = PUSH_ARG,
      typename PARTITIONER = DefaultPartitioner<KEY>,
      typename STORAGE = GoogleDenseHashMapShardStorage<KEY, VALUE>,
      typename DELTA_STORAGE = GoogleDenseHashMapShardStorage<KEY, PUSH_VALUE>,
      typename INCR_STORAGE = STORAGE>
class SparseTablePushOperator : public PushOperator {
public:
    static_assert(std::is_same<KEY, typename PARTITIONER::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Partitioner.");
    static_assert(std::is_same<KEY, typename STORAGE::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Storage.");
    static_assert(std::is_same<VALUE, typename STORAGE::value_type>::value,
          "VALUE type of Operator is not the same as VALUE type of Storage.");
    static_assert(std::is_same<KEY, typename DELTA_STORAGE::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Delta Storage.");
    static_assert(std::is_same<PUSH_VALUE, typename DELTA_STORAGE::value_type>::value,
          "PUSH_VALUE type of Operator is not the same as VALUE type of Delta Storage.");
    typedef KEY key_type;
    typedef VALUE value_type;
    typedef PUSH_ARG push_arg_type;
    typedef PUSH_VALUE push_value_type;

    typedef STORAGE storage_type;
    typedef typename storage_type::shard_type shard_type;
    typedef typename storage_type::vector_type vector_type;
    typedef typename storage_type::accessor_type accessor_type;
    typedef SizeMpscQueueShardStorage<key_type, push_value_type> delta_storage_type;
    typedef typename delta_storage_type::shard_type delta_shard_type;
    typedef typename delta_storage_type::vector_type delta_vector_type;
    typedef INCR_STORAGE incr_storage_type;
    typedef typename incr_storage_type::shard_type incr_shard_type;

    typedef EasyHashMap<key_type, push_value_type, PARTITIONER> ShardMap;

    SparseTablePushOperator(const Configure& config) : PushOperator(config) {
        initialize_compress_info(config, "PushOperator", _compress_info);
        if (config.has("max_request_merge_num")) {
            _max_request_merge_num = config["max_request_merge_num"].as<int32_t>();
        }
    }

    virtual ~SparseTablePushOperator() {}

    SparseTablePushOperator(SparseTablePushOperator&&) = default;
    SparseTablePushOperator& operator=(SparseTablePushOperator&&) = default;

    virtual void generate_request_data(core::vector<std::unique_ptr<PushItems>>& push_items,
          RuntimeInfo& rt,
          std::unique_ptr<PushRequestData>& push_request_data) override {
        VTIMER(1, framework, sparse push generate request unique, ms);

        if (push_request_data == nullptr) {
            push_request_data = std::make_unique<SparseTablePushRequestData>();
        }
        auto& request_data = *static_cast<SparseTablePushRequestData*>(push_request_data.get());
        int global_shard_num = rt.global_shard_num();
        request_data.init(global_shard_num);
        auto& whole_map = request_data.whole_map;
            
        size_t total = 0;
        for (auto& items : push_items) {
            auto& sparse_push_items
                = *static_cast<SparsePushItems<key_type, push_arg_type>*>(
                        items.get());
            total += sparse_push_items.n;
        }

        for (auto& items : push_items) {
            auto& sparse_push_items
                = *static_cast<SparsePushItems<key_type, push_arg_type>*>(
                        items.get());
            for (size_t i = 0; i < sparse_push_items.n; ++i) {
                auto& key = sparse_push_items.keys[i];
                auto rit = whole_map.try_emplace(key);
                if (rit.second) {
                    init_push_value(key, rit.first->second);
                }
                apply_push_arg(key, sparse_push_items.push_args[i], rit.first->second);
            }
        }
        for (auto& item: whole_map) {
            int32_t shard_id = _key_partitioner.partition(item.first, rt.global_shard_num());
            request_data.shards_items[shard_id].push_back(std::move(item));
        }
        request_data.item_num = request_data.whole_map.size();
        if (std::is_trivially_copyable<key_type>::value &&
            std::is_trivially_copyable<value_type>::value) {
            request_data.item_size = sizeof(key_type) + sizeof(value_type);
        } else {
            request_data.item_size = 0;
        }
    }

    virtual void generate_push_request(
          std::vector<PushRequestData*>& push_request_data,
          RuntimeInfo& rt,
          std::vector<PSRequest>& reqs) override {
        VTIMER(1, framework, sparse push generate request serialize, ms);
        for (auto& p: rt.nodes()) {
            int32_t shards_num = p.second.size();
            int32_t merge_num = push_request_data.size();
            reqs.emplace_back(p.first, 8 + shards_num * merge_num * 8);
            auto& req = reqs.back();
            req << shards_num << merge_num;
            int32_t node_items_num = 0;
            for (int32_t shard_id: p.second) {
                req << shard_id;
                for (auto data: push_request_data) {
                    auto& request_data = *static_cast<SparseTablePushRequestData*>(data);
                    int32_t shard_items_num = request_data.shards_items[shard_id].size();
                    req << shard_items_num;
                    node_items_num += shard_items_num;
                    if (shard_items_num != 0) {
                        BinaryArchive ar = vector_rpc_view(request_data.shards_items[shard_id]);
                        ps_serialize(req.lazy(), _compress_info, std::move(ar));
                    }
                }
            }
            if (node_items_num == 0) {
                reqs.pop_back();
            }
        }
    }

    virtual void generate_store_request(RuntimeInfo& rt,
          std::vector<PSRequest>& reqs) override {
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
          Storage* incr_storage,
          PSResponse& resp) override {
        VTIMER(1, framework, sparse apply async push request, ms);

        int32_t shards_num, merge_num;
        req >> shards_num >> merge_num;

        size_t inserted = 0;
        bool healthy = ShardStorageMemory::singleton().test_healthy();
        
        auto& st = *static_cast<storage_type*>(storage);
        core::shared_lock_guard<storage_type> l(st);
        while (shards_num--) {
            int32_t shard_id;
            req >> shard_id;

            SCHECK(rt.local_shards().count(shard_id) != 0) 
                  << "Bad Request: invalid shard_id = " << shard_id;

            auto& shard = *(st.get(shard_id));
            core::lock_guard<ShardData> ls(shard);
            auto& ht = *boost::any_cast<shard_type>(&shard.data);
            auto& svec = *boost::any_cast<vector_type>(&shard.data_vector);
            if (!svec.empty()) {
                healthy = false;
            }

            ShardData* incr_shard = nullptr;
            shard_type* incr_ht = nullptr;
            if (incr_storage) {
                auto& incr_st = *static_cast<incr_storage_type*>(incr_storage);
                incr_shard = incr_st[shard_id];
                incr_ht = boost::any_cast<incr_shard_type>(&incr_shard->data);
                //incr_shard->lock();
            }

            for (int i = 0; i < merge_num; ++i) {
                int32_t shard_items_num;
                req >> shard_items_num;
                if (shard_items_num == 0) {
                    continue;
                }
                BinaryArchive ar;
                ps_deserialize(req.lazy(), _compress_info, ar);
                
                while (shard_items_num--) {
                    std::pair<key_type, push_value_type> item;
                    ps_deserialize(ar, item);
                    accessor_type it;
                    if (!lock_find(ht, item.first, &it)) {
                        value_type store_val;
                        if (init_store_value(item.first, item.second, store_val)) {
                            inserted += pico_serialized_size(store_val) + 8;
                            if (incr_ht) {
                                bool allways_set = true;
                                safe_set(*incr_ht, item.first, store_val, allways_set);
                            }
                            if (!safe_insert(ht, item.first, std::move(store_val), healthy)) {
                                svec.emplace_back(item.first, std::move(store_val));
                            }
                        }
                    } else {
                        store_push_value(item.first, item.second, it->second);
                        if (incr_ht) {
                            bool allways_set = true;
                            safe_set(*incr_ht, item.first, it->second, allways_set);
                        }
                    }
                }
            }
        }

        SCHECK(req.archive().is_exhausted());
        ShardStorageMemory::singleton().watch_memory(inserted);
        resp = PSResponse(req);
        resp << Status();
    }

    virtual void apply_sync_push_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp) override {
        VTIMER(1, framework, sparse apply sync push request, ms);
        
        int32_t shards_num, merge_num;
        req >> shards_num >> merge_num;
        
        auto& st = *static_cast<delta_storage_type*>(storage);
        core::shared_lock_guard<delta_storage_type> l(st);
        while (shards_num--) {
            int32_t shard_id;
            req >> shard_id;

            SCHECK(rt.local_shards().count(shard_id) != 0) 
                  << "Bad Request: invalid shard_id = " << shard_id;

            auto& shard = *(st.get(shard_id));
            delta_shard_type& ht = *boost::any_cast<delta_shard_type>(&shard.data);

            for (int i = 0; i < merge_num; ++i) {
                int32_t shard_items_num;
                req >> shard_items_num;
                if (shard_items_num == 0) {
                    continue;
                }
                BinaryArchive ar;
                ps_deserialize(req.lazy(), _compress_info, ar);
                
                delta_vector_type block;
                block.reserve(shard_items_num);
                while (shard_items_num--) {
                    std::pair<key_type, push_value_type> item;
                    ps_deserialize(ar, item);
                    block.push_back(std::move(item));
                }
                ht.push(std::move(block));
            };
        }
        resp = PSResponse(req);
        resp << Status();
    }

    virtual void apply_store_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* delta_storage,
          Storage* storage,
          Storage* incr_storage,
          std::function<void(PSResponse&&)> send_response) override {  
        VTIMER(1, framework, sparse apply store request, ms);

        int32_t shard_id;
        req >> shard_id;
        SCHECK(req.archive().is_exhausted());
        SCHECK(rt.local_shards().count(shard_id))
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
        
        ShardData* incr_shard = nullptr;
        shard_type* incr_ht = nullptr;
        if (incr_storage) {
            auto& incr_st = *static_cast<incr_storage_type*>(incr_storage);
            incr_shard = incr_st[shard_id];
            incr_ht = boost::any_cast<incr_shard_type>(&incr_shard->data);
            //incr_shard->lock();
        }

        auto& shard = *(st.get(shard_id));
        auto& delta_shard = *(delta_st.get(shard_id));
        core::lock_guard<ShardData> sl(shard);
        
        auto& ht = *boost::any_cast<shard_type>(&shard.data);
        auto& svec = *boost::any_cast<vector_type>(&shard.data_vector);
        auto& delta_shard_ht = *boost::any_cast<delta_shard_type>(&delta_shard.data);
        size_t block_count = delta_shard_ht.size();

        PSResponse resp(req);
        resp << Status();
        send_response(std::move(resp));

        static thread_local ShardMap shard_map(key_type::empty_key());
   
        shard_map.clear();
        while (block_count--) {
            delta_vector_type block;
            while (!delta_shard_ht.pop(block));
            for (auto& item: block) {
                auto rit = shard_map.try_emplace(item.first, std::move(item.second));
                if (!rit.second) {
                    merge_push_value(item.first, item.second, rit.first->second);
                }
            }
        }

        for (auto& item: shard_map) {
            accessor_type it;
            if (!lock_find(ht, item.first, &it)) {
                value_type store_val;
                if (init_store_value(item.first, item.second, store_val)) {
                    inserted += pico_serialized_size(store_val) + 8;
                    if (incr_ht) {
                        bool allways_set = true;
                        safe_set(*incr_ht, item.first, store_val, allways_set);
                    }
                    if (!safe_insert(ht, item.first, std::move(store_val), healthy)) {
                        svec.emplace_back(item.first, std::move(store_val));
                    }
                }
            } else {
                store_push_value(item.first, item.second, it->second);
                if (incr_ht) {
                    bool allways_set = true;
                    safe_set(*incr_ht, item.first, it->second, allways_set);
                }
            }
        }
        //if (incr_shard) {
            //incr_shard->unlock();
        //}

        SCHECK(req.archive().is_exhausted());
        ShardStorageMemory::singleton().watch_memory(inserted);
    }

    virtual void apply_response(PSResponse& resp) override {
        Status status;
        resp >> status;
        SCHECK(resp.archive().is_exhausted());
        SCHECK(status.ok()) << status.ToString();
    }

    virtual std::unique_ptr<Storage> create_delta_storage(RuntimeInfo& runtime_info) override {
        return std::make_unique<delta_storage_type>(runtime_info.local_shards(), _config);
    }

    virtual std::unique_ptr<Storage> create_incr_storage(RuntimeInfo& runtime_info) override {
        return std::make_unique<incr_storage_type>(runtime_info.local_shards(), _config);
    }

    /*! \brief   用户自定义函数，client 使用*/
    virtual void init_push_value(const key_type&, push_value_type& val) {
        val = push_value_type();
    }

    /*! \brief   用户自定义函数，client 使用*/
    virtual void apply_push_arg(const key_type&, const push_arg_type&, push_value_type&) = 0;

    /*! \brief   用户自定义函数，server 使用*/
    virtual bool init_store_value(const key_type&, const push_value_type&, value_type&) = 0;

    /*! \brief   用户自定义函数，server 使用*/
    virtual void merge_push_value(const key_type&, const push_value_type&, push_value_type&) = 0;

    /*! \brief   用户自定义函数，server 使用*/
    virtual void store_push_value(const key_type&, const push_value_type&, value_type&) = 0;

protected:

    struct SparseTablePushRequestData: PushRequestData {
        SparseTablePushRequestData(): whole_map(key_type::empty_key()) {}
        
        void init(size_t shard_num) {
            shards_items.resize(shard_num);
            for (auto& items : shards_items) {
                items.clear();
            }
            whole_map.clear();
        }
        ShardMap whole_map;
        pico::vector<RpcVector<std::pair<key_type, push_value_type>>> shards_items;
    };

    CompressInfo _compress_info;
    PARTITIONER _key_partitioner;
};

/*!
 * \brief  将 PUSH_VALUE 直接作用于 Storage 的 SparsePushOperator，
 *         主要在 LoadOperator, UpdateContextOperator 中使用，不应在Learner中直接使用。
 */
template <typename KEY,
      typename VALUE,
      typename PARTITIONER,
      typename STORAGE,
      typename DELTA_STORAGE = STORAGE> //没有使用DELTA_STORAGE，仅仅用于实例化SparseTablePushOperator
class DirectPushOperator : public SparseTablePushOperator<KEY,
                                     VALUE,
                                     VALUE,
                                     VALUE,
                                     PARTITIONER,
                                     STORAGE,
                                     DELTA_STORAGE> {
public:
    static_assert(std::is_same<KEY, typename STORAGE::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Storage.");
    static_assert(std::is_same<VALUE, typename STORAGE::value_type>::value,
          "VALUE type of Operator is not the same as VALUE type of Storage.");
    
    typedef KEY key_type;
    typedef VALUE value_type;
    typedef VALUE push_value_type;
    typedef VALUE push_arg_type;
    typedef STORAGE storage_type;
    typedef typename storage_type::shard_type shard_type;

    DirectPushOperator(const Configure& config)
        : SparseTablePushOperator<KEY,
                VALUE,
                VALUE,
                VALUE,
                PARTITIONER,
                STORAGE,
                STORAGE>(config) {}

    virtual ~DirectPushOperator() {}

    DirectPushOperator(DirectPushOperator&&) = default;
    DirectPushOperator& operator=(DirectPushOperator&&) = default;

    virtual void apply_sync_push_request(RuntimeInfo&,
          PSRequest&,
          Storage*,
          PSResponse&) override {
        SLOG(FATAL) << "do not use this";
    }

    virtual void apply_async_push_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          Storage*,
          PSResponse& resp) override {
        VTIMER(1, framework, sparse apply_async_push_request, ms);

        resp = PSResponse(req);

        int32_t shards_num, merge_num;
        req >> shards_num >> merge_num;

        size_t inserted = 0;
        bool healthy = true;
        
        auto& st = *static_cast<storage_type*>(storage);
        core::shared_lock_guard<storage_type> l(st);
        while (shards_num--) {
            int32_t shard_id;
            req >> shard_id;

            SCHECK(rt.local_shards().count(shard_id) != 0) 
                  << "Bad Request: invalid shard_id = " << shard_id;

            auto& shard = *(st.get(shard_id));
            core::lock_guard<ShardData> ls(shard);
            auto& ht = *boost::any_cast<shard_type>(&shard.data);

            for (int i = 0; i < merge_num; ++i) {
                int32_t shard_items_num;
                req >> shard_items_num;
                if (shard_items_num == 0) {
                    continue;
                }
                BinaryArchive ar;
                ps_deserialize(req.lazy(), this->_compress_info, ar);
                      
                while (shard_items_num--) {
                    std::pair<key_type, push_value_type> item;
                    ps_deserialize(ar, item);
                    inserted += pico_serialized_size(item) + 8;
                    safe_set(ht, item.first, std::move(item.second), healthy);
                }        
            }
        }

        ShardStorageMemory::singleton().watch_memory(inserted);
        SCHECK(req.archive().is_exhausted());
        if (healthy) {
            resp << Status();
        } else {
            resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
            resp << Status::OOM("PServer OOM during direct push");
        }
    }

    virtual void apply_store_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* delta_storage,
          Storage* storage,
          Storage* /*incr_storage*/,
          std::function<void(PSResponse&&)> send_response) override {

        int32_t shard_id;
        req >> shard_id;
        SCHECK(req.archive().is_exhausted());
        SCHECK(rt.local_shards().count(shard_id))
            << "Bad Request: invalid shard_id = " << shard_id;
        
        if (delta_storage == nullptr) {
            PSResponse resp(req);
            resp << Status();
            send_response(std::move(resp));
            return;
        }

        size_t inserted = 0;
        bool healthy = true;
        
        auto& st = *static_cast<storage_type*>(storage);
        auto& delta_st = *static_cast<storage_type*>(delta_storage);
        
        core::shared_lock_guard<storage_type> l(st);
        core::shared_lock_guard<storage_type> dl(delta_st);

        auto& shard = *(st.get(shard_id));
        auto& delta_shard = *(delta_st.get(shard_id));

        core::lock_guard<ShardData> ls(shard);
        core::lock_guard<ShardData> dls(delta_shard);

        auto& ht = *boost::any_cast<shard_type>(&shard.data);
        auto& delta_shard_ht = *boost::any_cast<shard_type>(&delta_shard.data);

        for (auto& item: delta_shard_ht) {
            inserted += pico_serialized_size(item) + 8;
            safe_set(ht, item.first, std::move(item.second), healthy);
        }

        ShardStorageMemory::singleton().watch_memory(inserted);
        SCHECK(req.archive().is_exhausted());
        PSResponse resp(req);
        if (healthy) {
            resp << Status();
        } else {
            resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
            resp << Status::OOM("PServer OOM during direct store");
        }
        send_response(std::move(resp));
    }

    virtual void apply_push_arg(const key_type&,
          const push_arg_type& in,
          push_value_type& out) override {
        out = in;
    }

    virtual void merge_push_value(const key_type&,
          const push_value_type& in,
          push_value_type& out) override {
        out = in;
    }
    virtual bool init_store_value(const key_type&,
          const push_value_type& in,
          value_type& out) override {
        out = in;
        return true;
    }

    virtual void store_push_value(const key_type&,
          const push_value_type& in,
          value_type& out) override {
        out = in;
    }
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
