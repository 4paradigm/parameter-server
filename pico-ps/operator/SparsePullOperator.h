#ifndef PARADIGM4_PICO_PS_OPERATOR_SPARSEPULLOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_SPARSEPULLOPERATOR_H

#include "pico-ps/operator/PullOperator.h"
#include "pico-ps/common/EasyHashMap.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

/*!
 *  \brief 针对 SparseTable 的 PullOperator。
 *
 *         KEY sparsetable 中的索引。
 *         VALUE sparsetable server端存储的内容。
 *         PULL_ARG client 端回填的内容类型，比如参数的权重。
 *         PULL_VALUE server 端返回的内容类型，也是在 server 与 client 之间传输的数据类型。
 *         PARTITIONER KEY->shard_id 的映射。
 *         STORAGE server 端存储 pair<KEY,VALUE> 的数据结构。
 *
 *         appliaction developer 需要实现如下 3 个函数，其中 init_pull_value 有默认实现。
 *         store_pull_value() 函数为 client 使用，
 *         init_pull_value(), fetch_pull_value() 两个函数为 server 使用。
 *
 *         * bool init_store_value(const key_type&,  value_type& store_value) {
 *              store_value = value_type();
 *              return false
 *           }
 *          初始化 server 端的 VALUE 内容
 *
 *          返回值 True 表示在 PS 没有查询到此 key 将 store_value 初始化结果插入至 PS 表中，
 *                 False 表示对 PS 表不做修改。
 *
 *         * void fetch_pull_value(const key_type&, const value_type&, pull_value_type&);
 *         server 在 STORAGE 的 VALUE 类型中取出 PULL_VALUE 类型。
 *
 *         * void store_pull_value(const key_type&, const pull_value_type&, pull_arg_type&);
 *         client 将 PULL_VALUE 的内容应用至 PULL_ARG 类型。
 *
 */
template <typename KEY,
      typename VALUE,
      typename PULL_ARG,
      typename PULL_VALUE = PULL_ARG,
      typename PARTITIONER = DefaultPartitioner<KEY>,
      typename STORAGE = GoogleDenseHashMapShardStorage<KEY, VALUE>>
class SparseTablePullOperator : public PullOperator {
public:
    static_assert(std::is_same<KEY, typename PARTITIONER::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Partitioner.");
    static_assert(std::is_same<KEY, typename STORAGE::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Storage.");
    static_assert(std::is_same<VALUE, typename STORAGE::value_type>::value,
          "VALUE type of Operator is not the same as VALUE type of Storage.");

    typedef KEY key_type;
    typedef VALUE value_type;
    typedef PULL_ARG pull_arg_type;
    typedef PULL_VALUE pull_value_type;

    typedef STORAGE storage_type;
    typedef typename storage_type::shard_type shard_type;
    typedef typename storage_type::vector_type vector_type;
    typedef typename storage_type::accessor_type accessor_type;
    typedef EasyHashMap<key_type, pull_value_type, PARTITIONER> ShardMap;
        
    SparseTablePullOperator(const Configure& config) : PullOperator(config) {
        initialize_compress_info(config, "PullOperator", _compress_info);
        _algo = initialize_shard_pick_algo(config);
        if (config.has("max_request_merge_num")) {
            _max_request_merge_num = config["max_request_merge_num"].as<int32_t>();
        }
    }

    virtual ~SparseTablePullOperator() {}

    SparseTablePullOperator(SparseTablePullOperator&&) = default;
    SparseTablePullOperator& operator=(SparseTablePullOperator&&) = default;

    virtual void generate_request_data(core::vector<std::unique_ptr<PullItems>>& pull_items,
          RuntimeInfo& rt,
          std::unique_ptr<PullRequestData>& pull_request_data) override {
        VTIMER(1, framework, sparse pull generate request unique, ms);
        if (pull_request_data == nullptr) {
            pull_request_data = std::make_unique<SparseTablePullRequestData>();
        }
        auto& request_data = *static_cast<SparseTablePullRequestData*>(pull_request_data.get());
        int global_shard_num = rt.global_shard_num();
        request_data.init(global_shard_num);
        for (auto& items : pull_items) {
            auto& sparse_pull_items =
                *static_cast<SparsePullItems<KEY, pull_arg_type>*>(items.get());
            for (size_t i = 0; i < sparse_pull_items.n; ++i) {
                const key_type& key = sparse_pull_items.keys[i];
                if (!request_data.whole_map.count(key)) {
                    request_data.whole_map.force_emplace(key);
                    int32_t shard_id = _key_partitioner.partition(key, global_shard_num);
                    request_data.shards_keys[shard_id].push_back(key);
                }
            }
        }
        request_data.item_num = request_data.whole_map.size();
        if (std::is_trivially_copyable<key_type>::value &&
            std::is_trivially_copyable<value_type>::value) {
            request_data.item_size = sizeof(key_type) + sizeof(value_type);
        } else {
            request_data.item_size = 0;
        }     
    }

    virtual Status generate_request(std::vector<PullRequestData*>& pull_request_data,
          RuntimeInfo& rt,
          std::vector<PSRequest>& reqs) override {
        auto& center = *static_cast<SparseTablePullRequestData*>(pull_request_data[0]);
        VTIMER(1, framework, sparse pull generate request serialize, ms);

        std::vector<int> selected_nodes = rt.pick_one_replica(_algo);
        for (int32_t shard_id = 0; shard_id < rt.global_shard_num(); ++shard_id) {
            int node_id = selected_nodes[shard_id];
            if (node_id == -1) {
                return Status::NoReplica("");
            } else {
                center.node_shards[node_id].push_back(shard_id);
            }
        }

        for (auto& p: rt.nodes()) {
            int node_id = p.first;
            int32_t shards_num = center.node_shards[node_id].size();
            int32_t merge_num = pull_request_data.size();
            reqs.emplace_back(node_id, 12 + merge_num * 4 + shards_num * merge_num * 8);
            auto& req = reqs.back();
            req << shards_num << merge_num;
            int32_t node_keys_num = 0;
            for (int32_t shard_id: center.node_shards[node_id]) {
                req << shard_id;
                for (PullRequestData* data: pull_request_data) {
                    auto& request_data = *static_cast<SparseTablePullRequestData*>(data);
                    BinaryArchive keys_ar = vector_rpc_view(request_data.shards_keys[shard_id]);
                    int32_t shard_keys_num = request_data.shards_keys[shard_id].size();
                    req << shard_keys_num;
                    node_keys_num += shard_keys_num;
                    if (shard_keys_num != 0) {
                        ps_serialize(req.lazy(), _compress_info, std::move(keys_ar));
                    }
                }
            }
            if (node_keys_num == 0) {
                reqs.pop_back();
            }
        }
        return Status();
    }

    void apply_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp) override {
        VTIMER(1, framework, sparse pull apply request, ms);

        BinaryArchive keys_ar;
        int32_t shards_num, merge_num;
        req >> shards_num >> merge_num;
        static thread_local pico::vector<BinaryArchive> values_ars;
        values_ars.resize(merge_num, true);
        for (auto& values_ar: values_ars) {
            values_ar.clear();
        }
        resp = PSResponse(req, 4 + shards_num * 8);
        resp << shards_num;
        size_t inserted = 0;
        bool healthy = true;
        bool consistent = true;
        if (!_read_only) {
            healthy = ShardStorageMemory::singleton().test_healthy();
        }
        auto& st = *(static_cast<storage_type*>(storage));
        core::shared_lock_guard<storage_type> l(st);
        while (shards_num--) {
            int32_t shard_id;
            req >> shard_id;
            resp << shard_id;

            SCHECK(rt.local_shards().count(shard_id) != 0) 
                  << "Bad Request: invalid shard_id = " << shard_id;
            auto& shard = *(st.get(shard_id));
            if (inserted != 0) {
                shard.lock();
            } else {
                shard.lock_shared();
            }
            shard_type& ht = *boost::any_cast<shard_type>(&shard.data);
            vector_type& svec = *boost::any_cast<vector_type>(&shard.data_vector);
            if (!svec.empty()) {
                healthy = false;
                consistent = false;
                ShardStorageMemory::singleton().set_healthy(false);
            }
            for (int i = 0; i < merge_num; ++i) {
                int32_t shard_keys_num;
                req >> shard_keys_num;
                if (shard_keys_num == 0) {
                    continue;
                }
                ps_deserialize(req.lazy(), _compress_info, keys_ar);

                while (shard_keys_num--) {
                    key_type key;
                    ps_deserialize(keys_ar, key);
                    pull_value_type pull_val;
                    accessor_type it;
                    if (lock_find(ht, key, &it)) {
                        fetch_pull_value(key, it->second, pull_val);
                    } else {
                        value_type store_val;
                        bool insert = init_store_value(key, store_val);
                        fetch_pull_value(key, store_val, pull_val);
                        if (insert && !_read_only) {
                            if (inserted == 0) {
                                shard.upgrade();                      
                            }
                            inserted += pico_serialized_size(store_val) + 8;
                            safe_insert(ht, key, std::move(store_val), healthy);
                        }
                    }
                    ps_serialize(values_ars[i], pull_val);
                }
            }
            if (inserted != 0) {
                shard.unlock();
            } else {
                shard.unlock_shared();
            }
        }
        ShardStorageMemory::singleton().watch_memory(inserted);
        
        if (healthy && consistent) {
            for (auto& values_ar: values_ars) {
                int len = values_ar.capacity();
                ps_serialize(resp.lazy(), _compress_info, std::move(values_ar));
                values_ar = BinaryArchive(true);
                values_ar.reserve(len);
            }
        } else {
            resp = PSResponse(req);
            resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
            if (consistent) {
                resp << Status::OOM("PServer OOM during pull");
            } else {
                resp << Status::OOM("PServer OOM during sparse async push or store");
            }
        }
    }

    void apply_response(PSResponse& resp,
          std::vector<PullRequestData*>& pull_request_data) override {
        VTIMER(1, framework, sparse pull apply response serialize, ms);

        int32_t shards_num;
        resp >> shards_num;
        while (shards_num--) {
            int32_t shard_id;
            resp >> shard_id;
            for (PullRequestData* data: pull_request_data) {
                auto& request_data = *static_cast<SparseTablePullRequestData*>(data);
                request_data.resp_shard_ids.push_back(shard_id);
            }
        }
        for (PullRequestData* data: pull_request_data) {
            auto& request_data = *static_cast<SparseTablePullRequestData*>(data);
            BinaryArchive values_ar;
            ps_deserialize(resp.lazy(), _compress_info, values_ar);
            request_data.resp_values_ars.push_back(std::move(values_ar));
        } 
    }

    virtual void fill_pull_items(PullRequestData* pull_request_data,
          core::vector<std::unique_ptr<PullItems>>& pull_items) {
        VTIMER(1, framework, sparse pull apply response fill, ms);
        auto& request_data = *static_cast<SparseTablePullRequestData*>(pull_request_data);
        size_t resp_id = 0;
        for (int32_t shard_id: request_data.resp_shard_ids) {
            while (resp_id < request_data.resp_values_ars.size() &&
                  request_data.resp_values_ars[resp_id].is_exhausted()) {
                ++resp_id;
            }
            
            for (key_type& key: request_data.shards_keys[shard_id]) {
                ps_deserialize(request_data.resp_values_ars[resp_id],
                      request_data.whole_map.at(key));
            }
        }
        for (auto& items : pull_items) {
            auto& sparse_pull_items
                = *static_cast<SparsePullItems<KEY, pull_arg_type>*>(items.get());
            for (size_t i = 0; i < sparse_pull_items.n; ++i) {
                auto key = sparse_pull_items.keys[i];
                store_pull_value(key,
                        request_data.whole_map.at(key),
                        sparse_pull_items.pull_args[i]);
            }
        }
    }

    /*! \brief   用户自定义函数, server 端调用*/
    virtual bool init_store_value(const key_type&, value_type& store_value) {
        store_value = value_type();
        return false;
    }

    /*! \brief   用户自定义函数, server 端调用*/
    virtual void fetch_pull_value(const key_type&, const value_type&, pull_value_type&) = 0;

    /*! \brief   用户自定义函数, client 端调用*/
    virtual void store_pull_value(const key_type&, const pull_value_type&, pull_arg_type&) = 0;

protected:
    /*!
     *  \brief  根据 key 划分 shard 后，将 PushItems 中的 key 按照 ps_node_id, shard_id, key
     *          的顺序排序
     */
    struct SparseTablePullRequestData: PullRequestData {
        SparseTablePullRequestData(): whole_map(key_type::empty_key()) {}
        
        void init(size_t shard_num) {
            shards_keys.resize(shard_num);
            for (auto& keys : shards_keys) {
                keys.clear();
            }
            whole_map.clear();
            for(auto& p: node_shards){
                p.second.clear();
            }

            resp_shard_ids.clear();
            resp_values_ars.clear();
        }

        ShardMap whole_map;
        pico::vector<RpcVector<key_type>> shards_keys;
        std::unordered_map<int, pico::vector<int32_t>> node_shards;

        pico::vector<int32_t> resp_shard_ids;
        pico::vector<BinaryArchive> resp_values_ars;
    };

    CompressInfo _compress_info;
    PARTITIONER _key_partitioner;
    PickAlgo _algo;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
