#ifndef PARADIGM4_PICO_PS_OPERATOR_DENSEPULLOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_DENSEPULLOPERATOR_H

#include "pico-ps/operator/PullOperator.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

template <typename SLICE_KEY,
      typename VALUE,
      typename PULL_ARG,
      typename PULL_VALUE,
      typename PARTITIONER,
      typename VAL_MERGER,
      typename STORAGE>
class DenseTablePullOperator: public PullOperator {
public:
    static_assert(std::is_same<SLICE_KEY, typename PARTITIONER::key_type>::value,
          "SLICE_KEY type of Operator is not the same as KEY type of Partitioner.");
    static_assert(std::is_same<SLICE_KEY, typename STORAGE::key_type>::value,
          "SLICE_KEY type of Operator is not the same as KEY type of Storage.");
    static_assert(std::is_same<VALUE, typename STORAGE::value_type>::value,
          "VALUE type of Operator is not the same as VALUE type of Storage.");
    static_assert(std::is_same<PULL_ARG, typename VAL_MERGER::value_type>::value,
          "PULL_ARG type of Operator is not the same as value type of VAL_MERGER.");
 
    typedef SLICE_KEY slice_key_type;
    typedef typename slice_key_type::key_type key_type;
    typedef VALUE value_type;
    typedef PULL_ARG pull_arg_type;
    typedef PULL_VALUE pull_value_type;

    typedef STORAGE storage_type;
    typedef typename storage_type::shard_type shard_type;
    typedef typename storage_type::vector_type vector_type;

    DenseTablePullOperator(const Configure& config) : PullOperator(config) {
        initialize_compress_info(config, "PullOperator", _compress_info);
        _algo = initialize_shard_pick_algo(config);
    }

    virtual ~DenseTablePullOperator() {}

    DenseTablePullOperator(DenseTablePullOperator&&) = default;
    DenseTablePullOperator& operator=(DenseTablePullOperator&&) = default;

    virtual void generate_request_data(core::vector<std::unique_ptr<PullItems>>& pull_items,
          RuntimeInfo& rt,
          std::unique_ptr<PullRequestData>& pull_request_data) override {
        SCHECK(pull_items.size() == 1) << pull_items.size();
        
        VTIMER(1, framework, dense pull generate request partition, ms);
        if (pull_request_data == nullptr) {
            pull_request_data = std::make_unique<DenseTablePullRequestData>();
        }
        auto& request_data = *static_cast<DenseTablePullRequestData*>(pull_request_data.get());
        request_data.init(rt.global_shard_num());

        auto key = static_cast<DensePullItems<key_type, pull_arg_type>*>(pull_items[0].get())
                        ->key;
        auto& pull_arg
              = *(static_cast<DensePullItems<key_type, pull_arg_type>*>(pull_items[0].get())
                        ->pull_arg);

        request_data.key = key;
        request_data.pull_arg = &pull_arg;
        int32_t slices_num = _val_merger.slice_number(key, pull_arg);
        for (int32_t slice_id = 0; slice_id < slices_num; ++slice_id) {
            slice_key_type slice_key(key, slice_id);
            int32_t shard_id = _slice_key_partitioner.partition(
                    slice_key, rt.global_shard_num());
            request_data.shards_slices[shard_id].push_back(slice_id);
        }
    }
    
    virtual Status generate_request(std::vector<PullRequestData*>& pull_request_data,
          RuntimeInfo& rt,
          std::vector<PSRequest>& reqs) override {
        SCHECK(pull_request_data.size() == 1) << pull_request_data.size();

        VTIMER(1, framework, dense pull generate request serialize, ms);
        auto& request_data = *static_cast<DenseTablePullRequestData*>(pull_request_data[0]);
        std::vector<int> selected_nodes = rt.pick_one_replica(_algo);
        for (int32_t shard_id = 0; shard_id < rt.global_shard_num(); ++shard_id) {
            int node_id = selected_nodes[shard_id];
            if (node_id == -1) {
                return Status::NoReplica("");
            } else {
                request_data.node_shards[node_id].push_back(shard_id);
            }
        }

        for (auto& p: rt.nodes()) {
            int node_id = p.first;
            int32_t node_slices_num = 0;
            int32_t shards_num = request_data.node_shards[node_id].size();
            reqs.emplace_back(node_id, sizeof(key_type) + 8 + 8 * shards_num);
            auto& req = reqs.back();
            req << request_data.key;
            req << shards_num;
            for (int32_t shard_id: request_data.node_shards[node_id]) {
                int32_t shard_slices_num = request_data.shards_slices[shard_id].size();
                node_slices_num += shard_slices_num;
                req << shard_id << shard_slices_num;
                for (int32_t slice_id: request_data.shards_slices[shard_id]) {
                    req << slice_id;
                }
            }
            if (node_slices_num == 0) {
                reqs.pop_back();
            }
        }
        return Status();
    }

    virtual void apply_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp) override {
        VTIMER(1, framework, dense pull apply request, ms);
        
        BinaryArchive ar;

        key_type key;
        req >> key;
        int32_t shards_num;
        req >> shards_num;

        resp = PSResponse(req, 4 + 4 * shards_num);
        resp << shards_num;

        bool healthy = true;
        auto& st = *static_cast<storage_type*>(storage);
        core::shared_lock_guard<storage_type> l(st);
        while (shards_num--) {
            int32_t shard_id, shard_slices_num;
            req >> shard_id >> shard_slices_num;
            resp << shard_slices_num;
            SCHECK(rt.local_shards().count(shard_id) != 0) 
                    << "Bad Request: invalid shard_id = " << shard_id;
            auto& shard = *(st.get(shard_id));
            core::shared_lock_guard<ShardData> ls(shard);
            
            auto& ht = *boost::any_cast<shard_type>(&shard.data);
            auto& svec = *boost::any_cast<vector_type>(&shard.data_vector);
            if (!svec.empty()) {
                healthy = false;
                ShardStorageMemory::singleton().set_healthy(false);
            }

            for (int32_t i = 0; i < shard_slices_num; ++i) {
                int32_t slice_id;
                req >> slice_id;
                resp << slice_id;
                slice_key_type slice_key(key, slice_id);
                auto it = ht.find(slice_key);
                SCHECK(it != ht.end());
                pull_value_type pull_val;
                fetch_pull_value(slice_key, it->second, pull_val);

                //lazy部分不支持压缩
                if (_compress_info._enabled) {
                    ar << pull_val;
                } else {
                    resp.lazy() << std::move(pull_val);
                }
            }
        }
        if (healthy) {
            if (_compress_info._enabled) {
                ps_serialize(resp.lazy(), _compress_info, std::move(ar));
            }
        } else {
            resp = PSResponse(req);
            resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
            resp << Status::OOM("PServer OOM during dense async push or store");
        }
        SCHECK(req.archive().is_exhausted()) << req.archive().readable_length();
    }

    void apply_response(PSResponse& resp,
          std::vector<PullRequestData*>& pull_request_data) override {
        SCHECK(pull_request_data.size() == 1) << pull_request_data.size();
        VTIMER(1, framework, dense pull apply response, ms);
        
        auto& request_data = *static_cast<DenseTablePullRequestData*>(pull_request_data[0]);
        BinaryArchive ar;
        if (_compress_info._enabled) {
            ps_deserialize(resp.lazy(), _compress_info, ar);
        }
        int32_t slices_num = _val_merger.slice_number(
              request_data.key, *request_data.pull_arg);
        int32_t shards_num;
        resp >> shards_num;
        while (shards_num--) {
            int32_t shard_slices_num;
            resp >> shard_slices_num;
            while (shard_slices_num--) {
                int32_t slice_id;
                resp >> slice_id;
                SCHECK(0 <= slice_id && slice_id < slices_num) << slice_id;
                slice_key_type slice_key(request_data.key, slice_id);
                pull_value_type pull_val;
                //lazy部分不支持压缩
                if (_compress_info._enabled) {
                    ar >> pull_val;
                } else {
                    resp.lazy() >> pull_val;
                }
                pull_arg_type pull_arg_slice;
                store_pull_value(slice_key, pull_val, pull_arg_slice);
                _val_merger.merge(slice_key, *request_data.pull_arg, 
                      slices_num, pull_arg_slice, slice_id);
            }
        }
        SCHECK(resp.archive().is_exhausted()) << resp.archive().readable_length();
    }

    virtual void fill_pull_items(PullRequestData*,
          core::vector<std::unique_ptr<PullItems>>&) {}

    struct DenseTablePullRequestData: PullRequestData {
        void init(size_t shard_num) {
            shards_slices.resize(shard_num);
            for (auto& shard_slices : shards_slices) {
                shard_slices.clear();
            }
            for(auto& p: node_shards){
                p.second.clear();
            }
        }

        key_type key;
        pull_arg_type* pull_arg;
        pico::vector<pico::vector<int32_t>> shards_slices;
        std::unordered_map<int, pico::vector<int32_t>> node_shards;
    };

    /*! \brief   用户自定义函数, server 端调用*/
    virtual void fetch_pull_value(const slice_key_type&, const value_type&, pull_value_type&)
          = 0;

    /*! \brief   用户自定义函数, client 端调用*/
    virtual void store_pull_value(const slice_key_type&, pull_value_type&, pull_arg_type&) = 0;

protected:
    CompressInfo _compress_info;
    PARTITIONER _slice_key_partitioner;
    VAL_MERGER _val_merger;
    PickAlgo _algo;
};



} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif //PARADIGM4_PICO_PS_OPERATOR_DENSEPULLOPERATOR_H