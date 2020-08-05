#ifndef PARADIGM4_PICO_PS_OPERATOR_UPDATE_CONTEXT_OPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_UPDATE_CONTEXT_OPERATOR_H

#include "pico-ps/operator/SparsePushOperator.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

class PushItemsGenerator {
public:
    virtual ~PushItemsGenerator() {}
    virtual size_t generate_push_items(core::vector<std::unique_ptr<PushItems>>& push_items) = 0;
};

class BaseShardUpdateContextOperator : public Operator {
public:
    BaseShardUpdateContextOperator(const Configure& config) : Operator(config) {}

    virtual ~BaseShardUpdateContextOperator() {}

    BaseShardUpdateContextOperator(BaseShardUpdateContextOperator&&) = default;
    BaseShardUpdateContextOperator& operator=(BaseShardUpdateContextOperator&&)
          = default;

    virtual void generate_request(RuntimeInfo& old_rti,
          RuntimeInfo& new_rti,
          std::vector<PSRequest>& reqs)
          = 0;

    virtual void apply_response(PSResponse& resp) = 0;

    virtual void create_new_shards(RuntimeInfo& new_rti, Storage* storage) = 0;

    virtual void generate_shuffle_request(RuntimeInfo& old_rti, std::vector<PSRequest>& reqs)
          = 0;

    virtual void generate_purge_request(RuntimeInfo& old_rti, std::vector<PSRequest>& reqs) = 0;

    virtual void apply_purge_request(RuntimeInfo& new_rti,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp_ret)
          = 0;

    virtual std::unique_ptr<PushItemsGenerator> 
    lock_push_items_generator(RuntimeInfo& old_rti,
          RuntimeInfo& new_rti,
          Storage* storage,
          int32_t shard_id)
          = 0;

    virtual PushOperator* push_operator() = 0;
    // TODO: remove storage_id
    virtual std::unique_ptr<Storage> create_storage(RuntimeInfo& new_rti, int storage_id) = 0;
};

template <typename KEY,
      typename VALUE,
      typename PARTITIONER,
      typename STORAGE,
      typename PUSH_OP = DirectPushOperator<KEY, VALUE, PARTITIONER, STORAGE>>
class ShardUpdateContextOperator : public BaseShardUpdateContextOperator {
public:
    typedef KEY key_type;
    typedef VALUE value_type;
    typedef STORAGE storage_type;
    typedef typename STORAGE::shard_type shard_type;
    typedef typename STORAGE::vector_type vector_type;
    typedef typename STORAGE::iterator_type iterator_type;

    static_assert(std::is_same<key_type, typename STORAGE::key_type>::value,
          "KEY type of Operator is not the same as KEY type of Storage.");
    static_assert(std::is_same<value_type, typename STORAGE::value_type>::value,
          "VALUE type of Operator is not the same as VALUE type of Storage.");
    static_assert(std::is_same<storage_type, typename PUSH_OP::storage_type>::value,
          "storage type of Load Operator is not the same as storage type of Push Operator.");

    ShardUpdateContextOperator(const Configure& config)
        : BaseShardUpdateContextOperator(config), _push_op(config) {
        _algo = initialize_shard_pick_algo(config);
        if (config.has("block_serialized_size")) {
            _block_serialized_size = config["block_serialized_size"].as<uint32_t>();
        }
    }

    virtual ~ShardUpdateContextOperator() {}

    ShardUpdateContextOperator(ShardUpdateContextOperator&&) = default;
    ShardUpdateContextOperator& operator=(ShardUpdateContextOperator&&) = default;

    virtual void generate_request(RuntimeInfo& old_rt,
          RuntimeInfo& new_rt,
          std::vector<PSRequest>& reqs) override {
        std::unordered_set<comm_rank_t> nodes;
        for (auto item : old_rt.nodes()) {
            nodes.insert(item.first);
        }
        for (auto item : new_rt.nodes()) {
            nodes.insert(item.first);
        }
        for (auto node : nodes) {
            reqs.emplace_back(node);
        }
    }

    virtual void create_new_shards(RuntimeInfo& new_rt, Storage* storage) override {
        auto& st = *static_cast<storage_type*>(storage);
        for (auto& shard : new_rt.local_shards()) {
            st.create_shard(shard); 
        }
    }

    virtual void generate_shuffle_request(RuntimeInfo& old_rt,
          std::vector<PSRequest>& reqs) override {
        auto picked = old_rt.pick_one_replica(_algo);
        for (size_t i = 0; i < picked.size(); ++i) {
            SCHECK(picked[i] != -1);
            reqs.emplace_back(picked[i]);
            reqs.back() << int32_t(i);
        }
    }

    virtual void generate_purge_request(RuntimeInfo& old_rt,
          std::vector<PSRequest>& reqs) override {
        const auto& nodes = old_rt.nodes();
        for (const auto& it : nodes) {
            for (auto& shard: it.second) {
                reqs.emplace_back(it.first);
                reqs.back() << int32_t(shard);
            }
        }
    }

    virtual void apply_response(PSResponse& resp) override {
        Status st;
        resp >> st;
        SCHECK(resp.archive().is_exhausted()) << resp.archive().readable_length();
        SCHECK(st.ok()) << st.ToString();
    }

    virtual void apply_purge_request(RuntimeInfo& new_rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp) override {
        auto& st = *static_cast<storage_type*>(storage);
        int32_t shard_id;
        req >> shard_id;
        if (new_rt.local_shards().count(shard_id)) {
            core::shared_lock_guard<storage_type> l(st);
            auto& shard = *(st.get(shard_id));
            core::lock_guard<ShardData> ls(shard);
            auto& ht = *boost::any_cast<shard_type>(&shard.data);
            auto& svec = *boost::any_cast<vector_type>(&shard.data_vector);
            auto it = ht.begin();
            while (it != ht.end()) {
                if (_key_partitioner.partition(it->first, new_rt.global_shard_num()) == shard_id) {
                    ++it;
                } else {
                    key_type key(it->first);
                    ++it;
                    safe_erase(ht, key);
                }
            }

            size_t svec_size = 0;
            for (auto& item: svec) {
                if (_key_partitioner.partition(item.first, new_rt.global_shard_num()) == shard_id) {
                    svec[svec_size++] = std::move(item);
                }
            }
            svec.resize(svec_size);

            if (!safe_shrink(ht)) {
                SLOG(WARNING) << "no memory to shrink";
            }

            bool healthy = true;
            if (!svec.empty()) {
                for (auto& item: svec) {
                    safe_set(ht, item.first, std::move(item.second), healthy);
                }
                if (healthy) {
                    svec.clear();
                } else {
                    SLOG(WARNING) << "still OOM after shuffle";
                }
            }
        } else {
            st.erase_shard(shard_id);
        }
        resp = PSResponse(req);
        resp << Status();
    }

    //只push需要push的item，每次只push block_size个，否则update context过程中容易OOM。         
    virtual std::unique_ptr<ps::PushItemsGenerator> 
    lock_push_items_generator(RuntimeInfo& old_rt,
          RuntimeInfo& new_rt,
          Storage* storage,
          int32_t shard_id) override {
        auto it = std::make_unique<PushItemsGenerator>(
              old_rt, new_rt, storage, shard_id, _block_serialized_size, _key_partitioner);
        return it;
    }

    virtual PushOperator* push_operator() override {
        return &_push_op;
    }
    virtual std::unique_ptr<Storage> create_storage(RuntimeInfo& new_rti, int storage_id) override {
        // TODO: remove storage_id
        _config.node()["pmem_create_storage"]["storage_id"] = storage_id;
        return std::make_unique<STORAGE>(new_rti.local_shards(), _config);
    }
private:
    class PushItemsGenerator: public ps::PushItemsGenerator {
    public:
        PushItemsGenerator(RuntimeInfo& old_rt,
              RuntimeInfo& new_rt,
              Storage* storage, 
              int32_t shard_id, 
              uint32_t block_serialized_size,
              PARTITIONER& key_partitioner): 
                    _block_serialized_size(block_serialized_size), 
                    _key_partitioner(key_partitioner) {
            
            _shard_node_change = false;
            auto old_it = old_rt.shards().find(shard_id);
            auto new_it = new_rt.shards().find(shard_id);
            SCHECK(old_it != old_rt.shards().end());
            if (new_it != new_rt.shards().end()) {
                std::set<comm_rank_t> old_nodes(
                    old_it->second.begin(), old_it->second.end());
                for (comm_rank_t node_id: new_it->second) {
                    if (old_nodes.count(node_id) == 0) {
                        _shard_node_change = true;
                    }
                }
            }
            _global_shard_num = new_rt.global_shard_num();
            _storage = static_cast<storage_type*>(storage);
            _storage->lock_shared();
            _shard_id = shard_id;
            _shard = _storage->get(shard_id);
            _shard->lock_shared();
            auto ht = boost::any_cast<shard_type>(&_shard->data);
            auto svec = boost::any_cast<vector_type>(&_shard->data_vector);
            _it = ht->begin();
            _end = ht->end();
            _vit = svec->begin();
            _vend = svec->end();

        }
        virtual ~PushItemsGenerator() {
            _shard->unlock_shared();
            _storage->unlock_shared();
        }
        virtual size_t generate_push_items(core::vector<std::unique_ptr<PushItems>>& push_items) override {
            std::vector<key_type> keys;
            std::vector<value_type> vals;
            keys.clear();
            vals.clear();
            size_t serialized_size = 0;
            while (_it != _end || _vit != _vend) {
                std::pair<key_type, value_type> item;
                if (_it != _end) {
                    item = *_it;
                } else {
                    item = *_vit;
                }
                serialized_size += pico_serialized_size(item) + 8;
                if (serialized_size > _block_serialized_size && keys.size() > 0) {
                    break;
                }
                int32_t shard_id = _key_partitioner.partition(item.first, _global_shard_num);
                if (shard_id != _shard_id || _shard_node_change) {
                    keys.emplace_back(std::move(item.first));
                    vals.emplace_back(std::move(item.second));
                }
                if (_it != _end) {
                    ++_it;
                } else {
                    ++_vit;
                }
            }

            size_t count = keys.size();
            push_items.resize(1);
            push_items[0] = std::make_unique<SparsePushItems<key_type, value_type>>(
                std::move(keys), std::move(vals));
            return count;
        }
    private:
        uint32_t _block_serialized_size;
        PARTITIONER& _key_partitioner;
        
        bool _shard_node_change;
        int32_t _global_shard_num;
        storage_type* _storage;
        ShardData* _shard;
        int32_t _shard_id;
        int32_t _my_bucket_count;
        iterator_type _it, _end;
        typename vector_type::iterator _vit, _vend;
    };
    uint32_t _block_serialized_size = 1000000;
    PARTITIONER _key_partitioner;
    PUSH_OP _push_op;
    PickAlgo _algo;
};



} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_OPERATOR_UPDATE_CONTEXT_OPERATOR_H
