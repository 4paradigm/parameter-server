#ifndef PARADIGM4_PICO_PS_SYNC_OPERATOR_OPERATOR_H
#define PARADIGM4_PICO_PS_SYNC_OPERATOR_OPERATOR_H

#include "pico-ps/operator/Operator.h"
#include "pico-ps/common/message.h"
#include "pico-ps/common/common.h"
#include "pico-ps/operator/PushOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class StorageIterator {
public:
    virtual ~StorageIterator() {}

    virtual std::unique_ptr<StorageIterator> clone() = 0;
};

class SyncOperator : public Operator {
public:
    SyncOperator(const Configure& config) : Operator(config) {
        initialize_compress_info(config, "SyncOperator", _compress_info);
        _block_size = config.get<int>("block_size", 1000);
    }
    virtual ~SyncOperator() {}

    virtual void apply_sync_request(RuntimeInfo& rt,
            PSRequest& req,
            Storage* storage,
            PSResponse& resp_ret) = 0;

    virtual std::unique_ptr<StorageIterator> incr_iterator(Storage* incr_storage) = 0;

    virtual Status generate_sync_request(StorageIterator* incr_iter,
            RuntimeInfo& rt,
            std::vector<PSRequest>& reqs) = 0;

protected:
    CompressInfo _compress_info;
    int _block_size;
};


template <typename STORAGE>
class TemplateStorageIterator : public StorageIterator {
public:
    using storage_type = STORAGE;
    using table_type = typename storage_type::shard_table_t;
    using shard_type = typename storage_type::shard_type;
    using table_iterator_type = typename table_type::iterator;
    using shard_iterator_type = typename shard_type::iterator;

    TemplateStorageIterator(Storage* storage) {
        _st = static_cast<storage_type*>(storage);
        _table_it = _st->data().begin();
    }

    TemplateStorageIterator(const TemplateStorageIterator<STORAGE>& o) {
        _st = o._st;
        _shard = o._shard;
        _table_it = o._table_it;
        _shard_it = o._shard_it;
    }

    virtual std::unique_ptr<StorageIterator> clone() override {
        return std::make_unique<TemplateStorageIterator<STORAGE>>(*this);
    }

    shard_iterator_type* next() {
        if (_table_it == _st->data().end())
            return nullptr;
        else if (_shard == nullptr) {
            _shard = boost::any_cast<shard_type>(&_table_it->second->data);
            if (_shard == nullptr) {
                SLOG(FATAL) << "any cast " << _table_it->second->data.type().name() << " -> " << typeid(shard_type).name();
            }
            _shard_it = _shard->begin();
        } else {
            _shard_it++;
        }

        while (_shard_it == _shard->end()) {
            _table_it++;
            if (_table_it == _st->data().end())
                return nullptr;
            _shard = boost::any_cast<shard_type>(&_table_it->second->data);
            if (_shard == nullptr) {
                SLOG(FATAL) << "any cast " << _table_it->second->data.type().name() << " -> " << typeid(shard_type).name();
            }
            _shard_it = _shard->begin();
        }
        return &_shard_it;
    }
private:
    storage_type* _st = nullptr;
    shard_type* _shard = nullptr;
    table_iterator_type _table_it;
    shard_iterator_type _shard_it;
};

template <typename KEY,
         typename INCR_VALUE,
         typename SYNC_VALUE,
         typename PARTITIONER,
         typename INCR_STORAGE = GoogleDenseHashMapShardStorage<KEY, INCR_VALUE>,
         typename SYNC_STORAGE = GoogleDenseHashMapShardStorage<KEY, SYNC_VALUE>>
class KVSyncOperator : public SyncOperator {
public:
    typedef SYNC_STORAGE sync_storage_type;
    typedef typename sync_storage_type::shard_type sync_shard_type; 
    typedef INCR_STORAGE incr_storage_type;
    typedef typename incr_storage_type::shard_type incr_shard_type;
    typedef KEY key_type;
    typedef SYNC_VALUE sync_value_type;
    typedef INCR_VALUE incr_value_type;

    typedef typename incr_storage_type::shard_table_t table_iterator_type;
    typedef typename incr_shard_type::iterator shard_iterator_type;

public:
    KVSyncOperator(const Configure& config) : SyncOperator(config) {
    }

    virtual ~KVSyncOperator() {}

    virtual void apply_sync_request(RuntimeInfo& rt,
            PSRequest& req,
            Storage* storage,
            PSResponse& resp_ret) override {
        BinaryArchive* ar_ptr = &(req.archive());
        BinaryArchive tmp_ar;
        if (_compress_info._enabled) {
            BinaryArchive compressed_ar;
            req >> compressed_ar;
            _compress_info._compresser.raw_uncompress(compressed_ar, tmp_ar);
            ar_ptr = &tmp_ar;
        }
        BinaryArchive& ar = *ar_ptr;
        
        int32_t node_items;
        ar >> node_items;
        int32_t n_shards;
        ar >> n_shards;
        Status status = Status();
        while (n_shards--) {
            int32_t shard_id;
            ar >> shard_id;
            SCHECK(rt.local_shards().count(shard_id) != 0) 
                  << "Bad Request: invalid shard_id = " << shard_id;
            int32_t shard_items;
            ar >> shard_items;
            node_items -= shard_items;

            auto& st = *static_cast<sync_storage_type*>(storage);
            st.write_shard(shard_id, [&shard_items, &ar, &status, this](boost::any& any) {
                sync_shard_type* ht_ptr = boost::any_cast<sync_shard_type>(&any);
                if (ht_ptr == nullptr) {
                    SLOG(FATAL) << "any cast " << any.type().name() << " -> " << typeid(sync_shard_type).name();
                }
                sync_shard_type& ht = *ht_ptr;
                try {
                    for (int32_t i = 0; i < shard_items; ++i) {
                        ht[ar.get<key_type>()] = ar.get<sync_value_type>();
                    }
                } catch (std::bad_alloc&) {
                    //TODO rollback
                    status = Status::OOM("PServer OOM during sync op");
                    SLOG(WARNING) << "PServer OOM during sync op";
                }
            });
            if (!status.ok()) {
                node_items = 0;
            }
        }
        SCHECK(node_items == 0);
        SCHECK(ar.is_exhausted()) << ar.readable_length();
        PSResponse resp(req);
        resp << status;
        resp_ret = std::move(resp);
    }

    virtual void to_sync_value(INCR_VALUE& incr_value, SYNC_VALUE& sync_value) = 0;

    virtual std::unique_ptr<StorageIterator> incr_iterator(Storage* incr_storage) override {
        return std::make_unique<TemplateStorageIterator<incr_storage_type>>(incr_storage);
    }

    virtual Status generate_sync_request(StorageIterator* incr_iter,
            RuntimeInfo& rt,
            std::vector<PSRequest>& reqs) {
        std::vector<pico::vector<std::pair<KEY, SYNC_VALUE>>> sync_tables;
        sync_tables.resize(rt.global_shard_num());
        auto sit = static_cast<TemplateStorageIterator<incr_storage_type>*>(incr_iter);
        int cnt = 0;
        for (; cnt < _block_size; ++cnt) {
            auto it = sit->next();
            if (it == nullptr) {
                break;
            }
            auto shard_id = _key_partitioner.partition((*it)->first, rt.global_shard_num());
            SYNC_VALUE sync_val;
            to_sync_value((*it)->second, sync_val); 
            sync_tables[shard_id].emplace_back((*it)->first, std::move(sync_val));
        }

        if (cnt > 0) {
            return generate_request(sync_tables, rt, reqs);
        } else {
            return Status::Empty("");
        }
    }

private:
    Status generate_request(std::vector<pico::vector<std::pair<KEY, SYNC_VALUE>>>& tables,
            RuntimeInfo& rt,
            std::vector<PSRequest>& reqs) {
        for (auto& s : rt.shards()) {
            if (s.second.size() == 0)
                return Status::NoReplica("");
        }
        for (auto& p: rt.nodes()) {
            int32_t node_items = 0;
            for (int32_t shard_id: p.second) {
                node_items += tables[shard_id].size();
            }
            size_t reserve_size = sizeof(int32_t) * (2 + p.second.size() * 2)
                  + (sizeof(KEY) + sizeof(SYNC_VALUE)) * node_items;
            reqs.emplace_back(p.first, reserve_size);

            BinaryArchive* ar_ptr = &(reqs.back().archive());
            BinaryArchive tmp_ar;
            if (_compress_info._enabled) {
                tmp_ar.reserve(reserve_size);
                ar_ptr = &tmp_ar;
            }
            BinaryArchive& ar = *ar_ptr;

            ar << node_items;
            ar << static_cast<int32_t>(p.second.size());
            for (int32_t shard_id: p.second) {
                ar << shard_id;
                ar << static_cast<int32_t>(tables[shard_id].size());
                for(auto& item: tables[shard_id]) {
                    ar << item.first << item.second;
                }            
            }

            if (_compress_info._enabled) {
                BinaryArchive compressed_ar;
                _compress_info._compresser.raw_compress(ar, compressed_ar);
                reqs.back() << compressed_ar;
            }
        }
        return Status();
    }

protected:
    PARTITIONER _key_partitioner;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_SYNC_OPERATOR_OPERATOR_H
