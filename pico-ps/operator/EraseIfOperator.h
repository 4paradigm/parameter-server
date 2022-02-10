#ifndef PARADIGM4_PICO_PS_OPERATOR_ERASEIFOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_ERASEIFOPERATOR_H

#include "pico-ps/operator/ForEachOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

/*!
 * \brief   继承于 ShardStorageForEachOperator 的 ShardTableEraseIfOperator，
 *          用于将 Storage 中符合某种条件的 key-value pair 删除。
 *
 *          application developer 需要实现函数
 *          bool erase_if(const key_type& key, const value_type& value);
 */
template <typename STORAGE>
class SparseTableEraseIfOperator : public ShardStorageForEachOperator {
public:
    typedef STORAGE storage_type;
    typedef typename storage_type::key_type key_type;
    typedef typename storage_type::value_type value_type;
    typedef typename storage_type::shard_type shard_type;

    SparseTableEraseIfOperator(const Configure& conf) : ShardStorageForEachOperator(conf) {}

    void apply_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp_ret) override {
        int32_t shard_id;
        req >> shard_id;

        SCHECK(req.archive().is_exhausted());
        SCHECK(rt.local_shards().count(shard_id))
              << "Bad Request: invalid shard_id = " << shard_id;

        auto shard_storage = static_cast<storage_type*>(storage);
        shard_storage->write_shard(shard_id, [this](boost::any& shard) {
            shard_type& data = *boost::any_cast<shard_type>(&shard);
            for (auto it = data.begin(); it != data.end();) {
                if (erase_if(it->first, it->second)) {
                    it = data.erase(it);
                } else {
                    ++it;
                }
            }
        });

        PSResponse resp(req);
        BinaryArchive empty_ar;
        auto ok = Status();
        resp << ok << empty_ar;
        resp_ret = std::move(resp);
    }

    virtual bool erase_if(const key_type& key, const value_type& value) = 0;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_OPERATOR_ERASEIFOPERATOR_H
