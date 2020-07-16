#ifndef PARADIGM4_PICO_PS_OPERATOR_FOREACHOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_FOREACHOPERATOR_H

#include <memory>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/core.h"

#include "pico-ps/common/common.h"
#include "pico-ps/common/message.h"
#include "pico-ps/operator/Operator.h"
#include "pico-ps/storage/KVShardStorage.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

/*!
 * \brief   可序列化对象，ForEach 的参数与结果类型均派生自本类型。
 *          使用定义在 common/include/archive.h 中的 PICO_PS_SERIALIZATION 宏进行成员的序列化。
 *          //////
 *          Example:
 *             struct StatisticResult : ForEachResult {
 *                 StatisticResult(int64_t fea_num, int64_t effective_fea_num)
 *                     : fea_num(fea_num), effective_fea_num(effective_fea_num) {}
 *
 *                 int64_t fea_num = 0;
 *                 int64_t effective_fea_num = 0;
 *
 *                 PICO_PS_SERIALIZATION(StatisticResult, fea_num, effective_fea_num);
 *             };
 */

/*! \brief ForEachOperator 产生的结果，合并需要满足结合律 */
using ForEachResult = SerializableObject;

/*! \brief ForEachOperator client 发送给 server 的参数。 */
using CarriedItem = SerializableObject;

/*!
 * \brief ForEachOperator 类定义遍历 Storage 类型，进行内容修改或者统计的操作。
 */
class ForEachOperator : public Operator {
public:
    ForEachOperator(const Configure& conf) : Operator(conf) {}

    virtual ~ForEachOperator() {}

    ForEachOperator(ForEachOperator&&) = default;
    ForEachOperator& operator=(ForEachOperator&&) = default;

    virtual void generate_request(const CarriedItem&, RuntimeInfo&, std::vector<PSRequest>&)
          = 0;

    virtual void apply_request(RuntimeInfo&, PSRequest&, Storage*, PSResponse&) = 0;

    virtual std::unique_ptr<ForEachResult> apply_response(PSResponse&, CarriedItem&) = 0;

    virtual std::unique_ptr<ForEachResult> init_result_impl() {
        return nullptr;
    }

    virtual void merge_result_impl(const ForEachResult&, ForEachResult&, const CarriedItem&) {}
};

/*!
 * \brief ShardedStorage 的 Foreach 操作，是 ForEachReadOperator（只读）与
 *        ForEachWriteOperator（读写）操作的基类。
 */
class ShardStorageForEachOperator : public ForEachOperator {
public:
    ShardStorageForEachOperator(const Configure& conf) : ForEachOperator(conf) {}

    virtual ~ShardStorageForEachOperator() {}

    ShardStorageForEachOperator(ShardStorageForEachOperator&&) = default;
    ShardStorageForEachOperator& operator=(ShardStorageForEachOperator&&) = default;

    virtual void generate_request(const CarriedItem& carried,
          RuntimeInfo& rt,
          std::vector<PSRequest>& reqs) override  {
        reqs.reserve(rt.global_shard_num());
        for (auto& p: rt.nodes()) {
            for (int32_t shard_id: p.second) {
                reqs.emplace_back(p.first);
                reqs.back() << shard_id;
                carried._binary_archive_serialize_internal_(reqs.back().archive());
            }
        }
    }

    virtual std::unique_ptr<ForEachResult> apply_response(PSResponse& resp,
          CarriedItem&) override  {
        Status st;
        resp >> st;
        SCHECK(st.ok()) << st.ToString();
        auto res = init_result_impl();
        if (res.get() != nullptr) {
            res->_binary_archive_deserialize_internal_(resp.archive());
        }
        return res;
    }
};

/*!
 *  \brief 针对 SparseTable 的只读 ForEach 操作 ForEachReadOperator。
 *
 *         STORAGE     server 端存储 pair<KEY,VALUE> 的数据结构。
 *         RES         对于每一个 key-value pair
 *                     操作后产生的结果类型，最终结果由每一个结果合并产生 (非必需）。
 *         CARRIEDITEM 由 client 端上传至 server 处所需的参数（非必需）。
 *
 *         RES 与 CARRIEDITEM 均为保持 ForEach 操作的灵活性所设计，均可以忽略。
 *
 *         application developer 根据自己的需求实现如下几个函数。
 *         一般情况下，需要实现三个函数，即需要实现 init_result()，
 *         for_each() 两种函数签名二选一，
 *         merge_result() 两种签名二选一。
 *
 *         * std::unique_ptr<RES> init_result() {
 *             return nullptr;
 *         }
 *         初始化默认 ForEachResult 类型的函数。
 *
 *         * std::unique_ptr<RES> for_each(const key_type&, const value_type&) {
 *             return nullptr;
 *         }
 *         对于每一个 key-value pair 产生一个 ForEachResult 的函数，key-value 均为只读。
 *
 *         * std::unique_ptr<RES> for_each(const key_type& key,
 *               const value_type& value,
 *               const CARRIEDITEM&) {
 *             return for_each(key, value);
 *         }
 *         根据 CARRIEDITEM 内容，对于每一个 key-value pair 产生一个 ForEachResult 的函数，
 *         默认实现调用与 CARRIEDITEM 内容无关的 for_each() 函数。
 *
 *         * void merge_result(const RES& lhs, RES& rhs) {}
 *         将 ForEachResult lhs 合并至 ForEachResult rhs 的函数。
 *
 *         * void merge_result(const RES& in, RES& out, const CARRIEDITEM&) {
 *             merge_result(in, out);
 *         }
 *         根据 CARRIEDITEM 内容，将 ForEachResult lhs 合并至 ForEachResult rhs 的函数，
 *         默认实现调用与 CARRIEDITEM 内容无关的 merge_result() 函数。
 *
 */
template <typename STORAGE, typename RES = ForEachResult, typename CARRIEDITEM = CarriedItem>
class SparseTableForEachReadOperator : public ShardStorageForEachOperator {
public:
    typedef STORAGE storage_type;
    typedef typename storage_type::key_type key_type;
    typedef typename storage_type::value_type value_type;
    typedef typename storage_type::shard_type shard_type;

    SparseTableForEachReadOperator(const Configure& conf)
        : ShardStorageForEachOperator(conf) {}

    virtual ~SparseTableForEachReadOperator() {}

    SparseTableForEachReadOperator(SparseTableForEachReadOperator&&) = default;
    SparseTableForEachReadOperator& operator=(SparseTableForEachReadOperator&&) = default;

    void apply_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp_ret) override {
        int32_t shard_id;
        req >> shard_id;
        CARRIEDITEM carried;
        carried._binary_archive_deserialize_internal_(req.archive());

        SCHECK(req.archive().is_exhausted());
        SCHECK(rt.local_shards().count(shard_id))
              << "Bad Request: invalid shard_id = " << shard_id;

        auto op_res = init_result_impl();

        auto shard_storage = static_cast<storage_type*>(storage);
        shard_storage->read_shard(
              shard_id, [&op_res, &carried, this](const boost::any& shard) {
                  const shard_type& data = *boost::any_cast<const shard_type>(&shard);
                  for (auto it = data.begin(); it != data.end(); ++it) {
                      auto res = for_each(it->first, it->second, carried);
                      if (op_res.get() != nullptr) {
                          merge_result_impl(*res, *op_res, carried);
                      }
                  }
              });

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
          const CarriedItem& carried) override {
        merge_result(static_cast<const RES&>(in),
              static_cast<RES&>(out),
              static_cast<const CARRIEDITEM&>(carried));
    }

    virtual std::unique_ptr<RES> init_result() {
        return nullptr;
    }

    virtual std::unique_ptr<RES> for_each(const key_type&, const value_type&) {
        return nullptr;
    }

    virtual std::unique_ptr<RES> for_each(const key_type& key,
          const value_type& value,
          const CARRIEDITEM&) {
        return for_each(key, value);
    }

    virtual void merge_result(const RES&, RES&) {}

    virtual void merge_result(const RES& in, RES& out, const CARRIEDITEM&) {
        merge_result(in, out);
    }
};

/*!
 *  \brief 针对 SparseTable 的可写ForEach 操作 ForEachWriteOperator。
 *
 *         STORAGE     server 端存储 pair<KEY,VALUE> 的数据结构。
 *         RES         对于每一个 key-value pair
 *                     操作后产生的结果类型，最终结果由每一个结果合并产生 (非必需）。
 *         CARRIEDITEM 由 client 端上传至 server 处所需的参数（非必需）。
 *
 *         RES 与 CARRIEDITEM 均为保持 ForEach 操作的灵活性所设计，均可以忽略。
 *
 *         application developer 根据自己的需求实现如下几个函数。
 *         一般情况下，需要实现两个或三个函数，即需要实现 init_result()，
 *         for_each() 两种函数签名二选一，
 *         merge_result() 两种签名二选一，若没有返回结果，此组函数可以不实现。
 *
 *         * std::unique_ptr<RES> init_result() {
 *             return nullptr;
 *         }
 *         初始化默认 ForEachResult 类型的函数。
 *
 *         * std::unique_ptr<RES> for_each(const key_type&, value_type&) {
 *             return nullptr;
 *         }
 *         对于每一个 key-value pair 产生一个 ForEachResult 的函数，value 可写。
 *
 *         * std::unique_ptr<RES> for_each(const key_type& key,
 *               const value_type& value,
 *               CARRIEDITEM&) {
 *             return for_each(key, value);
 *         }
 *         根据 CARRIEDITEM 内容，对于每一个 key-value pair 产生一个 ForEachResult 的函数，
 *         默认实现调用与 CARRIEDITEM 内容无关的 for_each() 函数。
 *
 *         * void merge_result(const RES& lhs, RES& rhs) {}
 *         将 ForEachResult lhs 合并至 ForEachResult rhs 的函数。
 *
 *         * void merge_result(const RES& in, RES& out, const CARRIEDITEM&) {
 *             merge_result(in, out);
 *         }
 *         根据 CARRIEDITEM 内容，将 ForEachResult lhs 合并至 ForEachResult rhs 的函数，
 *         默认实现调用与 CARRIEDITEM 内容无关的 merge_result() 函数。
 */
template <typename STORAGE, typename RES = ForEachResult, typename CARRIEDITEM = CarriedItem>
class SparseTableForEachWriteOperator : public ShardStorageForEachOperator {
public:
    typedef STORAGE storage_type;
    typedef typename storage_type::key_type key_type;
    typedef typename storage_type::value_type value_type;
    typedef typename storage_type::shard_type shard_type;

    SparseTableForEachWriteOperator(const Configure& conf)
        : ShardStorageForEachOperator(conf) {}

    virtual ~SparseTableForEachWriteOperator() {}

    SparseTableForEachWriteOperator(SparseTableForEachWriteOperator&&) = default;
    SparseTableForEachWriteOperator& operator=(SparseTableForEachWriteOperator&&) = default;

    void apply_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp_ret) override {
        int32_t shard_id;
        req >> shard_id;
        CARRIEDITEM carried;
        carried._binary_archive_deserialize_internal_(req.archive());

        SCHECK(req.archive().is_exhausted());
        SCHECK(rt.local_shards().count(shard_id))
              << "Bad Request: invalid shard_id = " << shard_id;

        auto op_res = init_result_impl();

        auto shard_storage = static_cast<storage_type*>(storage);
        shard_storage->write_shard(shard_id, [&op_res, &carried, this](boost::any& shard) {
            auto& data = *boost::any_cast<shard_type>(&shard);
            for (auto it = data.begin(); it != data.end(); ++it) {
                auto res = for_each(it->first, it->second, carried);
                if (op_res.get() != nullptr) {
                    merge_result_impl(*res, *op_res, carried);
                }
            }
        });

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
          const CarriedItem& carried) override {
        merge_result(static_cast<const RES&>(in),
              static_cast<RES&>(out),
              static_cast<const CARRIEDITEM&>(carried));
    }

    virtual std::unique_ptr<RES> init_result() {
        return nullptr;
    }

    virtual std::unique_ptr<RES> for_each(const key_type&, value_type&) {
        return nullptr;
    }

    virtual std::unique_ptr<RES> for_each(const key_type& key,
          value_type& value,
          const CARRIEDITEM&) {
        return for_each(key, value);
    }

    virtual void merge_result(const RES&, RES&) {}

    virtual void merge_result(const RES& in, RES& out, const CARRIEDITEM&) {
        merge_result(in, out);
    }
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_OPERATOR_FOREACHOPERATOR_H
