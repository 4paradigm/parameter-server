#ifndef PARADIGM4_PICO_PS_OPERATOR_PUSHOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_PUSHOPERATOR_H

#include <algorithm>
#include <utility>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/Partitioner.h"

#include "pico-ps/common/common.h"
#include "pico-ps/common/message.h"
#include "pico-ps/operator/Operator.h"
#include "pico-ps/storage/KVShardStorage.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

class PushItems : public Object {};

//给Handler调整merge request num使用
struct PushRequestData: public Object {
    size_t item_num = 0;
    size_t item_size = 0;
};

/*!
 * \brief PushOperator 类定义将修改推送至 Storage 的操作，
 *        包括 async_push 和 sync_push 两种，
 *        async_push 直接将修改作用于 Storage 中， 主要在异步训练模式下使用；
 *        sync_push 将修改暂存于 Delta Storage 中，再由
 *        store 操作将 Delta Storage 合并回 Storage 当中，主要在
 *        同步训练模式下使用。
 */
class PushOperator : public Operator {
public:
    struct SyncTable {
        std::string model_name;
        std::string table_name;
        std::string op_key;
    };
public:
    PushOperator(const Configure& config) : Operator(config) {
        if (config.has("sync_table") && config["sync_table"].size() > 0) {
            auto sync_table = config["sync_table"];
            _sync_tables.resize(sync_table.size());
            for (size_t i = 0; i < sync_table.size(); ++i) {
                _sync_tables[i].model_name = sync_table[i]["model_name"].as<std::string>();
                _sync_tables[i].table_name = sync_table[i]["table_name"].as<std::string>();
                _sync_tables[i].op_key = sync_table[i]["op_key"].as<std::string>();
            }
        }
    }

    virtual ~PushOperator() {}

    PushOperator(PushOperator&&) = default;
    PushOperator& operator=(PushOperator&&) = default;

    std::vector<SyncTable>& sync_tables() {
        return _sync_tables;
    }

    /*!
     * \brief 产生 push 操作的 PSRequest，第二个参数中保存了目前集群的运行
     *        信息，如各个 parameter server 节点中 shard 的数目，这些信息将在产生
     *        PSrequest 时被使用。
     *        client 端调用。
     */
    virtual void generate_request_data(core::vector<std::unique_ptr<PushItems>>&,
          RuntimeInfo&,
          std::unique_ptr<PushRequestData>&) = 0;

    virtual void generate_push_request(std::vector<PushRequestData*>&,
          RuntimeInfo& runtime_info,
          std::vector<PSRequest>&)
          = 0;

    /*! \brief client 端调用*/
    virtual void generate_store_request(RuntimeInfo& runtime_info, std::vector<PSRequest>&) = 0;

    /*! \brief server 端调用*/
    virtual void
          apply_async_push_request(RuntimeInfo& runtime_info, 
          PSRequest&, 
          Storage* /*storage*/, 
          Storage* /*incr_storage*/,
          PSResponse&)
          = 0;

    /*! \brief server 端调用*/
    virtual void
          apply_sync_push_request(RuntimeInfo& runtime_info, 
          PSRequest&, 
          Storage*, 
          PSResponse&)
          = 0;

    /*! \brief server 端调用*/
    virtual void
          apply_store_request(RuntimeInfo& runtime_info,
          PSRequest&,
          Storage* /*delta_storage_type*/,
          Storage* /*storage_type*/,
          Storage* /*incr_storage*/,
          std::function<void(PSResponse&&)>)
          = 0;


    /*! \brief client 端调用*/
    virtual void apply_response(PSResponse&) = 0;


    /*!
     *  \brief 生成 Delta Storage 的方法。
     *         server 端调用
     */
    virtual std::unique_ptr<Storage> create_delta_storage(RuntimeInfo& runtime_info) = 0;

    /*!
     * \brief 生成 增量更新Storage的方法
     */
    virtual std::unique_ptr<Storage> create_incr_storage(RuntimeInfo& runtime_info) = 0;

    size_t max_request_merge_num() {
        return _max_request_merge_num;
    }
protected:
    size_t _max_request_merge_num = 1;
    //std::vector<std::pair<int32_t, std::string>> _sync_storage;
    std::vector<SyncTable> _sync_tables;
};

/*! \brief 可将多组 PushItems 打包至一个 PushItemsPackage */
template <typename T>
class PushItemsPackage {
public:
    void add(const T& push_items) {
        _content.emplace_back(new T(push_items));
    }
    void add(T&& push_items) {
        _content.emplace_back(new T(std::move(push_items)));
    }
    core::vector<std::unique_ptr<PushItems>>& content() {
        return _content;
    }

    PushItemsPackage() = default;
    PushItemsPackage(const PushItemsPackage&) = delete;
    PushItemsPackage(PushItemsPackage&& rhs) : _content(std::move(rhs._content)) {}

private:
    core::vector<std::unique_ptr<PushItems>> _content;
};

/*! \brief  向 server 端推送的一组 key-value */
template <typename KEY, typename PUSH_ARG>
class SparsePushItems : public PushItems {
private:
    std::vector<KEY> _keys;
    std::vector<PUSH_ARG> _push_args;

public:
    const KEY* keys;
    const PUSH_ARG* push_args;
    size_t n;

    SparsePushItems() = default;

    SparsePushItems(const KEY* keys, const PUSH_ARG* push_args, size_t n)
        : keys(keys), push_args(push_args), n(n) {}

    SparsePushItems(const std::vector<KEY>& keys, const std::vector<PUSH_ARG>& push_args)
        : _keys(keys), _push_args(push_args), keys(_keys.data()), push_args(_push_args.data()),
          n(_keys.size()) {}

    SparsePushItems(std::vector<KEY>&& keys, std::vector<PUSH_ARG>&& push_args)
        : _keys(std::move(keys)), _push_args(std::move(push_args)), keys(_keys.data()),
          push_args(_push_args.data()), n(_keys.size()) {}

    SparsePushItems(const SparsePushItems& rhs)
        : _keys(rhs._keys), _push_args(rhs._push_args),
          keys(_keys.size() == 0 ? rhs.keys : _keys.data()),
          push_args(_push_args.size() == 0 ? rhs.push_args : _push_args.data()), n(rhs.n) {}

    SparsePushItems(SparsePushItems&& rhs)
        : _keys(std::move(rhs._keys)), _push_args(std::move(rhs._push_args)),
          keys(_keys.size() == 0 ? rhs.keys : _keys.data()),
          push_args(_push_args.size() == 0 ? rhs.push_args : _push_args.data()), n(rhs.n) {}
};

template <typename KEY, typename PUSH_ARG>
class DensePushItems : public PushItems {
public:
    KEY key;
    PUSH_ARG* push_value;

    DensePushItems(KEY key, PUSH_ARG* val) : key(key), push_value(val) {}
};

template <typename KEY, typename VALUE>
class SyncPushItems : public PushItems {
public:
    SyncPushItems(int capacity) {
        _kvs.reserve(capacity);
    }

    std::vector<std::pair<KEY, VALUE>>& kvs() {
        return _kvs;
    }

private:
    std::vector<std::pair<KEY, VALUE>> _kvs;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
