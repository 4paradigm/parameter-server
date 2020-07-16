#ifndef PARADIGM4_PICO_PS_OPERATOR_PULLOPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_PULLOPERATOR_H

#include <memory>
#include <string>
#include <vector>

#include <boost/any.hpp>

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

class PullItems : public Object {};

//给Handler调整merge request num使用
struct PullRequestData : public Object {
    size_t item_num = 0; // item个数
    size_t item_size = 0; // 每个item大小，0表示每个item大小不等
};

/*!  \brief  client 端 apply_response 时回填所需信息，例如某个 value 对应的内存地址。 */
struct FillBackInfo : public Object {
    boost::any content;
};

/*!
 * \brief PullOperator 类定义从 Storage 拉取存储内容的操作
 *        config 中 read_only 定义是否在只读模式下，如果设置为 true ，
 *        在 apply_request 时 server 将不请求 shard 的读锁以增加性能，
 *        在 predict 模式下可以将此值设置为 true。
 */
class PullOperator : public Operator {
public:
    PullOperator(const Configure& config) : Operator(config) {
        if (config.has("read_only")) {
            _read_only = config["read_only"].as<bool>();
        }
    }

    virtual ~PullOperator() {}

    PullOperator(PullOperator&&) = default;
    PullOperator& operator=(PullOperator&&) = default;

    virtual void generate_request_data(core::vector<std::unique_ptr<PullItems>>&,
          RuntimeInfo&,
          std::unique_ptr<PullRequestData>&) = 0;

    virtual Status generate_request(std::vector<PullRequestData*>&,
          RuntimeInfo&,
          std::vector<PSRequest>&) = 0;

    virtual void apply_request(RuntimeInfo&, PSRequest&, Storage*, PSResponse&) = 0;

    virtual void apply_response(PSResponse&,
          std::vector<PullRequestData*>&) = 0;
    
    virtual void fill_pull_items(PullRequestData*,
          core::vector<std::unique_ptr<PullItems>>&) = 0;

    size_t max_request_merge_num() {
        return _max_request_merge_num;
    }

    bool read_only() {
        return _read_only;
    }
protected:
    size_t _max_request_merge_num = 1;
    bool _read_only = false;
};

/*! \brief 可将多组 PullItems 打包至一个 PullItemsPackage */
template <typename T>
class PullItemsPackage {
public:
    void add(const T& pull_items) {
        _content.emplace_back(new T(pull_items));
    }
    void add(T&& pull_items) {
        _content.emplace_back(new T(std::move(pull_items)));
    }
    core::vector<std::unique_ptr<PullItems>>& content() {
        return _content;
    }

    PullItemsPackage() = default;
    PullItemsPackage(const PullItemsPackage&) = delete;
    PullItemsPackage(PullItemsPackage&& rhs) : _content(std::move(rhs._content)) {}

private:
    core::vector<std::unique_ptr<PullItems>> _content;
};

/*! \brief  向 server 端请求的一组 key */
template <typename KEY, typename VALUE>
class SparsePullItems : public PullItems {
private:
    std::vector<KEY> _keys;

public:
    const KEY* keys;
    VALUE* pull_args;
    size_t n;

    SparsePullItems() = default;

    SparsePullItems(const KEY* keys, VALUE* pull_args, size_t n)
        : keys(keys), pull_args(pull_args), n(n) {}

    SparsePullItems(const std::vector<KEY>& keys, VALUE* pull_args)
        : _keys(keys), keys(keys.data()), pull_args(pull_args), n(_keys.size()) {}

    SparsePullItems(std::vector<KEY>&& keys, VALUE* pull_args)
        : _keys(std::move(keys)), keys(keys.data()), pull_args(pull_args), n(_keys.size()) {}

    SparsePullItems(const SparsePullItems& rhs)
        : _keys(rhs._keys), keys(_keys.size() == 0 ? rhs.keys : _keys.data()),
          pull_args(rhs.pull_args), n(rhs.n) {}

    SparsePullItems(SparsePullItems&& rhs)
        : _keys(std::move(rhs._keys)), keys(_keys.size() == 0 ? rhs.keys : _keys.data()),
          pull_args(rhs.pull_args), n(rhs.n) {}
};

template <typename KEY, typename PULL_ARG>
struct DensePullItems : public PullItems {
    KEY key;
    PULL_ARG* pull_arg;
    DensePullItems(KEY key, PULL_ARG* arg) : key(key), pull_arg(arg) {}
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
