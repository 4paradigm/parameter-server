#ifndef PARADIGM4_PICO_PS_OPERATOR_RPC_OPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_RPC_OPERATOR_H

#include "pico-ps/operator/Operator.h"
#include "pico-ps/common/common.h"
#include "pico-ps/common/message.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

/*!
 * 可以覆盖大部分操作，而不需要修改server代码。
 */
class RpcOperatorBase : public Operator {
public:
    RpcOperatorBase(const Configure& config) : Operator(config) {}
    RpcOperatorBase(RpcOperatorBase&&) = default;
    RpcOperatorBase& operator=(RpcOperatorBase&&) = default;

    virtual ~RpcOperatorBase() {}

    virtual bool read_only() = 0;

    virtual std::shared_ptr<void> create_state() = 0;

    virtual std::shared_ptr<void> create_param() = 0;

    virtual void copy_param(const void* param, void* copy) = 0;
  
    virtual Status generate_request(void* param, RuntimeInfo& rt, void* state, std::vector<PSRequest>& reqs) = 0;

    virtual void apply_request(const PSMessageMeta& meta, PSRequest&, const TableDescriptor&, Dealer* dealer) = 0;

    virtual Status apply_response(PSResponse&, void* state, void* result) = 0;

    const Configure& op_config() {
        return _config;
    }

protected:
    Configure _config;
};

template<class Param, class State> // result不用create
class RpcOperator: public RpcOperatorBase  {
public:
    RpcOperator(const Configure& config) : RpcOperatorBase(config) {}
    RpcOperator(RpcOperator&&) = default;
    RpcOperator& operator=(RpcOperator&&) = default;

    virtual ~RpcOperator() {}

    std::shared_ptr<void> create_param()override {
        return std::make_shared<Param>();
    }

    std::shared_ptr<void> create_state()override {
        return std::make_shared<State>();
    }

    void copy_param(const void* param, void* copy)override {
        *static_cast<Param*>(copy) = *static_cast<const Param*>(param);
    }

    Status generate_request(void* param, RuntimeInfo& rt, void* state, std::vector<PSRequest>& reqs) {
        return generate_request(*static_cast<Param*>(param), rt, *static_cast<State*>(state), reqs);
    }

    Status apply_response(PSResponse& resp, void* state, void* result) {
        return apply_response(resp, *static_cast<State*>(state), result);
    }

    virtual Status generate_request(Param& param, RuntimeInfo& rt, State& state, std::vector<PSRequest>& reqs) = 0;

    virtual Status apply_response(PSResponse&, State& state, void* result) = 0;

    const Configure& op_config() {
        return _config;
    }

};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
