#ifndef PARADIGM4_PICO_PS_HANDLER_HANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_HANDLER_H

#include <cstdint>

#include <memory>
#include <vector>

#include "pico-ps/common/core.h"

#include "pico-ps/common/common.h"
#include "pico-ps/common/message.h"
#include "pico-ps/common/DistributedAsyncReturn.h"
#include "pico-ps/operator/Operator.h"
#include "pico-ps/service/Client.h"

namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

/*!
 * \brief Handler 是 client 使用 Operator 的入口。
 *        Handler 的使用方式是异步的，需要使用 wait(pid) 来等待结果返回。
 *        每调用一次 Handler::send()，将会产生一个 pid，用以去区分不同的调用，
 *        Handler 的实现中尽可能复用 pid。
 *        代码示例
 *        //////
 *        Handler hdl;
 *        size p1 = hdl.send();  // p1 == 0
 *        size p2 = hdl.send();  // p2 == 1
 *        hdl.wait(p1);
 *        hdl.wait(p2);
 *        size p3 = hdl.send();  // p3 == 0
 *        size p4 = hdl.send();  // p4 == 1
 *        size p5 = hdl.send();  // p5 == 2
 *        hdl.wait_all();
 *        //////
 */
class Handler: public DistributedAsyncReturn {
public:
    Handler() = default;

    Handler(int32_t storage_id,
            int32_t handler_id,
            std::shared_ptr<Operator> op,
            Client* client);

    Handler(Handler&& rhs) = default;

    Handler& operator=(Handler&& rhs) = default;

    Handler(const Handler&) = delete;

    Handler& operator=(const Handler&) = delete;

    //如果不支持retry则override wait并直接调用wait_no_retry
    virtual Status wait()override;

    virtual Status wait_no_retry();

    int32_t storage_id() const {
        return _storage_id;
    }

    int32_t handler_id() const {
        return _handler_id;
    }

protected:
    virtual Status wait_no_release();

    virtual void retry(int timeout = - 1);

    virtual Status apply_response(PSResponse&, PSMessageMeta&)override;

    virtual void release_dealer();

    int32_t _storage_id, _handler_id;
    std::shared_ptr<Operator> _op;
    Client* _client = nullptr;

};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_HANDLER_H
