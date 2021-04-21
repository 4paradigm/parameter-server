#ifndef PARADIGM4_PICO_PS_HANDLER_UDFHANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_UDFHANDLER_H


#include "pico-ps/handler/Handler.h"
#include "pico-ps/operator/UDFOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class UDFHandler : public Handler {
public:
    UDFHandler(int32_t storage_id,
          int32_t handler_id,
          std::shared_ptr<Operator>& op,
          Client* client): Handler(storage_id, handler_id, op, client) {}

    virtual ~UDFHandler() {}

    UDFHandler(UDFHandler&&) = default;
    UDFHandler& operator=(UDFHandler&&) = default;

    void call(void* param, int timeout = -1);

    void set_wait_result(void* result);

protected:
    virtual void retry(int timeout = -1) override;
    
    virtual Status apply_response(PSResponse& resp, PSMessageMeta& meta) override;

    virtual void release_dealer() override;
    
    void* _result = nullptr;
    std::shared_ptr<void> _param; // for retry
    std::shared_ptr<void> _state;
    std::vector<PSRequest> _reqs;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
