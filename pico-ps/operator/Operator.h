#ifndef PARADIGM4_PICO_PS_OPERATOR_OPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_OPERATOR_H
#include <dlfcn.h>

#include <memory>
#include <string>
#include <unordered_map>

#include <boost/functional/hash.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "pico-ps/common/core.h"
#include "pico-ps/common/core.h"
namespace paradigm4 {
namespace pico {
/*! \brief namespace of parameter server */
namespace ps {

/*!
 * \brief Operator 类是定义作用于 Storage 类的操作类型，
 *        Operator 类为操作无状态(stateless)，但操作不保证幂等性(non-idempotent)。
 *        目前有如下7种子类（具体实现详见本目录下其他文件）
 *        1. PushOperator:
 *           修改 Storage 的操作
 *        2. PullOperator:
 *           拉取 Storage 内容的操作
 *        3. DumpOperator:
 *           将 Storage 内容 Dump 到文件系统的操作
 *        4. LoadOperator:
 *           从文件系统读取 Storage 内容的操作
 *        5. ForEachOpeartor:
 *           遍历 Storage 内容，同时统计某些信息或修改 Storage
 *           的操作
 *        6. EraseIfOpeartor:
 *           遍历 Storage 内容，清除满足条件的某些元素的操作
 *        7. StorageOperator:
 *           修改、获取 Storage 元信息的操作，比如清除所有 Storage 内容，
 *           获取 Storage 中保存元素个数等信息。
 *
 *        一般来说，Operator 会定义 generate_request(), apply_request(),
 *        apply_response() 这一组方法；
 *        generate_request() 由 client 端调用，产生发送至 server 的 PSRequest，
 *        比如在 pull 操作中 client 会将所需要拉取的 key 填写在 PSRequest 中；
 *        apply_request() 由 server 端使用，消耗 PSRequest 并产生 PSResponse，
 *        如何解析 PSRequest 并如何做出对应的操作各个 Operator 并不相同，各自
 *        定义在本函数中；
 *        apply_response() 由 client 端使用，消耗 PSResponse，比如在 Pull
 *        操作中将拉取得到的 value 回填至训练的 weight 中。
 *        PSRequest/PSResponse 在 pico-ps/common/message.h 中定义。
 */
class Operator : public Object {
public:
    /*!
     * \brief Operator 的初始化函数。
     *        将 Operator 的配置信息保存在_config 中，比如 Push 操作的 learning_rate。
     *        建议在注册好一个 Operator 之后不再修改 config，若必须修改建议是
     *        重新注册一个新的 Opeartor。
     * \param conf Opeartor 的配置信息。
     *        对于 StorageOperator 为 storage config， 详见 StorageOperator.h；
     *        对于其他的 Opeartor 为具体 Op 的 config，均包含一个子域为
     *        config["storage_config"] 在其中保存关于 storage 的配置，可参考 PushOpeartor
     *        的实现。
     */
    Operator(const Configure& conf) : _config(conf) {}

    virtual ~Operator();

    Operator(Operator&&) = default;
    Operator& operator=(Operator&&) = default;

    const Configure& op_config() {
        return _config;
    }

protected:
    Configure _config;
};

/*! \brief 用以生产 Operator 子类的工厂函数 */
class OperatorFactory : public VirtualObject {
public:
    typedef Operator* (*producer_t)(const Configure&);
    typedef std::pair<std::string, std::string> key_type;
    typedef std::unordered_map<key_type, producer_t, boost::hash<key_type>>
        producer_storage_type;

    static OperatorFactory& singleton() {
        static OperatorFactory ins;
        return ins;
    }

    virtual ~OperatorFactory();

    bool load_library(const std::string& path);

    std::shared_ptr<Operator> create(
        const std::string& lib_name, const std::string& op_name, const Configure& conf);

    void register_producer(
        const std::string& lib_name, const std::string& op_name, producer_t p);

    struct OperatorFactoryRegisterAgent {
        OperatorFactoryRegisterAgent(
            const std::string& lib_name, const std::string& op_name, producer_t p) {
            OperatorFactory::singleton().register_producer(lib_name, op_name, p);
            STDERR_LOG() << "register op:" << op_name;
        }
    };

private:
    producer_storage_type _producers;
    std::vector<void*> _opened_libs;
    std::unordered_set<std::string> _opened_lib_path;
    boost::shared_mutex _producer_mu;
    std::mutex _lib_mu;
};

#define REGISTER_OPERATOR(library_name, class_name)                               \
    extern "C" {                                                                  \
    Operator* library_name##class_name##_producer(const Configure& conf) {        \
        return new class_name(conf);                                              \
    }                                                                             \
    }                                                                             \
    namespace op_register {                                                       \
                                                                                  \
    static paradigm4::pico::ps::OperatorFactory::OperatorFactoryRegisterAgent     \
        library_name##class_name##_agent =                                        \
            paradigm4::pico::ps::OperatorFactory::OperatorFactoryRegisterAgent(   \
                #library_name, #class_name, library_name##class_name##_producer); \
    }

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
