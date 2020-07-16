#ifndef PARADIGM4_PICO_PS_SERVICE_INCREMENT_STORAGE_H
#define PARADIGM4_PICO_PS_SERVICE_INCREMENT_STORAGE_H

#include "pico-ps/storage/Storage.h"
#include "pico-ps/operator/Operator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class IncrementStorage {
public:
    IncrementStorage(std::unique_ptr<Storage>&& s1,
            std::unique_ptr<Storage>&& s2) {
        _storages[0] = std::move(s1);
        _storages[1] = std::move(s2);
    }

    Storage* start_write() {
        _mtx.lock_shared();
        return _storages[_write_idx].get();
    }

    void finish_write() {
        _updated = true;
        _mtx.unlock_shared();
    }

    Storage* read_storage() {
        std::unique_lock<RWSpinLock> lock(_mtx);
        if (!_updated)
            return nullptr;
        _updated = false;
        _write_idx = !_write_idx;
        return _storages[!_write_idx].get();
    }

private:
    std::unique_ptr<Storage> _storages[2];
    int _write_idx = 0;
    RWSpinLock _mtx;
    bool _updated = false;
};

}
}
}

#endif // PARADIGM4_PICO_PS_SERVICE_INCREMENT_STORAGE_H

