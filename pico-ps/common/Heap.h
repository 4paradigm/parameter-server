#ifndef PARADIGM4_PICO_PS_COMMON_HEAP_H
#define PARADIGM4_PICO_PS_COMMON_HEAP_H

#include <functional>
#include <vector>

namespace paradigm4 {
namespace pico {
namespace ps {

template <typename T,
      typename Container = std::vector<T>,
      typename Compare = std::less<typename Container::value_type>>
class Heap {
public:
    void clear() {
        _data.clear();
    }

    size_t size() const {
        return _data.size();
    }

    bool empty() const {
        return size() == 0;
    }

    T top() const {
        return *(_data.begin());
    }

    void push(const T& num) {
        _data.emplace_back(num);
        upshift();
    }

    void push(T&& num) {
        _data.emplace_back(std::move(num));
        upshift();
    }

    void pop() {
        std::swap(_data[0], _data[_data.size() - 1]);
        _data.pop_back();
        downshift();
    }

    Container& container() {
        return _data;
    }

private:
    void downshift() {
        size_t pos = 0;
        while (pos < _data.size()) {
            size_t tmp_pos = pos;
            if (pos * 2 < _data.size()) {
                if (_comp(_data[tmp_pos], _data[pos * 2])) {
                    tmp_pos = pos * 2;
                }
            }
            if (pos * 2 + 1 < _data.size()) {
                if (_comp(_data[tmp_pos], _data[pos * 2 + 1])) {
                    tmp_pos = pos * 2 + 1;
                }
            }
            if (tmp_pos == pos) {
                break;
            }
            std::swap(_data[pos], _data[tmp_pos]);
            pos = tmp_pos;
        }
    }

    void upshift() {
        size_t pos = _data.size() - 1;
        while (pos != 0) {
            if (_comp(_data[pos / 2], _data[pos])) {
                std::swap(_data[pos / 2], _data[pos]);
                pos = pos / 2;
            } else {
                break;
            }
        }
    }

    Container _data;
    Compare _comp;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_COMMON_MIN_HEAP_H
