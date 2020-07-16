#ifndef PARADIGM4_PICO_PS_TEST_TESTOPSDS_H
#define PARADIGM4_PICO_PS_TEST_TESTOPSDS_H

#include "pico-ps/common/core.h"

namespace paradigm4 {
namespace pico {
namespace ps {


template<class T>
class TestDsReader {
public:
    TestDsReader() {}

    bool open(const core::URIConfig& uri) {
        _file = core::ShellUtility::open_read(uri.name(), "", "");
        if (!_file) {
            return false;
        }
        _ar.reset(_file);
        return true;
    }

    bool read(T& val) {
        return pico_deserialize(_ar, val);
    }

    bool eof() {
        return feof(&*_file);
    }

    void close() {
        _ar.release();
        _file.reset();
    }

private:
    core::shared_ptr<FILE> _file;
    core::TextFileArchive _ar;
};


template<class T>
class TestDsWriter {
public:
    TestDsWriter() {}

    bool open(const core::URIConfig& uri) {
        _file = core::ShellUtility::open_write(uri.name(), "", "");
        if (!_file) {
            return false;
        }
        _ar.reset(_file);
        return true;
    }

    bool write(const T& val) {
        return pico_serialize(_ar, val);
    }

    bool write(T&& val) {
        return pico_serialize(_ar, std::move(val));
    }

    void close() {
        _ar.release();
        _file.reset();
    }

private:
    core::shared_ptr<FILE> _file;
    core::TextFileArchive _ar;
};


}
}
}

#endif