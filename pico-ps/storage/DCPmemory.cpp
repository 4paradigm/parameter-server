#ifdef USE_DCPMM
#include "pico-ps/storage/DCPmemory.h"
#include <unistd.h>
#include <iostream>
#include <fstream>

namespace paradigm4{
namespace pico{
namespace ps{

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

static inline bool file_exists(char const* file) {
    return access(file, F_OK) == 0;
}

static inline bool file_exists(const std::string& file) {
    return file_exists(file.c_str());
}

static inline std::string meta_path(const std::string& root_path) {
    return root_path + "/storage_meta";
}

static inline std::string storage_poolset_file_path(const std::string& root_path, int32_t storage_id) {
    return root_path + "/storage_pool_set." + std::to_string(storage_id);
}

static inline std::string storage_path(const std::string& root_path, int32_t storage_id) {
    return root_path + "/storage/" + std::to_string(storage_id);
}

static inline std::string storage_poolset_file_content(
    const std::string& root_path, int32_t storage_id, uint64_t maximum_storage_pool_size) {
    std::string content = "PMEMPOOLSET\nOPTION SINGLEHDR\n";
    content += std::to_string(maximum_storage_pool_size) + " " + storage_path(root_path, storage_id) + "\n";
    return content;
}

const std::string LAYOUT = "";
//#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

DCPmemory& DCPmemory::singleton(){
    static DCPmemory single;
    return single;
}

bool DCPmemory::initialize(std::string root_path, uint64_t meta_pool_size, uint64_t maximum_storage_pool_size) {
    /* force-disable SDS feature during pool creation
     * this is for handle err when unnormal shutdown after hardware failure.*/
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
    
    _root_path = root_path;
    _maximum_storage_pool_size = maximum_storage_pool_size;

    try {
        std::string meta_pool_path = meta_path(root_path);
        
        if (!file_exists(meta_pool_path.c_str())) {
            if (!file_exists(root_path)) {
                URIConfig root_path_uri(root_path);
                FileSystem::mkdir_p(root_path_uri);
            }
            _pm_meta_pool = meta_pool_t::create(meta_pool_path.c_str(), LAYOUT, meta_pool_size, S_IRWXU);
        } else {
            _pm_meta_pool = meta_pool_t::open(meta_pool_path.c_str(), LAYOUT);
            _pm_meta_pool.root()->storage_set->runtime_initialize();
        }

        auto r = _pm_meta_pool.root();

        if (r->storage_set == nullptr) {
            pmem::obj::transaction::run(_pm_meta_pool, [&] {
                r->storage_set = pmem::obj::make_persistent<dcpmm_storage_id_set_type>();
                r->node_id.get_rw() = -1;
            });
        }

        std::unordered_set<int32_t> remove_exclude_storage;
        for (const auto& storage_it : *(r->storage_set)) { 
            if (file_exists(storage_path(root_path, storage_it.first))) {
                storage_pool_t tmp;
                _open_storage_pool(storage_it.first, tmp);
                remove_exclude_storage.emplace(storage_it.first);
            }
        }
        remove_storage_exclude(remove_exclude_storage);

        is_initialized = true;
    } catch (pmem::pool_error &e) {
		std::cerr << e.what() << std::endl;
		std::cerr
			<< "Err when create root meta pool"
			<< std::endl;
        return false;
	} catch (std::exception &e) {
		std::cerr << e.what() << std::endl;
        return false;
	}
    return true;
}

bool DCPmemory::get_storage_pool_or_create(int32_t storage_id, storage_pool_t& out) {
    if (_storage_pool.count(storage_id) != 0) {
        out = _storage_pool[storage_id];
        return true;
    } else {
        // 没打开该 pool 或者需要创建.
        dcpmm_storage_id_set_type::accessor acc;
        if (_pm_meta_pool.root()->storage_set->find(acc, storage_id)) {
            return _open_storage_pool(storage_id, out);
        } else {
            return _create_storage_pool(storage_id, out);
        }
    }
}

bool DCPmemory::_create_storage_pool(int32_t storage_id, storage_pool_t& out) {
    remove_storage(storage_id);

    URIConfig storage_uri(storage_path(_root_path, storage_id));
    FileSystem::mkdir_p(storage_uri);

    std::ofstream poolset_file;
    poolset_file.open(storage_poolset_file_path(_root_path, storage_id));
    poolset_file << storage_poolset_file_content(_root_path, storage_id, _maximum_storage_pool_size);
    poolset_file.close();

    try {
        out = storage_pool_t::create(storage_poolset_file_path(_root_path, storage_id).c_str(), LAYOUT, 0, S_IRWXU);
        _storage_pool.emplace(storage_id, out);
        auto r = out.root();
        pmem::obj::transaction::run(out, [&] {
            r->storage_shards = pmem::obj::make_persistent<dcpmm_storage_shards_t>();
            r->storage_version_uuid = pmem::obj::make_persistent<pmem::obj::string>();
            r->storage_version_uuid->assign("-");
        });
    } catch (pmem::pool_error &e) {
		std::cerr << e.what() << std::endl;
        std::cerr << "Err when create storage pool" << std::endl;
        return false;
	} catch (std::exception &e) {
		std::cerr << e.what() << std::endl;
        return false;
	}

    _pm_meta_pool.root()->storage_set->insert({storage_id, true});
    return true;
}

bool DCPmemory::_open_storage_pool(int32_t storage_id, storage_pool_t& out) {
    try {
        out = storage_pool_t::open(storage_poolset_file_path(_root_path, storage_id).c_str(), LAYOUT);
        _storage_pool.emplace(storage_id, out);
        _storage_pool[storage_id].root()->storage_shards->runtime_initialize();
    } catch (pmem::pool_error &e) {
		std::cerr << e.what() << std::endl;
        std::cerr << "Err when create storage pool" << std::endl;
        return false;
    } catch (std::exception &e) {
		std::cerr << e.what() << std::endl;
        std::cerr << "Err when create storage pool" << std::endl;
        return false;
    }
    return true;
}

void DCPmemory::remove_storage_exclude(const std::unordered_set<int32_t>& exclude_storage) {
    for (auto storage_it : *(_pm_meta_pool.root()->storage_set)) {
        if (exclude_storage.count(storage_it.first) == 0) {
            remove_storage(storage_it.first);
        }
    }
}

void DCPmemory::remove_storage(int32_t storage_id) {
    if (_storage_pool.count(storage_id) != 0) {
        _storage_pool[storage_id].close();
        _storage_pool.erase(storage_id);
    }
    _remove_storage_file(storage_id);
    _pm_meta_pool.root()->storage_set->erase(storage_id);
}

void DCPmemory::_remove_storage_file(int32_t storage_id) {
    URIConfig storage_uri(storage_path(_root_path, storage_id));
    if (FileSystem::exists(storage_uri)) {
        FileSystem::rmr(storage_uri);
    }

    URIConfig poolset_file_url(storage_poolset_file_path(_root_path, storage_id));
    if (FileSystem::exists(poolset_file_url)) {
        FileSystem::rmr(poolset_file_url);
    }
}

void DCPmemory::finalize() {
    LOG(INFO)<<"Stopping DCPmemory";      
    try {
        _pm_meta_pool.close();
        for (auto& storage_it : _storage_pool) {
            storage_it.second.close();
        }
        _storage_pool.clear();
    } catch (std::exception& e) {
        SLOG(WARNING) << "Exception caught during close pools: " << e.what();
    }
    LOG(INFO)<<"Stopped DCPmemory Successfully";
}
/*
bool DCPmemory::free_all_shards(){
    auto &map = _pmpool.root()->kv;
    map.defragment();
    map.clear();
    pmem::obj::transaction::run(_pmpool, [&] {
			pmem::obj::delete_persistent<kv_type>(_pmpool.root()->kv);
		});
}*/



} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif  // USE_DCPMM
