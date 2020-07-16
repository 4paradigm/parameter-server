#ifdef USE_DCPMM
#include "pico-ps/storage/DCPmemory.h"
#include <unistd.h>

namespace paradigm4{
namespace pico{
namespace ps{

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

static inline int
file_exists(char const *file)
{
	return access(file, F_OK);
}

const std::string LAYOUT = "";
//#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

DCPmemory& DCPmemory::singleton(){
    static DCPmemory single;
    return single;
}

bool DCPmemory::initialize(std::string path, std::uint64_t pool_size){
    /* force-disable SDS feature during pool creation
     * this is for handle err when unnormal shutdown after hardware failure.*/
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
    
    try {
        if (file_exists(path.c_str()) != 0)
            _pmpool = pool_t::create(path.c_str(), LAYOUT, pool_size, S_IRWXU);
        else
            _pmpool = pool_t::open(path.c_str(), LAYOUT);
    
        auto r = _pmpool.root();

        if (r->kv == nullptr) {
            pmem::obj::transaction::run(_pmpool, [&] {
                r->kv = pmem::obj::make_persistent<kv_type>();
                r->node_id.get_rw() = -1;
            });
        }
        is_initialized = true;
    } catch (pmem::pool_error &e) {
		std::cerr << e.what() << std::endl;
		std::cerr
			<< "Err when create root simplekv"
			<< std::endl;
        return false;
	} catch (std::exception &e) {
		std::cerr << e.what() << std::endl;
        return false;
	}
    return true;
}
void DCPmemory::finalize(){
    LOG(INFO)<<"Stopping DCPmemory";      
    //free_all_shard();

    // add by cc wwww 
    // lack of code for release all the data in DCPMM, need to check whether we need such kind of function, since we can directly delete the data file in DCPMM to quickly remove all the data.
    _pmpool.close();
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

#endif