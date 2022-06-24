#include "rocksdb/db.h"
#include <iostream>
#include <cassert>
#include <fstream>
typedef uint32_t VID_TYPE;
#define REPORT_STATUS_ERROR_DEL(return_code)         \
    if (status.ok() == 0)                            \
    {                                                \
        std::cout << status.ToString() << std::endl; \
        delete db;                                   \
        return return_code;                          \
    }

rocksdb::Status add_nbr(rocksdb::DB *db, VID_TYPE s, VID_TYPE nbr)
{
    std::string snbrs;
    rocksdb::Slice skey = rocksdb::Slice((const char *)&s, sizeof(VID_TYPE));
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), skey, &snbrs);
    // std::cout << status.ToString() << std::endl;
    if (status.ok())
    {
        size_t sz = snbrs.size();
        snbrs.resize(sz + sizeof(VID_TYPE));

        const char *str = snbrs.c_str();
        *((VID_TYPE *)(str + sz)) = nbr;
        status = db->Put(rocksdb::WriteOptions(), skey, rocksdb::Slice(str, snbrs.size()));
    }
    else if (status.IsNotFound())
    {
        status = db->Put(rocksdb::WriteOptions(), skey, rocksdb::Slice((const char *)&nbr, sizeof(VID_TYPE)));
    }
    return status;
}

rocksdb::Status add_edge(rocksdb::DB *db, VID_TYPE s, VID_TYPE t)
{
    rocksdb::Status status = add_nbr(db, s, t);
    if (status.ok())
    {
        status = add_nbr(db, t, s);
    }
    return status;
}

VID_TYPE slice2key(rocksdb::Slice &slice)
{
    const VID_TYPE *d = (const VID_TYPE *)slice.data();
    // std::cout << "\t" << d[0] << std::endl;
    return d[0];
}

void slice2nbrs(rocksdb::Slice &slice, std::vector<VID_TYPE> &data)
{
    size_t sz = slice.size() / sizeof(VID_TYPE);
    const VID_TYPE *d = (const VID_TYPE *)slice.data();
    for (size_t i = 0; i < sz; ++i)
    {
        // std::cout << "\t" << d[i] << std::endl;
        data.push_back(d[i]);
    }
}

rocksdb::Status print(rocksdb::DB *db)
{
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        rocksdb::Slice ks = it->key();
        rocksdb::Slice vs = it->value();
        std::cout << slice2key(ks) << ":";
        std::vector<VID_TYPE> nbrs;
        slice2nbrs(vs, nbrs);
        for (auto nbr : nbrs)
        {
            std::cout << nbr << " ";
        }
        std::cout << std::endl;
    }
    rocksdb::Status stat = it->status(); // indicate any errors found during the scan
    delete it;
    return stat;
}

//<binname> g_path db_path
int main(int argc, char **argv)
{
    if (argc < 3)
    {
        std::cout << "usage: <binname> g_path db_path" << std::endl;
        return -1;
    }
    std::string g_path = argv[1];  //"../test_graph.txt";
    std::string db_path = argv[2]; //"../testdb";

    // open
    rocksdb::DB *db;
    rocksdb::Options options;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);

    // missing: create db
    if (status.ok() == 0)
    {
        std::cout << status.ToString() << std::endl;

        // free
        delete db;

        options.create_if_missing = true;
        status = rocksdb::DB::Open(options, db_path, &db);
        REPORT_STATUS_ERROR_DEL(-1);

        // insert by edge
        std::ifstream fin(g_path);
        VID_TYPE s, t;
        uint64_t edge_num = 0;
        while (fin >> s >> t)
        {
            status = add_edge(db, s, t);
            REPORT_STATUS_ERROR_DEL(-1);
            ++edge_num;
            if (edge_num % 10000 == 0)
            {
                std::cout << "\t" << edge_num << std::endl;
            }
        }
        fin.close();
    }

    // print
    status = print(db);
    REPORT_STATUS_ERROR_DEL(-1);

    // free
    delete db;
    return 0;
}
