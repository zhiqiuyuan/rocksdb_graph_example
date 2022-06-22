#include "rocksdb/db.h"
#include <iostream>
#include <cassert>
#include <fstream>

rocksdb::Status add_nbr(rocksdb::DB *db, uint32_t s, uint32_t nbr)
{
    std::string snbrs;
    rocksdb::Slice skey = rocksdb::Slice((const char *)&s, 4);
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), skey, &snbrs);
    // std::cout << status.ToString() << std::endl;
    if (status.ok())
    {
        const char *str = snbrs.c_str();
        size_t sz = snbrs.size();
        *((uint32_t *)(str + sz)) = nbr;
        status = db->Put(rocksdb::WriteOptions(), skey, rocksdb::Slice(str, sz + 4));
    }
    else if (status.IsNotFound())
    {
        status = db->Put(rocksdb::WriteOptions(), skey, rocksdb::Slice((const char *)&nbr, 4));
    }
    return status;
}

rocksdb::Status add_edge(rocksdb::DB *db, uint32_t s, uint32_t t)
{
    rocksdb::Status status = add_nbr(db, s, t);
    if (status.ok())
    {
        status = add_nbr(db, t, s);
    }
    return status;
}

void slice2uint32_vec(rocksdb::Slice &slice, std::vector<uint32_t> &data)
{
    size_t sz = slice.size() / 4;
    const uint32_t *d = (const uint32_t *)slice.data();
    for (size_t i = 0; i < sz; ++i)
    {
        // std::cout << "\t" << d[i] << std::endl;
        data.push_back(d[i]);
    }
}
uint32_t slice2uint32(rocksdb::Slice &slice)
{
    const uint32_t *d = (const uint32_t *)slice.data();
    // std::cout << "\t" << d[0] << std::endl;
    return d[0];
}

int main()
{
    std::string db_path = "../testdb";
    std::string g_path = "../test_graph.txt";

    // create
    rocksdb::DB *db;
    rocksdb::Options options;
    // options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);

    // missing: create db
    if (status.ok() == 0)
    {
        std::cout << status.ToString() << std::endl;
        // Invalid argument: ../testdb/CURRENT: does not exist (create_if_missing is false)

        options.create_if_missing = true;
        status = rocksdb::DB::Open(options, db_path, &db);
        assert(status.ok());

        // insert by edge
        std::ifstream fin(g_path);
        uint32_t s, t;
        while (fin >> s >> t)
        {
            add_edge(db, s, t);
        }
        fin.close();
    }

    // print
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        rocksdb::Slice ks = it->key();
        rocksdb::Slice vs = it->value();
        std::cout << slice2uint32(ks) << ":";
        std::vector<uint32_t> nbrs;
        slice2uint32_vec(vs, nbrs);
        for (auto nbr : nbrs)
        {
            std::cout << nbr << " ";
        }
        std::cout << std::endl;
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;

    // free
    delete db;
    return 0;
}