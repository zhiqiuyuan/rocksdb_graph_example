#include "rocksdb/db.h"
#include <iostream>
#include <cassert>
#include <fstream>
#include <unordered_set>
typedef uint32_t VID_TYPE;
#define REPORT_STATUS_ERROR(return_code)             \
    if (status.ok() == 0)                            \
    {                                                \
        std::cout << status.ToString() << std::endl; \
        return return_code;                          \
    }
#define REPORT_STATUS_ERROR_DEL(return_code)         \
    if (status.ok() == 0)                            \
    {                                                \
        std::cout << status.ToString() << std::endl; \
        delete db;                                   \
        return return_code;                          \
    }
#define MAKE_KEY_SLICE(s) rocksdb::Slice((const char *)&s, sizeof(VID_TYPE))
#define END_OF_VERTEX (VID_TYPE)(-1)
// if next_vid==END_OF_VERTEX: there's no more vertex
// a record (key:END_OF_VERTEX, value:begin_vid) is stored in db
#define HEADER_LEN 3 // header len in each value: next vertex; degree; first neighbor's index in neighbors

void print_value(const char *str, size_t bytes_len)
{
    const VID_TYPE *d = (const VID_TYPE *)str;
    size_t len = bytes_len / sizeof(VID_TYPE);
    for (size_t i = 0; i < len; ++i)
    {
        std::cout << d[i] << " ";
    }
    std::cout << std::endl;
}

// last nbr's next_nbr_idx is set END_OF_VERTEX by link_vertices
rocksdb::Status add_nbr(rocksdb::DB *db, VID_TYPE s, VID_TYPE nbr)
{
    std::string info;
    rocksdb::Slice skey = MAKE_KEY_SLICE(s);
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), skey, &info);
    if (status.ok())
    {
        // add nbr
        size_t sz = info.size();
        info.resize(sz + 2 * sizeof(VID_TYPE));

        const char *str = info.c_str();
        VID_TYPE deg = *((VID_TYPE *)(str + sizeof(VID_TYPE)));
        *((VID_TYPE *)(str + sz)) = nbr;
        *((VID_TYPE *)(str + sz + sizeof(VID_TYPE))) = deg + 1; // next nbr idx

        // update degree
        *((VID_TYPE *)(str + sizeof(VID_TYPE))) = deg + 1;

        // print_value(str, sz + 2 * sizeof(VID_TYPE));

        status = db->Put(rocksdb::WriteOptions(), skey, rocksdb::Slice(str, info.size()));
    }
    else if (status.IsNotFound())
    {
        // next_vid, degree, first_nbr_idx, nbr, next_nbr_idx
        VID_TYPE value[HEADER_LEN + 2] = {END_OF_VERTEX, 1, 0, nbr, 1};
        status = db->Put(rocksdb::WriteOptions(), skey, rocksdb::Slice((const char *)value, (HEADER_LEN + 2) * sizeof(VID_TYPE)));
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
VID_TYPE str2key(std::string &slice)
{
    const VID_TYPE *d = (const VID_TYPE *)slice.c_str();
    // std::cout << "\t" << d[0] << std::endl;
    return d[0];
}

void slice2nbrs(rocksdb::Slice &slice, std::vector<VID_TYPE> &data)
{
    VID_TYPE *d = (VID_TYPE *)slice.data();
    size_t pos = d[2];
    size_t deg = d[1];
    d += HEADER_LEN; // now d starts at nbr
    for (size_t i = 0; i < deg; ++i)
    {
        // std::cout << "\t" << d[2*pos] << std::endl;
        data.push_back(d[2 * pos]);
        pos = d[2 * pos + 1];
    }
}
void str2nbrs(std::string &slice, std::vector<VID_TYPE> &data)
{
    VID_TYPE *d = (VID_TYPE *)slice.c_str();
    size_t pos = d[2];
    size_t deg = d[1];
    d += HEADER_LEN; // now d starts at nbr
    for (size_t i = 0; i < deg; ++i)
    {
        // std::cout << "\t" << d[2*pos] << std::endl;
        data.push_back(d[2 * pos]);
        pos = d[2 * pos + 1];
    }
}

void slice2vec(rocksdb::Slice &slice, std::vector<VID_TYPE> &data)
{
    size_t sz = slice.size() / sizeof(VID_TYPE);
    const VID_TYPE *d = (const VID_TYPE *)slice.data();
    for (size_t i = 0; i < sz; ++i)
    {
        // std::cout << "\t" << d[i] << std::endl;
        data.push_back(d[i]);
    }
}

rocksdb::Status print_all(rocksdb::DB *db)
{
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        rocksdb::Slice ks = it->key();
        rocksdb::Slice vs = it->value();
        VID_TYPE k = slice2key(ks);
        std::cout << k << ":";
        if (k == END_OF_VERTEX) // begin_vid record, not a vertex adjlist record
        {
            std::cout << "begin_vid:" << slice2key(vs) << std::endl;
            continue;
        }
        // print_value(vs.data(), vs.size());
        std::vector<VID_TYPE> nbrs;
        slice2nbrs(vs, nbrs);
        std::vector<VID_TYPE> info;
        slice2vec(vs, info);
        size_t deg = info[1];
        std::cout << "next_vid:" << info[0] << " degree:" << deg << " first_neighbor_idx:" << info[2] << std::endl;
        for (size_t i = 1; i <= deg; ++i)
        {
            std::cout << "\t" << info[2 * i + 1] << " next_nbr_idx:" << info[2 * i + 2] << std::endl;
        }
        std::cout << std::endl;
    }
    rocksdb::Status stat = it->status(); // indicate any errors found during the scan
    delete it;
    return stat;
}

rocksdb::Status print(rocksdb::DB *db)
{
    rocksdb::Slice ks;
    std::string begin_vids;
    VID_TYPE k = END_OF_VERTEX;
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), MAKE_KEY_SLICE(k), &begin_vids);
    k = str2key(begin_vids);
    while (k != END_OF_VERTEX)
    {
        ks = MAKE_KEY_SLICE(k);
        std::cout << k << ":";

        std::string info;
        status = db->Get(rocksdb::ReadOptions(), ks, &info);
        REPORT_STATUS_ERROR(status);
        std::vector<VID_TYPE> nbrs;
        str2nbrs(info, nbrs);
        for (auto nbr : nbrs)
        {
            std::cout << nbr << " ";
        }
        std::cout << std::endl;

        k = ((VID_TYPE *)info.c_str())[0];
    }
    return status;
}

rocksdb::Status link_vertices(rocksdb::DB *db)
{
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    VID_TYPE pre_vid;
    rocksdb::Slice ks, pre_ks;
    bool is_first = 1;
    rocksdb::Status status;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        ks = it->key();
        VID_TYPE k = slice2key(ks);
        if (k == END_OF_VERTEX) // begin_vid record, not a vertex adjlist record
        {
            continue;
        }
        if (is_first == 0)
        {
            std::string info;
            pre_ks = rocksdb::Slice((const char *)&pre_vid, sizeof(VID_TYPE));
            status = db->Get(rocksdb::ReadOptions(), pre_ks, &info);
            REPORT_STATUS_ERROR(status);

            VID_TYPE *d = (VID_TYPE *)info.c_str();
            d[0] = k; // link pre_vid to k

            // print_value((const char *)d, info.size());

            status = db->Put(rocksdb::WriteOptions(), pre_ks, rocksdb::Slice((const char *)d, info.size()));
            REPORT_STATUS_ERROR(status);
        }
        else
        {
            // store begin_vid(k)
            VID_TYPE begin_vid_key = END_OF_VERTEX;
            status = db->Put(rocksdb::WriteOptions(), MAKE_KEY_SLICE(begin_vid_key), MAKE_KEY_SLICE(k));
            REPORT_STATUS_ERROR(status);
        }
        pre_vid = k;
        is_first = 0;
    }
    status = it->status(); // indicate any errors found during the scan
    delete it;
    return status;
}

// update db -> only contain all vertices in vid_set and all edges between vid_set
rocksdb::Status write_graph(rocksdb::DB *db, std::unordered_set<VID_TYPE> vid_set)
{
    VID_TYPE pre_vid;
    rocksdb::Slice ks, pre_ks;
    bool is_first = 1;
    rocksdb::Status status;
    for (VID_TYPE k : vid_set)
    {
        if (is_first == 0)
        {
            std::string info;
            pre_ks = rocksdb::Slice((const char *)&pre_vid, sizeof(VID_TYPE));
            status = db->Get(rocksdb::ReadOptions(), pre_ks, &info);
            REPORT_STATUS_ERROR(status);

            VID_TYPE *d = (VID_TYPE *)info.c_str();

            // print_value((const char *)d, info.size());

            d[0] = k; // link pre_vid to k

            // link nbrs
            size_t sz = info.size();
            size_t fake_deg = (sz / sizeof(VID_TYPE) - HEADER_LEN) / 2;
            size_t deg = 0;
            VID_TYPE *nbrs = d + HEADER_LEN;
            size_t pre_nbr_idx;
            for (size_t i = 0; i < fake_deg; ++i)
            {
                if (vid_set.count(nbrs[2 * i]))
                {
                    if (deg == 0)
                    {
                        // first neighbor
                        d[2] = i;
                    }
                    else
                    {
                        nbrs[2 * pre_nbr_idx + 1] = i;
                    }
                    pre_nbr_idx = i;
                    ++deg;
                }
            }
            d[1] = deg;

            // print_value((const char *)d, info.size());

            status = db->Put(rocksdb::WriteOptions(), pre_ks, rocksdb::Slice((const char *)d, info.size()));
            REPORT_STATUS_ERROR(status);
        }
        else
        {
            // store begin_vid(k)
            VID_TYPE begin_vid_key = END_OF_VERTEX;
            status = db->Put(rocksdb::WriteOptions(), MAKE_KEY_SLICE(begin_vid_key), MAKE_KEY_SLICE(k));
            REPORT_STATUS_ERROR(status);
        }
        pre_vid = k;
        is_first = 0;
    }
    // last vertex
    std::string info;
    pre_ks = rocksdb::Slice((const char *)&pre_vid, sizeof(VID_TYPE));
    status = db->Get(rocksdb::ReadOptions(), pre_ks, &info);
    REPORT_STATUS_ERROR(status);

    VID_TYPE *d = (VID_TYPE *)info.c_str();

    // print_value((const char *)d, info.size());

    d[0] = END_OF_VERTEX; // indicate no more vertex

    // link nbrs
    size_t sz = info.size();
    size_t fake_deg = (sz / sizeof(VID_TYPE) - HEADER_LEN) / 2;
    size_t deg = 0;
    VID_TYPE *nbrs = d + HEADER_LEN;
    size_t pre_nbr_idx;
    for (size_t i = 0; i < fake_deg; ++i)
    {
        if (vid_set.count(nbrs[2 * i]))
        {
            if (deg == 0)
            {
                // first neighbor
                d[2] = i;
            }
            else
            {
                nbrs[2 * pre_nbr_idx + 1] = i;
            }
            pre_nbr_idx = i;
            ++deg;
        }
    }
    d[1] = deg;

    // print_value((const char *)d, info.size());

    status = db->Put(rocksdb::WriteOptions(), pre_ks, rocksdb::Slice((const char *)d, info.size()));
    REPORT_STATUS_ERROR(status);

    return status;
}

rocksdb::Status recover(rocksdb::DB *db)
{
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    VID_TYPE pre_vid;
    rocksdb::Slice ks, pre_ks;
    bool is_first = 1;
    rocksdb::Status status;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        ks = it->key();
        VID_TYPE k = slice2key(ks);
        if (k == END_OF_VERTEX) // begin_vid record, not a vertex adjlist record
        {
            continue;
        }
        if (is_first == 0)
        {
            std::string info;
            pre_ks = rocksdb::Slice((const char *)&pre_vid, sizeof(VID_TYPE));
            status = db->Get(rocksdb::ReadOptions(), pre_ks, &info);
            REPORT_STATUS_ERROR(status);

            VID_TYPE *d = (VID_TYPE *)info.c_str();

            // print_value((const char *)d, info.size());

            d[0] = k; // link pre_vid to k

            // recover next_nbr_idx
            size_t sz = info.size();
            size_t deg = (sz / sizeof(VID_TYPE) - HEADER_LEN) / 2;
            d[1] = deg;
            d[2] = 0;
            VID_TYPE *nbrs = d + HEADER_LEN;
            for (size_t i = 0; i < deg; ++i)
            {
                nbrs[2 * i + 1] = i + 1;
            }

            // print_value((const char *)d, info.size());

            status = db->Put(rocksdb::WriteOptions(), pre_ks, rocksdb::Slice((const char *)d, sz));
            REPORT_STATUS_ERROR(status);
        }
        else
        {
            // store begin_vid(k)
            VID_TYPE begin_vid_key = END_OF_VERTEX;
            status = db->Put(rocksdb::WriteOptions(), MAKE_KEY_SLICE(begin_vid_key), MAKE_KEY_SLICE(k));
            REPORT_STATUS_ERROR(status);
        }
        pre_vid = k;
        is_first = 0;
    }
    // last vertex
    std::string info;
    pre_ks = rocksdb::Slice((const char *)&pre_vid, sizeof(VID_TYPE));
    status = db->Get(rocksdb::ReadOptions(), pre_ks, &info);
    REPORT_STATUS_ERROR(status);

    VID_TYPE *d = (VID_TYPE *)info.c_str();

    // print_value((const char *)d, info.size());

    d[0] = END_OF_VERTEX; // indicate no more vertex
    // recover next_nbr_idx
    size_t sz = info.size();
    size_t deg = (sz / sizeof(VID_TYPE) - HEADER_LEN) / 2;
    d[1] = deg;
    d[2] = 0;
    VID_TYPE *nbrs = d + HEADER_LEN;
    for (size_t i = 0; i < deg; ++i)
    {
        nbrs[2 * i + 1] = i + 1;
    }

    // print_value((const char *)d, info.size());

    status = db->Put(rocksdb::WriteOptions(), pre_ks, rocksdb::Slice((const char *)d, info.size()));
    REPORT_STATUS_ERROR(status);

    status = it->status(); // indicate any errors found during the scan
    delete it;
    return status;
}

// just wrapper functions:
int write_graph_test(rocksdb::DB *db, std::unordered_set<VID_TYPE> &set1)
{
    write_graph(db, set1);
    std::cout << "only leave vertices in set:{";
    for (VID_TYPE v : set1)
    {
        std::cout << v << " ";
    }
    std::cout << "}:" << std::endl;
    rocksdb::Status status = print(db);
    REPORT_STATUS_ERROR_DEL(-1);
    return 1;
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
    std::cout << "g_path:" << g_path << std::endl;
    std::cout << "db_path:" << db_path << std::endl;

    // open
    rocksdb::DB *db;
    rocksdb::Options options;
    options.create_if_missing = false;
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
            // std::cout << s << " " << t << std::endl;
            status = add_edge(db, s, t);
            REPORT_STATUS_ERROR_DEL(-1);
            ++edge_num;
            if (edge_num % 10000 == 0)
            {
                std::cout << "\t" << edge_num << std::endl;
            }
        }
        fin.close();

        status = link_vertices(db);
        REPORT_STATUS_ERROR_DEL(-1);
    }

    // print
    status = print_all(db);
    REPORT_STATUS_ERROR_DEL(-1);
    status = print(db);
    REPORT_STATUS_ERROR_DEL(-1);

    // reduce and recover test
    std::unordered_set<VID_TYPE> set1 = {0, 1, 2, 3, 4, 5, 6, 7, 8}; // del 9
    std::unordered_set<VID_TYPE> set2 = {0, 1, 2, 3, 4, 6, 7, 8};    // del 9 5
    std::unordered_set<VID_TYPE> set3 = {0, 1, 2, 3, 4, 7, 8};       // del 9 5 6
    if (write_graph_test(db, set1) == -1)
    {
        return -1;
    }
    if (write_graph_test(db, set2) == -1)
    {
        return -1;
    }
    if (write_graph_test(db, set3) == -1)
    {
        return -1;
    }
    recover(db);
    std::cout << "after recover:" << std::endl;
    status = print(db);
    REPORT_STATUS_ERROR_DEL(-1);

    // free
    delete db;
    return 0;
}
