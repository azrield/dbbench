/******************************************************************************************************/
/*                                                                                                    */
/*    INFINIDAT LTD. - Proprietary and Confidential Material                                          */
/*                                                                                                    */
/*    Copyright (C) 2024, Infinidat Ltd. - All Rights Reserved                                     */
/*                                                                                                    */
/*    NOTICE: All information contained herein is, and remains the property                           */
/*    of Infinidat Ltd. All information contained herein is protected by                              */
/*    trade secret or copyright law. The intellectual and technical concepts                          */
/*    contained herein are proprietary to Infinidat Ltd., and may be                                  */
/*    protected by U.S. and foreign patents, or patents in progress.                                  */
/*                                                                                                    */
/*    Redistribution or use, in source or binary forms, with or without                               */
/*    modification, are strictly forbidden unless prior written permission                            */
/*    is obtained from Infinidat Ltd.                                                                 */
/*                                                                                                    */
/******************************************************************************************************/

#include <tkrzw_dbm.h>
#include <tkrzw_dbm_hash.h>
#include <tkrzw_dbm_tree.h>
#include <tkrzw_dbm_skip.h>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "dbb.h"

using namespace tkrzw;

// Configuration flags for Tkrzw options
static int FLAGS_write_buffer_size = 0;
static int FLAGS_max_file_size = 0;
static int FLAGS_cache_size = 1;
static int FLAGS_open_files = 0;

static DBM* db = nullptr;

static void db_open(int dbflags) {
    HashDBM* hash_db = new HashDBM(/*std::make_unique<PositionalParallelFile>()*/);  // Change this to TreeDBM or SkipDBM if needed

    HashDBM::TuningParameters params;
    params.update_mode = HashDBM::UPDATE_IN_PLACE;
    params.cache_buckets = FLAGS_cache_size > 0 ? FLAGS_cache_size : -1;
    params.num_buckets = 1000000;

    Status s = hash_db->OpenAdvanced(FLAGS_db, true, File::OPEN_TRUNCATE, params).OrDie();
    if (!s.IsOK()) {
        fprintf(stderr, "Open error: %s\n", s.GetMessage().c_str());
        delete hash_db;
        exit(1);
    }
//    fprintf(stdout, "Opened DB: %s\n", FLAGS_db);
    db = hash_db;
}

static void db_close() {
    if (db) {
        Status s = db->Close();
        if (!s.IsOK()) {
            fprintf(stderr, "Close error: %s\n", s.GetMessage().c_str());
        }
        delete db;
        db = nullptr;
    }
}

static void db_write(DBB_local *dl) {
    DBB_global *dg = dl->dl_global;

    DBB_val dv;
    dv.dv_size = FLAGS_value_size;
    int64_t bytes = 0;
    unsigned long i = 0;

    do {
        for (int j = 0; j < dg->dg_batchsize; j++) {
            const uint64_t k = (dg->dg_order == DO_FORWARD) ? i + j : (DBB_random(dl->dl_rndctx) % FLAGS_num);
            char key[100];
            snprintf(key, sizeof(key), "%016lx", k);

            // Generate random value
            DBB_randstring(dl, &dv);
            std::string value((const char*)dv.dv_data, dv.dv_size);

            // Write to Tkrzw database
            Status s = db->Set(key, value);
            if (!s.IsOK()) {
                fprintf(stderr, "Write error: %s\n", s.GetMessage().c_str());
                exit(1);
            }
            bytes += FLAGS_value_size + FLAGS_key_size;
            DBB_opdone(dl);
        }
        i += dg->dg_batchsize;
    } while (!DBB_done(dl));
    dl->dl_bytes += bytes;
}

static void db_read(DBB_local *dl) {
    DBB_global *dg = dl->dl_global;

    int64_t bytes = 0;
    size_t found = 0;
    std::string value;
    char key[100];

    do {
        const uint64_t k = DBB_random(dl->dl_rndctx) % FLAGS_num;
        snprintf(key, sizeof(key), "%016lx", k);

        Status s = db->Get(key, &value);
        if (s.IsOK()) {
            bytes += FLAGS_key_size + value.size();
            found++;
        }
        DBB_opdone(dl);
    } while (!DBB_done(dl));
    dl->dl_bytes += bytes;

    char msg[100];
    snprintf(msg, sizeof(msg), "(%zd found)", found);
    DBB_message(dl, msg);
}

static char *db_verstr() {
    static char vstr[32];
    snprintf(vstr, sizeof(vstr), "%s", "Tkrzw 1.0");  // Replace with actual Tkrzw version if available
    return vstr;
}

// Option descriptions for the benchmark tool
static arg_desc db_opts[] = {
    { "write_buffer_size", arg_int, &FLAGS_write_buffer_size },
    { "max_file_size", arg_int, &FLAGS_max_file_size },
    { "cache_size", arg_int, &FLAGS_cache_size },
    { "open_files", arg_int, &FLAGS_open_files },
    { NULL }
};

// Define Tkrzw backend for DBBench
static DBB_backend db_tkrzw = {
    "tkrzw",
    "tkrzw-1.0.32",
    db_opts,
    db_verstr,
    db_open,
    db_close,
    db_read,
    db_write
};

extern DBB_backend *dbb_backend;
DBB_backend *dbb_backend = &db_tkrzw;


