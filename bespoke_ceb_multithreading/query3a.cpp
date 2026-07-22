#include "query3a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
#include <atomic>
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cstring>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <numeric>
#include <unordered_set>
#include <vector>
#include <cstdlib>
#include <climits>

// SQL:
/** SELECT COUNT(*) FROM title as t,
movie_keyword as mk, keyword as k,
movie_companies as mc, company_name as cn,
company_type as ct, kind_type as kt,
cast_info as ci, name as n, role_type as rt
WHERE t.id = mk.movie_id AND t.id = mc.movie_id AND t.id = ci.movie_id
AND k.id = mk.keyword_id AND cn.id = mc.company_id
AND ct.id = mc.company_type_id AND kt.id = t.kind_id
AND ci.person_id = n.id AND ci.role_id = rt.id
AND (t.production_year <= YEAR1) AND (t.production_year >= YEAR2)
AND (k.keyword IN KEYWORD) AND (cn.country_code IN COUNTRY)
AND (ct.kind IN KIND1) AND (kt.kind IN KIND2)
AND (rt.role IN ROLE) AND (n.gender IN GENDER) */

std::vector<std::vector<std::string>> run_q3a(Database* db, const Q3aArgs& args) {
    if (!db) throw std::runtime_error("run_q3a: db is null");
    PROFILE_SCOPE("q3a_total");

    int year1 = -1, year2 = -1;
    for (char c : args.YEAR1) if (c >= '0' && c <= '9') year1 = (year1 < 0 ? 0 : year1) * 10 + (c - '0');
    for (char c : args.YEAR2) if (c >= '0' && c <= '9') year2 = (year2 < 0 ? 0 : year2) * 10 + (c - '0');

    // -----------------------------------------------------------------------
    // Resolve small dimension tables
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> kind1_set(args.KIND1.begin(), args.KIND1.end());
    bool kind1_null_ok = kind1_set.count("<<NULL>>") > 0;
    const auto& ct_table = db->company_type;
    const int32_t max_ct_id = ct_table.id.empty() ? 0
        : *std::max_element(ct_table.id.begin(), ct_table.id.end());
    std::vector<uint8_t> valid_ct_bits(max_ct_id + 1, 0);
    for (size_t i = 0; i < ct_table.id.size(); ++i)
        if (kind1_set.count(ct_table.kind[i]))
            valid_ct_bits[ct_table.id[i]] = 1;

    std::unordered_set<std::string> kind2_set(args.KIND2.begin(), args.KIND2.end());
    bool kind2_null_ok = kind2_set.count("<<NULL>>") > 0;
    const auto& kt_table = db->kind_type;
    const int32_t max_kt_id = kt_table.id.empty() ? 0
        : *std::max_element(kt_table.id.begin(), kt_table.id.end());
    std::vector<uint8_t> valid_kt_bits(max_kt_id + 1, 0);
    for (size_t i = 0; i < kt_table.id.size(); ++i)
        if (kind2_set.count(kt_table.kind[i]))
            valid_kt_bits[kt_table.id[i]] = 1;

    std::unordered_set<std::string> role_set(args.ROLE.begin(), args.ROLE.end());
    bool role_null_ok = role_set.count("<<NULL>>") > 0;
    const auto& rt_table = db->role_type;
    const int32_t max_rt_id = rt_table.id.empty() ? 0
        : *std::max_element(rt_table.id.begin(), rt_table.id.end());
    std::vector<uint8_t> valid_rt_bits(max_rt_id + 1, 0);
    for (size_t i = 0; i < rt_table.id.size(); ++i)
        if (role_set.count(rt_table.role[i]))
            valid_rt_bits[rt_table.id[i]] = 1;

    const auto& nm = db->name;
    uint8_t accepted_gender[256] = {};
    bool gender_null_ok = false;
    for (const std::string& g : args.GENDER) {
        if (g == "<<NULL>>") { gender_null_ok = true; accepted_gender[0] = 1; }
        else if (!g.empty()) accepted_gender[(uint8_t)g[0]] = 1;
    }
    const int32_t  nm_itr_sz     = (int32_t)nm.id_to_row.size();
    const int32_t* nm_id_to_row  = nm.id_to_row.data();
    const uint8_t* nm_gender_byte= nm.gender_byte.data();
    TRACE_COUNT("q3a_valid_persons", (int64_t)nm.id.size());
    { PROFILE_SCOPE("q3a_name_gender_build"); }

    // Keyword resolve
    const auto& kw_table = db->keyword;
    const int32_t max_keyword_id = kw_table.id.empty() ? 0
        : *std::max_element(kw_table.id.begin(), kw_table.id.end());
    std::vector<uint8_t> valid_keyword_bits(max_keyword_id + 1, 0);
    bool keyword_null_ok = false;
    int64_t n_valid_keywords = 0;
    {
        PROFILE_SCOPE("q3a_kw_resolve");
        for (const std::string& kw : args.KEYWORD) {
            if (kw == "<<NULL>>") { keyword_null_ok = true; continue; }
            auto it = kw_table.str_to_ids.find(kw);
            if (it != kw_table.str_to_ids.end())
                for (int32_t kid : it->second)
                    if (kid >= 0 && kid <= max_keyword_id && !valid_keyword_bits[kid]) {
                        valid_keyword_bits[kid] = 1; ++n_valid_keywords;
                    }
        }
        TRACE_COUNT("q3a_valid_keyword_ids", n_valid_keywords);
    }

    // Country filter
    bool country_null_ok = false;
    for (const std::string& s : args.COUNTRY) if (s == "<<NULL>>") country_null_ok = true;
    const auto& cn_table = db->company_name;
    const int32_t max_cn_id = cn_table.id_to_row.empty() ? 0
        : (int32_t)cn_table.id_to_row.size() - 1;
    std::vector<uint8_t> valid_cn_bits(max_cn_id + 1, 0);
    std::vector<uint64_t> country_u64_vec;
    country_u64_vec.reserve(args.COUNTRY.size());
    for (const std::string& s : args.COUNTRY) {
        if (s == "<<NULL>>") continue;
        uint64_t v = 0;
        std::memcpy(&v, s.data(), std::min(s.size(), (size_t)8));
        country_u64_vec.push_back(v);
    }

    const int32_t max_movie_id = (int32_t)db->title.id_to_row.size() - 1;
    const int n_threads_val = pool.num_threads;

    // -----------------------------------------------------------------------
    // Phase 0: Build valid_cn_bits (parallel 234K cn_table scan).
    // -----------------------------------------------------------------------
    {
        PROFILE_SCOPE("q3a_company_name_build");
        const int32_t n_cn = (int32_t)cn_table.id.size();
        const uint64_t* __restrict__ cc_u64 = cn_table.country_code_u64.data();
        const int32_t* __restrict__ cn_id_a  = cn_table.id.data();
        const size_t n_countries = country_u64_vec.size();
        uint8_t* __restrict__ cn_bits_ptr = valid_cn_bits.data();
        std::atomic<int64_t> cn_valid_count_atomic{0};
        pool.parallel_for([&](int tid, int n_threads) {
            const int32_t cn_chunk = (n_cn + n_threads - 1) / n_threads;
            const int32_t cn_beg = tid * cn_chunk;
            const int32_t cn_end = std::min(cn_beg + cn_chunk, n_cn);
            int64_t local_valid = 0;
            for (int32_t i = cn_beg; i < cn_end; ++i) {
                const uint64_t v = cc_u64[i];
                bool matches = (v == 0) ? country_null_ok : false;
                if (!matches)
                    for (size_t j = 0; j < n_countries && !matches; ++j)
                        matches = (v == country_u64_vec[j]);
                if (matches) {
                    const int32_t cid = cn_id_a[i];
                    if (cid >= 0 && cid <= max_cn_id) {
                        __atomic_store_n(&cn_bits_ptr[cid], 1, __ATOMIC_RELAXED);
                        ++local_valid;
                    }
                }
            }
            cn_valid_count_atomic.fetch_add(local_valid, std::memory_order_relaxed);
        });
        TRACE_COUNT("q3a_valid_company_ids", cn_valid_count_atomic.load());
    }

    // -----------------------------------------------------------------------
    // Phase 1: Parallel mk scan + inline mc probe + title filter.
    //
    // Optimizations vs baseline:
    // 1. No mk_flat/mc_flat (no ~20MB zero-init, saves ~1ms).
    // 2. Inline mc probe: only ~1M mc rows checked (vs 2.6M full scan).
    // 3. Load-balanced movie_id-range splits → no atomics on shared state.
    // -----------------------------------------------------------------------
    struct MovieMult { int32_t movie_id; int64_t mult; };
    std::vector<std::vector<MovieMult>> per_thread_movies(n_threads_val);
    for (auto& v : per_thread_movies) v.reserve(64);

    std::atomic<int64_t> mk_rows_emitted_atomic{0}, mc_rows_emitted_atomic{0};
    std::atomic<int64_t> title_rows_scanned_total{0}, title_rows_emitted_total{0};

    {
        PROFILE_SCOPE("q3a_movie_keyword_build");

        const auto& mk = db->movie_keyword;
        const int32_t kbits_sz = (int32_t)valid_keyword_bits.size();
        const int32_t n_mk_rows = (int32_t)mk.movie_id.size();
        const int32_t* __restrict__ mk_mid = mk.movie_id.data();
        const int32_t* __restrict__ mk_kid = mk.keyword_id.data();
        const int32_t mk_csr_max_key = (int32_t)mk.movie_id_csr.offsets.size() - 2;

        const auto& mc = db->movie_companies;
        const int32_t ct_sz = (int32_t)valid_ct_bits.size();
        const int32_t cn_bits_sz = (int32_t)valid_cn_bits.size();
        const int32_t* __restrict__ mc_ctid_arr = mc.company_type_id.data();
        const int32_t* __restrict__ mc_cid_arr  = mc.company_id.data();
        const auto& mc_csr = mc.movie_id_csr;

        const auto& t = db->title;
        const int32_t* __restrict__ t_id_to_row = t.id_to_row.data();
        const int32_t t_itr_sz = (int32_t)t.id_to_row.size();
        const int32_t kt_sz = (int32_t)valid_kt_bits.size();

        // Load-balanced row splits for mk (snapped to movie_id boundaries).
        std::vector<int32_t> mk_row_beg(n_threads_val), mk_row_end(n_threads_val);
        {
            const int32_t* off = mk.movie_id_csr.offsets.data();
            for (int thr = 0; thr < n_threads_val; ++thr) {
                const int32_t tb = (int64_t)thr * n_mk_rows / n_threads_val;
                const int32_t te = (int64_t)(thr + 1) * n_mk_rows / n_threads_val;
                auto find = [&](int32_t row_target) -> int32_t {
                    if (row_target <= 0) return 0;
                    if (row_target >= n_mk_rows) return mk_csr_max_key + 1;
                    int32_t lo = 0, hi = mk_csr_max_key + 1;
                    while (lo < hi) { int32_t m = lo + (hi - lo) / 2; if (off[m] < row_target) lo = m + 1; else hi = m; }
                    return lo;
                };
                int32_t mb = find(tb), me = find(te);
                mk_row_beg[thr] = (mb <= mk_csr_max_key) ? off[mb] : n_mk_rows;
                mk_row_end[thr] = (me <= mk_csr_max_key) ? off[me] : n_mk_rows;
            }
            mk_row_end[n_threads_val - 1] = n_mk_rows;
        }

        pool.parallel_for([&](int tid, int n_threads) {
            auto& my_movies = per_thread_movies[tid];
            int64_t mk_rows = 0, mc_rows = 0;
            int64_t rows_scanned = 0, rows_emitted = 0;

            const int32_t mk_lo = mk_row_beg[tid];
            const int32_t mk_hi = mk_row_end[tid];
            int32_t cur_mid = -1, cur_mk_count = 0;

            auto process_movie = [&](int32_t movie_id, int32_t mk_count) {
                // Count mc matches via CSR (sequential since movie_ids increase in our band).
                auto [mc_beg, mc_end] = mc_csr.range(movie_id);
                int32_t mc_count = 0;
                for (int32_t mr = mc_beg; mr < mc_end; ++mr) {
                    const int32_t cid = mc_cid_arr[mr];
                    bool cn_ok = (cid == -1) ? country_null_ok
                        : (cid < cn_bits_sz ? (bool)valid_cn_bits[cid] : false);
                    if (!cn_ok) continue;
                    const int32_t ct_id = mc_ctid_arr[mr];
                    bool ct_ok = (ct_id >= 0 && ct_id < ct_sz) ? (bool)valid_ct_bits[ct_id]
                        : (ct_id == -1 && kind1_null_ok);
                    if (ct_ok) ++mc_count;
                }
                mc_rows += (mc_end - mc_beg);
                if (mc_count == 0) return;
                if (movie_id >= t_itr_sz) return;
                const int32_t trow = t_id_to_row[movie_id];
                if (trow < 0) return;
                ++rows_scanned;
                const int32_t ktid = t.kind_id[trow];
                bool kt_ok = (ktid >= 0 && ktid < kt_sz) ? (bool)valid_kt_bits[ktid]
                    : (ktid == -1 && kind2_null_ok);
                if (!kt_ok) return;
                const int32_t py = t.production_year[trow];
                if (py == -1 || (year1 >= 0 && py > year1) || (year2 >= 0 && py < year2)) return;
                my_movies.push_back({movie_id, (int64_t)mk_count * mc_count});
                ++rows_emitted;
            };

            for (int32_t r = mk_lo; r < mk_hi; ++r) {
                const int32_t kid = mk_kid[r];
                // Check keyword filter: random access to valid_keyword_bits (fits in L3).
                bool valid = (kid == -1) ? keyword_null_ok
                    : (kid < kbits_sz ? (bool)valid_keyword_bits[kid] : false);
                if (!valid) continue;
                const int32_t mid = mk_mid[r];
                if (mid < 0 || mid > max_movie_id) continue;
                ++mk_rows;
                if (mid == cur_mid) {
                    ++cur_mk_count;
                } else {
                    if (cur_mid >= 0 && cur_mk_count > 0)
                        process_movie(cur_mid, cur_mk_count);
                    cur_mid = mid;
                    cur_mk_count = 1;
                }
            }
            if (cur_mid >= 0 && cur_mk_count > 0)
                process_movie(cur_mid, cur_mk_count);

            mk_rows_emitted_atomic.fetch_add(mk_rows, std::memory_order_relaxed);
            mc_rows_emitted_atomic.fetch_add(mc_rows, std::memory_order_relaxed);
            title_rows_scanned_total.fetch_add(rows_scanned, std::memory_order_relaxed);
            title_rows_emitted_total.fetch_add(rows_emitted, std::memory_order_relaxed);
        });

        {
            const int64_t r = mk_rows_emitted_atomic.load();
            TRACE_COUNT("q3a_mk_scan_rows_in", r);
            TRACE_COUNT("q3a_mk_scan_rows_emitted", r);
        }
        {
            PROFILE_SCOPE("q3a_movie_companies_build");
            const int64_t r = mc_rows_emitted_atomic.load();
            TRACE_COUNT("q3a_mc_scan_rows_in", r);
            TRACE_COUNT("q3a_mc_scan_rows_emitted", r);
        }
    }

    TRACE_COUNT("q3a_title_rows_scanned", title_rows_scanned_total.load());
    TRACE_COUNT("q3a_title_rows_emitted", title_rows_emitted_total.load());

    // Flatten per_thread_movies
    std::vector<MovieMult> valid_movies;
    {
        size_t total = 0;
        for (auto& v : per_thread_movies) total += v.size();
        valid_movies.reserve(total);
        for (auto& v : per_thread_movies)
            for (auto& m : v) valid_movies.push_back(m);
    }

    // -----------------------------------------------------------------------
    // Phase 2: Parallel cast_info probe.
    // -----------------------------------------------------------------------
    std::atomic<int64_t> count_atomic{0};
    {
        PROFILE_SCOPE("q3a_cast_info_probe");
        const auto& ci  = db->cast_info;
        const auto& csr = ci.movie_id_csr;
        const int32_t rt_sz  = (int32_t)valid_rt_bits.size();
        const int32_t* __restrict__ role_id_arr   = ci.role_id.data();
        const int32_t* __restrict__ person_id_arr = ci.person_id.data();
        const int32_t n_valid_movies = (int32_t)valid_movies.size();
        std::atomic<int64_t> total_probe_in{0}, total_join_emitted{0};

        pool.parallel_for([&](int tid, int n_threads) {
            int64_t local_count = 0, probe_rows_in = 0, join_rows_emitted = 0;
            const int32_t chunk = (n_valid_movies + n_threads - 1) / n_threads;
            const int32_t m_beg = tid * chunk;
            const int32_t m_end = std::min(m_beg + chunk, n_valid_movies);
            for (int32_t mi = m_beg; mi < m_end; ++mi) {
                const MovieMult& mm = valid_movies[mi];
                const int32_t mid  = mm.movie_id;
                const int64_t mult = mm.mult;
                auto [beg, end] = csr.range(mid);
                for (int32_t r = beg; r < end; ++r) {
                    ++probe_rows_in;
                    const int32_t rid = role_id_arr[r];
                    bool rid_ok = (rid >= 0 && rid < rt_sz) ? (bool)valid_rt_bits[rid]
                        : (rid == -1 && role_null_ok);
                    if (!rid_ok) continue;
                    const int32_t pid = person_id_arr[r];
                    if ((uint32_t)pid >= (uint32_t)nm_itr_sz) continue;
                    const int32_t prow = nm_id_to_row[pid];
                    if (prow < 0) continue;
                    if (!accepted_gender[nm_gender_byte[prow]]) continue;
                    local_count += mult;
                    ++join_rows_emitted;
                }
            }
            count_atomic.fetch_add(local_count, std::memory_order_relaxed);
            total_probe_in.fetch_add(probe_rows_in, std::memory_order_relaxed);
            total_join_emitted.fetch_add(join_rows_emitted, std::memory_order_relaxed);
        });

        TRACE_COUNT("q3a_cast_probe_rows_in", total_probe_in.load());
        TRACE_COUNT("q3a_join_rows_emitted", total_join_emitted.load());
    }

    TRACE_COUNT("q3a_valid_movies", (int64_t)valid_movies.size());
    TRACE_COUNT("q3a_query_output_rows", 1);
    const int64_t count = count_atomic.load(std::memory_order_relaxed);
    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}