#include "query4a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
#include <atomic>
#include <mutex>
static ThreadPool& pool = get_query_pool();

#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

// NULL sentinels in IN-lists never match stored null values in SQL semantics.
static inline bool is_null_sentinel(const std::string& s) {
    return s == "NULL" || s == "<<NULL>>";
}

std::vector<std::vector<std::string>> run_q4a(Database* db, const Q4aArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q4a: db is null");
    }
    PROFILE_SCOPE("q4a_total");

    // -----------------------------------------------------------------------
    // 1. Resolve valid role_ids from role_type (rt.role IN ROLE)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_role_ids;
    {
        const auto& rt = db->role_type;
        std::unordered_set<std::string> role_set;
        for (const auto& s : args.ROLE)
            if (!is_null_sentinel(s)) role_set.insert(s);
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (role_set.count(rt.role[i]))
                valid_role_ids.insert(rt.id[i]);
        TRACE_COUNT("q4a_valid_role_ids", (int64_t)valid_role_ids.size());
    }

    // -----------------------------------------------------------------------
    // 2. Resolve valid info_type_ids (it1.id IN ID)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_info_type_ids;
    for (const auto& s : args.ID)
        if (!is_null_sentinel(s))
            try { valid_info_type_ids.insert(std::stoi(s)); } catch (...) {}
    TRACE_COUNT("q4a_valid_info_type_ids", (int64_t)valid_info_type_ids.size());

    // -----------------------------------------------------------------------
    // 3. Build note filter for cast_info (ci.note IN NOTE)
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> note_set;
    for (const auto& s : args.NOTE)
        if (!is_null_sentinel(s)) note_set.insert(s);
    TRACE_COUNT("q4a_note_set_size", (int64_t)note_set.size());

    // -----------------------------------------------------------------------
    // 4. Gender filter on name table (n.gender IN GENDER)
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> gender_set;
    for (const auto& s : args.GENDER)
        if (!is_null_sentinel(s)) gender_set.insert(s);
    TRACE_COUNT("q4a_gender_set_size", (int64_t)gender_set.size());

    // -----------------------------------------------------------------------
    // 5. name_pcode_nf filter (n.name_pcode_nf IN NAME_PCODE_NF)
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> pcode_set;
    for (const auto& s : args.NAME_PCODE_NF)
        if (!is_null_sentinel(s)) pcode_set.insert(s);
    TRACE_COUNT("q4a_pcode_set_size", (int64_t)pcode_set.size());

    // -----------------------------------------------------------------------
    // 6. Main join
    //
    // All tables join on person_id (= name.id).
    // COUNT contribution per qualifying person:
    //   ci_count(p) × pi_count(p) × an_count(p)
    //
    // Execution strategy — two-phase depending on name filter selectivity:
    //
    // Phase A (fast-path): If gender_set or pcode_set is empty (no valid
    //   non-null values to match), the name table join always returns 0
    //   rows → entire result is 0.  Skip all large-table scans.
    //
    // Phase B (name-first): Scan name table (gender + pcode_nf filters),
    //   collect qualifying person_ids. Use CSR to probe aka_name, person_info,
    //   and cast_info for each qualifying person. This avoids scanning the
    //   full cast_info table (≥8M rows) when the name filter is selective.
    // -----------------------------------------------------------------------

    const auto& nm  = db->name;
    const auto& ci  = db->cast_info;
    const auto& pi1 = db->person_info;
    const auto& an  = db->aka_name;

    int64_t count = 0;
    TRACE_COUNT("q4a_name_rows_total", (int64_t)nm.id.size());

    int64_t ci_rows_probed = 0, ci_rows_matched = 0;
    int64_t persons_gender_pass = 0, persons_pcode_pass = 0;
    int64_t persons_an_pass = 0, persons_pi_pass = 0;

    // Phase A: fast-exit when any required filter set is empty
    if (gender_set.empty() || pcode_set.empty() || valid_role_ids.empty()
        || valid_info_type_ids.empty() || note_set.empty()) {
        TRACE_COUNT("q4a_fast_exit",           1);
        TRACE_COUNT("q4a_ci_rows_probed",       0);
        TRACE_COUNT("q4a_ci_rows_matched",      0);
        TRACE_COUNT("q4a_persons_gender_pass",  0);
        TRACE_COUNT("q4a_persons_pcode_pass",   0);
        TRACE_COUNT("q4a_persons_an_pass",      0);
        TRACE_COUNT("q4a_persons_pi_pass",      0);
        TRACE_COUNT("q4a_query_output_rows",    1);
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"count_star()"});
        rows.push_back({"0"});
        return rows;
    }

    // Phase B: name-first scan
    // Scan name table applying gender + pcode_nf filters.
    // For each qualifying person, use CSR to probe aka_name, person_info,
    // and cast_info (with note + role_id filters).
    // Parallelized with morsel-driven approach: each thread scans a disjoint
    // range of name rows and accumulates a local count.
    {
        PROFILE_SCOPE("q4a_name_scan");

        const int32_t total_rows = (int32_t)nm.id.size();
        const int n_threads = pool.num_threads;

        // Per-thread local accumulators (avoid false sharing with padding)
        struct alignas(64) LocalAcc {
            int64_t count            = 0;
            int64_t ci_rows_probed   = 0;
            int64_t ci_rows_matched  = 0;
            int64_t gender_pass      = 0;
            int64_t pcode_pass       = 0;
            int64_t an_pass          = 0;
            int64_t pi_pass          = 0;
        };
        std::vector<LocalAcc> local(n_threads);

        pool.parallel_for([&](int tid, int n_t) {
            // Compute this thread's row range
            const int32_t chunk = (total_rows + n_t - 1) / n_t;
            const int32_t beg   = tid * chunk;
            const int32_t end   = std::min(beg + chunk, total_rows);

            LocalAcc& acc = local[tid];

            for (int32_t nrow = beg; nrow < end; ++nrow) {

                // gender filter
                if (!gender_set.count(nm.gender[nrow])) continue;
                ++acc.gender_pass;

                // name_pcode_nf filter
                if (!pcode_set.count(nm.name_pcode_nf[nrow])) continue;
                ++acc.pcode_pass;

                int32_t pid = nm.id[nrow];

                // aka_name existence check
                auto [an_beg2, an_end2] = an.person_id_csr.range(pid);
                int32_t an_count = an_end2 - an_beg2;
                if (an_count == 0) continue;
                ++acc.an_pass;

                // Count person_info rows with matching info_type_id
                int64_t pi_count = 0;
                {
                    auto [pi_beg2, pi_end2] = pi1.person_id_csr.range(pid);
                    for (int32_t pi = pi_beg2; pi < pi_end2; ++pi) {
                        int32_t row = pi1.person_id_csr.values[pi];
                        if (valid_info_type_ids.count(pi1.info_type_id[row]))
                            ++pi_count;
                    }
                }
                if (pi_count == 0) continue;
                ++acc.pi_pass;

                // Count matching cast_info rows (role_id + note filters)
                int64_t ci_count = 0;
                {
                    auto [ci_beg2, ci_end2] = ci.person_id_csr.range(pid);
                    for (int32_t ci_i = ci_beg2; ci_i < ci_end2; ++ci_i) {
                        int32_t row = ci.person_id_csr.values[ci_i];
                        ++acc.ci_rows_probed;
                        if (!valid_role_ids.count(ci.role_id[row])) continue;
                        if (!note_set.count(ci.note[row])) continue;
                        ++ci_count;
                        ++acc.ci_rows_matched;
                    }
                }
                if (ci_count == 0) continue;

                acc.count += ci_count * pi_count * (int64_t)an_count;
            }
        });

        // Merge per-thread accumulators
        for (int t = 0; t < n_threads; ++t) {
            count             += local[t].count;
            ci_rows_probed    += local[t].ci_rows_probed;
            ci_rows_matched   += local[t].ci_rows_matched;
            persons_gender_pass += local[t].gender_pass;
            persons_pcode_pass  += local[t].pcode_pass;
            persons_an_pass     += local[t].an_pass;
            persons_pi_pass     += local[t].pi_pass;
        }
    } // end PROFILE_SCOPE("q4a_name_scan")

    TRACE_COUNT("q4a_persons_gender_pass", persons_gender_pass);
    TRACE_COUNT("q4a_persons_pcode_pass",  persons_pcode_pass);
    TRACE_COUNT("q4a_persons_an_pass",     persons_an_pass);
    TRACE_COUNT("q4a_persons_pi_pass",     persons_pi_pass);
    TRACE_COUNT("q4a_ci_rows_probed",      ci_rows_probed);
    TRACE_COUNT("q4a_ci_rows_matched",     ci_rows_matched);
    TRACE_COUNT("q4a_query_output_rows",   1);

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}
