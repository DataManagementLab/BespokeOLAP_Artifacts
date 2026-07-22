#include "query11b.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
#include <atomic>
static ThreadPool& pool = get_query_pool();
#include <algorithm>
#include <cstdint>
#include <cctype>
#include <cstring>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ---------------------------------------------------------------------------
// Fast case-insensitive helpers
// ---------------------------------------------------------------------------
static const uint8_t lc_tab[256] = {
    0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,
    32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,
    60,61,62,63,64,
    97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,
    91,92,93,94,95,96,
    97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,
    123,124,125,126,127,
    128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,
    144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,
    160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,
    176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,
    192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,
    208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,
    224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,
    240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255
};

static inline bool ci_contains(const char* text, size_t tlen,
                                const char* needle, size_t nlen) {
    if (!nlen) return true;
    if (tlen < nlen) return false;
    const uint8_t fn_lo = (uint8_t)needle[0];
    const uint8_t fn_up = (fn_lo >= 'a' && fn_lo <= 'z') ? (uint8_t)(fn_lo - 32) : fn_lo;
    const char* end = text + tlen - nlen;
    if (nlen == 1) {
        for (const char* p = text; p <= end; ++p) {
            const uint8_t ch = (uint8_t)*p;
            if (ch == fn_lo || ch == fn_up) return true;
        }
        return false;
    }
    for (const char* p = text; p <= end; ++p) {
        const uint8_t ch = (uint8_t)*p;
        if (ch != fn_lo && ch != fn_up) continue;
        bool ok = true;
        for (size_t i = 1; i < nlen; ++i)
            if (lc_tab[(uint8_t)p[i]] != (uint8_t)needle[i]) { ok = false; break; }
        if (ok) return true;
    }
    return false;
}

static bool ilike_lc(const char* tp, size_t tl, const char* pp, size_t pl) {
    bool all_pct = true;
    for (size_t i = 0; i < pl; ++i) if (pp[i] != '%') { all_pct = false; break; }
    if (all_pct) return true;
    const char *te = tp + tl, *pe = pp + pl;
    bool lw = false;
    while (pp <= pe) {
        const char *ss = pp, *pc = pp;
        while (pc < pe && *pc != '%') ++pc;
        size_t sl = (size_t)(pc - ss);
        if (!sl) {
            if (pc < pe) { lw = true; pp = pc + 1; continue; }
            return lw || tp == te;
        }
        if (!lw) {
            if ((size_t)(te-tp) < sl || std::memcmp(tp, ss, sl)) return false;
            tp += sl;
        } else {
            bool f = false;
            while ((size_t)(te-tp) >= sl) {
                if (!std::memcmp(tp, ss, sl)) { tp += sl; f = true; break; }
                ++tp;
            }
            if (!f) return false;
        }
        pp = pc;
        if (pp < pe && *pp == '%') { lw = true; ++pp; }
        else { lw = false; if (pp == pe) return tp == te; }
    }
    return lw || tp == te;
}

static bool ilike_raw(const std::string& text, const char* pp, size_t pl) {
    const size_t tl = text.size();
    constexpr size_t BUF = 512;
    char sb[BUF]; std::vector<char> hb;
    char* buf = (tl < BUF) ? sb : (hb.resize(tl), hb.data());
    for (size_t i = 0; i < tl; ++i)
        buf[i] = (char)std::tolower((unsigned char)text[i]);
    return ilike_lc(buf, tl, pp, pl);
}

static bool parse_contains_pattern(const std::string& pat, std::string& needle) {
    if (pat.size() < 2 || pat.front() != '%' || pat.back() != '%') return false;
    for (size_t i = 1; i + 1 < pat.size(); ++i)
        if (pat[i] == '%') return false;
    needle = pat.substr(1, pat.size() - 2);
    return true;
}

// ---------------------------------------------------------------------------
// Aggregation hash table
// ---------------------------------------------------------------------------
struct AggEntry {
    uint64_t key; int64_t count;
    uint8_t gender_byte; uint8_t _pad[3];
    uint32_t role_id, name_intern_id;
};

struct AggHashTable {
    std::vector<AggEntry> slots;
    size_t capacity, mask, size_;
    bool zero_key_used;
    AggEntry zero_entry;

    explicit AggHashTable(size_t cap = 65536) {
        capacity = 1;
        while (capacity < cap * 2) capacity <<= 1;
        mask = capacity-1; size_ = 0; zero_key_used = false;
        AggEntry e{0,0,0,{0,0,0},0,0}; slots.assign(capacity, e); zero_entry = e;
    }

    static inline uint64_t hk(uint64_t k) noexcept {
        k ^= k>>33; k *= 0xff51afd7ed558ccdULL;
        k ^= k>>33; k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k>>33; return k;
    }

    inline int64_t& get_or_insert(uint64_t key, uint8_t g, uint32_t rid, uint32_t nid) {
        if (__builtin_expect(!key, 0)) {
            if (!zero_key_used) { zero_key_used=true; zero_entry={0,0,g,{0,0,0},rid,nid}; ++size_; }
            return zero_entry.count;
        }
        size_t idx = (size_t)(hk(key) & mask);
        for (;;) {
            AggEntry& e = slots[idx];
            if (e.key == key) return e.count;
            if (!e.key) {
                e = {key,0,g,{0,0,0},rid,nid}; ++size_;
                if (__builtin_expect(size_*2 > capacity, 0)) grow();
                return e.count;
            }
            idx = (idx+1) & mask;
        }
    }

    void grow() {
        size_t nc = capacity*2, nm = nc-1;
        AggEntry e{0,0,0,{0,0,0},0,0};
        std::vector<AggEntry> ns(nc, e);
        for (auto& x : slots) {
            if (!x.key) continue;
            size_t i = (size_t)(hk(x.key) & nm);
            while (ns[i].key) i=(i+1)&nm;
            ns[i] = x;
        }
        slots = std::move(ns); capacity = nc; mask = nm;
    }

    void for_each(std::function<void(const AggEntry&)> fn) const {
        if (zero_key_used) fn(zero_entry);
        for (const auto& e : slots) if (e.key) fn(e);
    }
};

static inline uint64_t make_agg_key(uint8_t g, uint32_t rid, uint32_t nid) {
    return ((uint64_t)g<<48)|((uint64_t)(rid&0xFFFFu)<<32)|(uint64_t)nid;
}
static inline int32_t pack_pi_gbyte(int32_t cnt, uint8_t g) { return (cnt<<8)|(int32_t)g; }
static inline int32_t unpack_pi_count(int32_t v) { return v>>8; }
static inline uint8_t unpack_gbyte(int32_t v)    { return (uint8_t)(v&0xFF); }

// ---------------------------------------------------------------------------
// Uninitialized allocator: skips zero-fill on resize() so we can do it
// ourselves in parallel. ONLY safe when caller explicitly zeroes new regions.
// ---------------------------------------------------------------------------
template<typename T>
struct UninitAlloc : public std::allocator<T> {
    template<typename U> struct rebind { using other = UninitAlloc<U>; };
    UninitAlloc() = default;
    template<typename U> UninitAlloc(const UninitAlloc<U>&) {}
    // Don't default-construct: skip zero-fill
    void construct(T*) noexcept {}
    template<typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        ::new(static_cast<void*>(p)) U(std::forward<Args>(args)...);
    }
};

template<typename T>
using UninitVec = std::vector<T, UninitAlloc<T>>;

// ---------------------------------------------------------------------------
// Persistent scratch buffers: avoid large alloc+memset per call.
// Queries are sequential — statics are safe.
// ---------------------------------------------------------------------------
struct Q11bScratch {
    UninitVec<uint8_t>  movie_ok;    // [max_title_id+2]  idempotent byte writes
    UninitVec<int32_t>  mi1;         // [max_title_id+2]
    UninitVec<uint64_t> mi1_has;     // bitset for mi1 non-zero mids
    UninitVec<uint64_t> ok_has;      // bitset for ok[] non-zero mids (Phase 2 collect)
    UninitVec<uint8_t>  pi_cnt;      // [max_person_id+2] byte counts (selective cleanup)
    UninitVec<uint64_t> pi_has;      // bitset for persons with pi hit
    UninitVec<int32_t>  pi_gbyte;    // [max_person_id+2]
    // per-thread local vectors for parallel ok_mids collection
    std::vector<std::vector<int32_t>> tl_ok_mids;
    // Persistent company name lookup arrays (avoids page faults on each query)
    std::vector<const std::string*>   cn_ptr_arr;   // [max_company_id+2]
    std::vector<int32_t>              cn_intern_arr; // [max_company_id+2]

    bool initialized = false;

    // Called on each query run. First run: allocates + parallel zero-fills (page faults distributed).
    // Subsequent runs: resizes only if needed (usually no-op).
    void ensure(int32_t max_mid, int32_t max_pid, int32_t max_cid, int n_threads, ThreadPool& tpool) {
        const size_t nt  = (size_t)max_mid + 2;
        const size_t ntb = (nt + 63) / 64;
        const size_t np  = (size_t)max_pid + 2;
        const size_t npb = (np + 63) / 64;
        const size_t nc  = (size_t)max_cid + 2;

        // Check if we need to grow any array
        bool need_grow = (movie_ok.size() < nt || mi1.size() < nt || mi1_has.size() < ntb
                       || pi_cnt.size() < np  || pi_has.size() < npb || pi_gbyte.size() < np
                       || cn_ptr_arr.size() < nc || cn_intern_arr.size() < nc);
        if (ok_has.size() < ntb)    { ok_has.resize(ntb); need_grow = true; }
        if ((int)tl_ok_mids.size() < n_threads) tl_ok_mids.resize(n_threads);
        // Pre-reserve thread-local ok_mids vectors to avoid per-thread malloc contention
        // during parallel scan. Reserve enough for ~max_title_id / n_threads entries.
        // Use a fixed reasonable cap: ~20K entries per thread is plenty for ok_mids
        // (766K total / 96 threads = 8K avg; 20K = 2.5x safety margin).
        constexpr size_t reserve_per_thread = 20480; // 80KB per thread
        for (int t = 0; t < n_threads; ++t)
            if (tl_ok_mids[t].capacity() < reserve_per_thread)
                tl_ok_mids[t].reserve(reserve_per_thread);

        if (!need_grow && initialized) return;

        // Record old sizes BEFORE resize (we only need to zero new region)
        const size_t old_nt  = movie_ok.size();
        const size_t old_ntb = mi1_has.size();
        const size_t old_np  = pi_cnt.size();
        const size_t old_npb = pi_has.size();

        // Resize WITHOUT zero-fill (UninitAlloc skips construction)
        if (movie_ok.size() < nt)  movie_ok.resize(nt);
        if (mi1.size()     < nt)   mi1.resize(nt);
        if (mi1_has.size() < ntb)  mi1_has.resize(ntb);
        if (pi_cnt.size()  < np)   pi_cnt.resize(np);
        if (pi_has.size()  < npb)  pi_has.resize(npb);
        if (pi_gbyte.size()< np)   pi_gbyte.resize(np);

        // ok_has is already resized above
        // Company lookup arrays - need value initialization (not UninitAlloc)
        // because they're accessed by index and must be NULL/-1 for absent entries.
        if (cn_ptr_arr.size() < nc)   cn_ptr_arr.assign(nc, nullptr);
        if (cn_intern_arr.size() < nc) cn_intern_arr.assign(nc, -1);

        // Parallel zero-fill of newly allocated regions.
        // UninitAlloc means elements are uninitialized - we MUST zero them.
        // Zeroing in parallel distributes page faults across all CPU cores.
        auto par_zero_u8  = [&](UninitVec<uint8_t>& v, size_t from) {
            const size_t sz = v.size();
            if (from >= sz) return;
            const size_t chunk = (sz + n_threads - 1) / n_threads;
            tpool.parallel_for([&](int tid, int) {
                const size_t lo = std::max(from, (size_t)tid * chunk);
                const size_t hi = std::min((size_t)(tid+1) * chunk, sz);
                if (lo < hi) std::memset(v.data() + lo, 0, hi - lo);
            });
        };
        auto par_zero_u64 = [&](UninitVec<uint64_t>& v, size_t from) {
            const size_t sz = v.size();
            if (from >= sz) return;
            const size_t chunk = (sz + n_threads - 1) / n_threads;
            tpool.parallel_for([&](int tid, int) {
                const size_t lo = std::max(from, (size_t)tid * chunk);
                const size_t hi = std::min((size_t)(tid+1) * chunk, sz);
                if (lo < hi) std::memset(v.data() + lo, 0, (hi-lo)*8);
            });
        };
        auto par_zero_i32 = [&](UninitVec<int32_t>& v, size_t from) {
            const size_t sz = v.size();
            if (from >= sz) return;
            const size_t chunk = (sz + n_threads - 1) / n_threads;
            tpool.parallel_for([&](int tid, int) {
                const size_t lo = std::max(from, (size_t)tid * chunk);
                const size_t hi = std::min((size_t)(tid+1) * chunk, sz);
                if (lo < hi) std::memset(v.data() + lo, 0, (hi-lo)*4);
            });
        };

        // Zero only newly-allocated regions (from old_X to new size)
        par_zero_u8 (movie_ok,  old_nt);
        par_zero_i32(mi1,       old_nt);
        par_zero_u64(mi1_has,   old_ntb);
        par_zero_u8 (pi_cnt,    old_np);
        par_zero_u64(pi_has,    old_npb);
        par_zero_i32(pi_gbyte,  old_np);
        par_zero_u64(ok_has,    old_ntb);  // ok_has shares same bitset size as mi1_has

        initialized = true;
    }
};
static Q11bScratch g_scr;

std::vector<std::vector<std::string>> run_q11b(Database* db, const Q11bArgs& args) {
    if (!db) throw std::runtime_error("run_q11b: db is null");
    PROFILE_SCOPE("q11b_total");

    auto is_null  = [](const std::string& s) { return s=="<<NULL>>"||s=="NULL"; };
    auto strip_sq = [](const std::string& s) -> std::string {
        return (s.size()>=2&&s.front()=='\''&&s.back()=='\'') ? s.substr(1,s.size()-2) : s;
    };
    auto to_lower = [](const std::string& s) -> std::string {
        std::string r; r.reserve(s.size());
        for (unsigned char c : s) r += (char)std::tolower(c); return r;
    };

    const std::string lc_p1 = to_lower(strip_sq(args.INFO1));
    const std::string lc_p2 = to_lower(strip_sq(args.INFO2));
    const int32_t year1 = std::stoi(args.YEAR1), year2 = std::stoi(args.YEAR2);

    std::string needle1, needle2;
    const bool ic1 = parse_contains_pattern(lc_p1, needle1);
    const bool ic2 = parse_contains_pattern(lc_p2, needle2);
    const char* nd1 = needle1.data(); const size_t ns1 = needle1.size();
    const char* nd2 = needle2.data(); const size_t ns2 = needle2.size();
    const char* p1d = lc_p1.c_str(); const size_t p1s = lc_p1.size();
    const char* p2d = lc_p2.c_str(); const size_t p2s = lc_p2.size();

    std::unordered_set<int32_t> valid_kind_ids, valid_role_ids, valid_it1_ids, valid_it2_ids;
    {
        std::unordered_set<std::string> ks;
        for (const auto& s : args.KIND) if (!is_null(s)) ks.insert(s);
        for (size_t i = 0; i < db->kind_type.id.size(); ++i)
            if (ks.count(db->kind_type.kind[i])) valid_kind_ids.insert(db->kind_type.id[i]);
    }
    TRACE_COUNT("q11b_valid_kind_ids", (int64_t)valid_kind_ids.size());
    {
        std::unordered_set<std::string> rs;
        for (const auto& s : args.ROLE) if (!is_null(s)) rs.insert(s);
        for (size_t i = 0; i < db->role_type.id.size(); ++i)
            if (rs.count(db->role_type.role[i])) valid_role_ids.insert(db->role_type.id[i]);
    }
    TRACE_COUNT("q11b_valid_role_ids", (int64_t)valid_role_ids.size());
    for (const auto& s : args.ID1) if (!is_null(s)) valid_it1_ids.insert(std::stoi(s));
    TRACE_COUNT("q11b_valid_it1_ids", (int64_t)valid_it1_ids.size());
    for (const auto& s : args.ID2) if (!is_null(s)) valid_it2_ids.insert(std::stoi(s));
    TRACE_COUNT("q11b_valid_it2_ids", (int64_t)valid_it2_ids.size());

    if (valid_kind_ids.empty()||valid_role_ids.empty()||valid_it1_ids.empty()||valid_it2_ids.empty())
        return {{"gender","role","name","count_star()"}};

    const int32_t max_title_id  = (int32_t)db->title.id_to_row.size() - 1;
    const int32_t max_person_id = (int32_t)db->name.id_to_row.size() - 1;
    const int n_threads = pool.num_threads;

    {
        PROFILE_SCOPE("q11b_ensure");
        const int32_t max_cid_tmp = (int32_t)db->company_name.id_to_row.size() - 1;
        g_scr.ensure(max_title_id, max_person_id, max_cid_tmp, n_threads, pool);
    }

    const size_t pi_bsw  = g_scr.pi_has.size();
    const size_t mi1_bsw = g_scr.mi1_has.size();
    uint8_t*  ok      = g_scr.movie_ok.data();
    int32_t*  mi1p    = g_scr.mi1.data();
    uint64_t* mi1_has = g_scr.mi1_has.data();
    uint8_t*  pcnt    = g_scr.pi_cnt.data();    // byte per person
    uint64_t* phas    = g_scr.pi_has.data();
    int32_t*  pgby    = g_scr.pi_gbyte.data();
    uint64_t* ok_has  = g_scr.ok_has.data();    // bitset for valid mids

    constexpr int32_t MORSEL = 65536;

    auto build_morsels = [](const std::vector<std::pair<int32_t,int32_t>>& rngs)
        -> std::vector<std::pair<int32_t,int32_t>> {
        std::vector<std::pair<int32_t,int32_t>> out; out.reserve(rngs.size()*4);
        for (auto& p : rngs) for (int32_t lo=p.first; lo<p.second; lo+=MORSEL)
            out.push_back({lo, std::min(p.second, lo+MORSEL)});
        return out;
    };

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // 5. Title scan → movie_ok (parallel morsel-driven).
    //    Phase 1: parallel idempotent byte writes to shared ok[] (no atomics needed).
    //    Phase 2: parallel collection of ok_mids via per-thread tl_ok_mids vectors.
    // -----------------------------------------------------------------------
    std::vector<int32_t> ok_mids;
    ok_mids.reserve(800000);
    int64_t title_rows_scanned = 0, title_rows_emitted = 0;
    {
        PROFILE_SCOPE("q11b_title_scan");
        const auto& t = db->title;
        const int32_t mkp = (int32_t)t.kind_part_start.size() - 1;

        std::vector<std::pair<int32_t,int32_t>> rngs;
        for (int32_t kid : valid_kind_ids) {
            if (kid<0||kid>mkp) continue;
            if (t.kind_part_start[kid] < t.kind_part_end[kid])
                rngs.push_back({t.kind_part_start[kid], t.kind_part_end[kid]});
        }
        auto morsels = build_morsels(rngs);
        const int32_t nm = (int32_t)morsels.size();
        std::atomic<int32_t> cur{0};
        std::vector<int64_t> tl_sc(n_threads,0), tl_em(n_threads,0);
        const int32_t* t_id = t.id.data(), *t_yr = t.production_year.data();

        // Phase 1: parallel write to ok[] (idempotent byte write - no atomics)
        pool.parallel_for([&](int tid, int) {
            int64_t sc=0, em=0;
            while (true) {
                int32_t mi = cur.fetch_add(1, std::memory_order_relaxed);
                if (mi >= nm) break;
                for (int32_t r=morsels[mi].first, re=morsels[mi].second; r<re; ++r) {
                    ++sc;
                    const int32_t yr = t_yr[r];
                    if (yr==-1||yr<year2||yr>year1) continue;
                    const int32_t mid = t_id[r];
                    if (mid<0||mid>max_title_id) continue;
                    ok[mid] = 1; ++em;
                }
            }
            tl_sc[tid]=sc; tl_em[tid]=em;
        });

        for (int ti=0;ti<n_threads;++ti) { title_rows_scanned+=tl_sc[ti]; title_rows_emitted+=tl_em[ti]; }
    }

    // Phase 2: parallel collection of ok_mids via per-thread tl_ok_mids vectors
    {
        PROFILE_SCOPE("q11b_title_collect");
        const int32_t total = max_title_id + 1;
        const int32_t chunk = std::max<int32_t>(64, ((total + n_threads - 1) / n_threads + 63) & ~63);
        pool.parallel_for([&](int tid, int) {
            const int32_t lo = (int32_t)tid * chunk;
            if (lo > max_title_id) { g_scr.tl_ok_mids[tid].clear(); return; }
            const int32_t hi = std::min(lo + chunk, total);
            auto& lv = g_scr.tl_ok_mids[tid];
            lv.clear();
            for (int32_t mid = lo; mid < hi; ++mid)
                if (ok[mid]) lv.push_back(mid);
        });
        // Merge in order (each thread's sublist is sorted; concat preserves order)
        for (int t=0; t<n_threads; ++t) {
            const auto& lv = g_scr.tl_ok_mids[t];
            ok_mids.insert(ok_mids.end(), lv.begin(), lv.end());
        }
    }
    TRACE_COUNT("q11b_title_rows_scanned", title_rows_scanned);
    TRACE_COUNT("q11b_title_rows_emitted", title_rows_emitted);
    TRACE_COUNT("q11b_valid_movie_ids",    (int64_t)ok_mids.size());


    // -----------------------------------------------------------------------
    // 6. mi1 build (parallel morsel-driven, atomic fetch_add on int32).
    //    Collect mi1_mids via mi1_has bitset (fast, ~780KB scan).
    // -----------------------------------------------------------------------
    std::vector<int32_t> mi1_mids;
    mi1_mids.reserve(4096);
    {
        PROFILE_SCOPE("q11b_mi1_build");
        const auto& mi = db->movie_info;
        const int32_t mxt = (int32_t)mi.type_part_start.size() - 1;

        std::vector<std::pair<int32_t,int32_t>> rngs;
        for (int32_t it : valid_it1_ids) {
            if (it<0||it>mxt) continue;
            if (mi.type_part_start[it] < mi.type_part_end[it])
                rngs.push_back({mi.type_part_start[it], mi.type_part_end[it]});
        }
        auto morsels = build_morsels(rngs);
        const int32_t nm = (int32_t)morsels.size();
        std::atomic<int32_t> cur{0};
        std::vector<int64_t> tl_sc(n_threads,0), tl_em(n_threads,0);

        static_assert(sizeof(std::atomic<int32_t>)  == sizeof(int32_t),  "");
        static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t), "");
        std::atomic<int32_t>*  mi1a      = reinterpret_cast<std::atomic<int32_t>*>(mi1p);
        std::atomic<uint64_t>* mi1_has_a = reinterpret_cast<std::atomic<uint64_t>*>(mi1_has);
        const int32_t* mi_mid = mi.movie_id.data();

        pool.parallel_for([&](int tid, int) {
            int64_t sc=0, em=0;
            while (true) {
                int32_t idx = cur.fetch_add(1, std::memory_order_relaxed);
                if (idx >= nm) break;
                for (int32_t r=morsels[idx].first, re=morsels[idx].second; r<re; ++r) {
                    ++sc;
                    const int32_t mid = mi_mid[r];
                    if (mid<0||mid>max_title_id||!ok[mid]) continue;
                    const std::string& info = mi.info_str[r];
                    bool match = ic1
                        ? ci_contains(info.data(), info.size(), nd1, ns1)
                        : ilike_raw(info, p1d, p1s);
                    if (!match) continue;
                    mi1a[mid].fetch_add(1, std::memory_order_relaxed);
                    // Mark in bitset for fast collection
                    mi1_has_a[(size_t)mid>>6].fetch_or(
                        (uint64_t)1<<((size_t)mid&63), std::memory_order_relaxed);
                    ++em;
                }
            }
            tl_sc[tid]=sc; tl_em[tid]=em;
        });

        int64_t rs=0, re=0;
        for (int ti=0;ti<n_threads;++ti){rs+=tl_sc[ti]; re+=tl_em[ti];}
        TRACE_COUNT("q11b_mi1_rows_scanned", rs);
        TRACE_COUNT("q11b_mi1_rows_emitted", re);

        // Collect mi1_mids via bitset (~780KB, cache-warm after scan)
        for (size_t w = 0; w < mi1_bsw; ++w) {
            uint64_t word = mi1_has[w]; if (!word) continue;
            while (word) {
                int b = __builtin_ctzll(word); word &= word-1;
                const int32_t mid = (int32_t)(w*64+(size_t)b);
                if (mid <= max_title_id) mi1_mids.push_back(mid);
            }
        }

        TRACE_COUNT("q11b_mi1_movie_groups", (int64_t)mi1_mids.size());
    }

    // -----------------------------------------------------------------------
    // 7. Company lookup
    // -----------------------------------------------------------------------
    const int32_t max_company_id = (int32_t)db->company_name.id_to_row.size() - 1;
    // Use persistent arrays from g_scr (already sized in ensure())
    // Reset only the entries we'll fill (avoids 14MB re-zero each query).
    const std::string** cn_ptr    = g_scr.cn_ptr_arr.data();
    int32_t*            cn_intern = g_scr.cn_intern_arr.data();
    {
        PROFILE_SCOPE("q11b_cn_build");
        for (size_t i = 0; i < db->company_name.id.size(); ++i) {
            const int32_t cid = db->company_name.id[i];
            if (cid<0||cid>max_company_id) continue;
            cn_ptr[cid]    = &db->company_name.name_str[i];
            cn_intern[cid] = (int32_t)i;
        }
    }

    // -----------------------------------------------------------------------
    // 7b. mc_build: iterate only mi1_mids (~3.5K).
    // -----------------------------------------------------------------------
    std::vector<int32_t> mc_movie_ids, mc_cid_start, mc_cid_end_v;
    mc_movie_ids.reserve(4096); mc_cid_start.reserve(4096); mc_cid_end_v.reserve(4096);
    std::vector<std::pair<int32_t,int32_t>> mc_flat; mc_flat.reserve(16384);
    {
        PROFILE_SCOPE("q11b_mc_build");
        int64_t rs=0, re=0;
        const auto& mc = db->movie_companies;
        const int32_t mc_max = (int32_t)mc.movie_id_csr.offsets.size() - 2;
        std::vector<std::pair<int32_t,int32_t>> scratch; scratch.reserve(16);
        const int32_t* mc_off = mc.movie_id_csr.offsets.data();
        const int32_t* mc_cid = mc.company_id.data();
        const int32_t* mc_cty = mc.company_type_id.data();

        for (int32_t mid : mi1_mids) {
            if (mid > mc_max) continue;
            const int32_t beg = mc_off[mid], end = mc_off[mid+1];
            if (beg == end) continue;
            scratch.clear();
            for (int32_t r = beg; r < end; ++r) {
                ++rs;
                if (mc_cty[r]==-1) continue;
                const int32_t cid = mc_cid[r];
                if (cid<0||cid>max_company_id) continue;
                const int32_t nid = cn_intern[cid];
                if (nid<0) continue;
                bool found = false;
                for (auto& p : scratch) if (p.first==nid){++p.second;found=true;break;}
                if (!found) scratch.push_back({nid,1});
                ++re;
            }
            if (!scratch.empty()) {
                mc_movie_ids.push_back(mid);
                mc_cid_start.push_back((int32_t)mc_flat.size());
                for (const auto& p : scratch) mc_flat.push_back(p);
                mc_cid_end_v.push_back((int32_t)mc_flat.size());
            }
        }
        TRACE_COUNT("q11b_mc_rows_scanned", rs);
        TRACE_COUNT("q11b_mc_rows_emitted", re);
        TRACE_COUNT("q11b_mc_movie_groups", (int64_t)mc_movie_ids.size());
    }

    // -----------------------------------------------------------------------
    // 8. pi_build: parallel morsel-driven, thread-local int32 counts.
    //    Using int32 per person avoids byte-level false sharing.
    //    The shared phas bitset uses atomic fetch_or (64-bit, minimal contention).
    // -----------------------------------------------------------------------
    {
        PROFILE_SCOPE("q11b_pi_build");
        const auto& pi = db->person_info;
        const int32_t mxt = (int32_t)pi.type_part_start.size() - 1;

        std::vector<std::pair<int32_t,int32_t>> rngs;
        for (int32_t it : valid_it2_ids) {
            if (it<0||it>mxt) continue;
            if (pi.type_part_start[it] < pi.type_part_end[it])
                rngs.push_back({pi.type_part_start[it], pi.type_part_end[it]});
        }

        // Use smaller morsels for pi_build to maximize parallelism.
        // With 672K rows / MORSEL=65536, we get only 11 morsels → only 11 threads work!
        // Use MORSEL_PI=4096 → ~164 morsels → all 96 threads can work.
        constexpr int32_t MORSEL_PI = 4096;
        std::vector<std::pair<int32_t,int32_t>> morsels;
        morsels.reserve(rngs.size() * 200);
        for (auto& [rb, re] : rngs)
            for (int32_t lo = rb; lo < re; lo += MORSEL_PI)
                morsels.push_back({lo, std::min(re, lo + MORSEL_PI)});
        const int32_t nm = (int32_t)morsels.size();
        std::atomic<int32_t> cur{0};

        static_assert(sizeof(std::atomic<int32_t>)  == sizeof(int32_t),  "");
        static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t), "");
        std::atomic<uint8_t>*  pcnt_a = reinterpret_cast<std::atomic<uint8_t>*>(pcnt);
        std::atomic<uint64_t>* phas_a = reinterpret_cast<std::atomic<uint64_t>*>(phas);

        std::vector<int64_t> tl_sc(n_threads,0), tl_em(n_threads,0);
        const int32_t* pid_arr = pi.person_id.data();

        pool.parallel_for([&](int tid, int) {
            int64_t sc=0, em=0;
            while (true) {
                int32_t idx = cur.fetch_add(1, std::memory_order_relaxed);
                if (idx >= nm) break;
                for (int32_t r=morsels[idx].first, re=morsels[idx].second; r<re; ++r) {
                    ++sc;
                    const int32_t pid = pid_arr[r];
                    if (pid<0||pid>max_person_id) continue;
                    const std::string& info = pi.info_str[r];
                    bool match = ic2
                        ? ci_contains(info.data(), info.size(), nd2, ns2)
                        : ilike_raw(info, p2d, p2s);
                    if (!match) continue;
                    // CAS loop on byte (saturating at 255)
                    uint8_t ov = pcnt_a[pid].load(std::memory_order_relaxed);
                    while (ov<255 && !pcnt_a[pid].compare_exchange_weak(
                               ov, (uint8_t)(ov+1), std::memory_order_relaxed)) {}
                    // Mark in bitset - 1 atomic per 64 persons (low contention)
                    phas_a[(size_t)pid>>6].fetch_or(
                        (uint64_t)1<<((size_t)pid&63), std::memory_order_relaxed);
                    ++em;
                }
            }
            tl_sc[tid]=sc; tl_em[tid]=em;
        });

        int64_t rs=0, re=0;
        for (int ti=0;ti<n_threads;++ti){rs+=tl_sc[ti]; re+=tl_em[ti];}
        TRACE_COUNT("q11b_pi_rows_scanned", rs);
        TRACE_COUNT("q11b_pi_rows_emitted", re);

        // pi_gbyte_merge: parallel over bitset words
        // We have ~pi_bsw words to scan. Each set bit requires a lookup.
        // Parallelize by splitting bitset words across threads.
        {
            PROFILE_SCOPE("q11b_pi_gbyte_merge");
            const auto& nm_tbl = db->name;
            const int32_t* id2r = nm_tbl.id_to_row.data();
            const uint8_t* gbs  = nm_tbl.gender_byte.data();
            const int32_t  nmx  = (int32_t)nm_tbl.id_to_row.size()-1;

            const size_t bsw_per_thread = (pi_bsw + n_threads - 1) / n_threads;
            pool.parallel_for([&](int tid, int) {
                const size_t w_lo = (size_t)tid * bsw_per_thread;
                const size_t w_hi = std::min(w_lo + bsw_per_thread, pi_bsw);
                for (size_t w = w_lo; w < w_hi; ++w) {
                    uint64_t word = phas[w]; if (!word) continue;
                    while (word) {
                        int b=__builtin_ctzll(word); word &= word-1;
                        int32_t pid=(int32_t)(w*64+(size_t)b);
                        if (pid>max_person_id) break;
                        uint8_t gb=0;
                        if (pid<=nmx){int32_t nr=id2r[pid]; if(nr>=0) gb=gbs[nr];}
                        pgby[pid] = pack_pi_gbyte(pcnt[pid], gb);
                    }
                }
            });
        }

        int64_t nz=0;
        for (size_t w=0;w<pi_bsw;++w) nz+=__builtin_popcountll(phas[w]);
        TRACE_COUNT("q11b_pi_person_groups", nz);
    }

    // -----------------------------------------------------------------------
    // 9. Role lookups
    // -----------------------------------------------------------------------
    const int32_t max_role_id = [&]() {
        int32_t mx=0;
        for (int32_t r : valid_role_ids) mx=std::max(mx,r);
        for (size_t i=0; i<db->role_type.id.size(); ++i) mx=std::max(mx,db->role_type.id[i]);
        return mx;
    }();
    std::vector<const std::string*> role_str(max_role_id+2, nullptr);
    std::vector<uint8_t> role_ok(max_role_id+2, 0);
    for (size_t i=0; i<db->role_type.id.size(); ++i) {
        int32_t rid = db->role_type.id[i];
        if (rid>=0&&rid<=max_role_id&&valid_role_ids.count(rid)){
            role_ok[rid]=1; role_str[rid]=&db->role_type.role[i];
        }
    }
    TRACE_COUNT("q11b_role_max_id", (int64_t)max_role_id);

    // -----------------------------------------------------------------------
    // 9c. Main join
    // -----------------------------------------------------------------------
    AggHashTable counts(std::max((size_t)16384, mc_movie_ids.size()*4));
    {
        PROFILE_SCOPE("q11b_cast_info_join");
        int64_t ci_sc=0, ci_rp=0, mv=0, ci_pp=0, ci_psp=0;
        const auto& ci = db->cast_info;
        const int32_t* ci_rid=ci.role_id.data(), *ci_pid=ci.person_id.data();
        const int32_t* ci_off=ci.movie_id_csr.offsets.data();
        const uint64_t* hp=phas;
        const uint8_t* rok=role_ok.data();
        const std::pair<int32_t,int32_t>* mcf=mc_flat.data();

        for (size_t mi=0; mi<mc_movie_ids.size(); ++mi) {
            const int32_t mid=mc_movie_ids[mi], mc1=mi1p[mid];
            const int32_t ms=mc_cid_start[mi], ml=mc_cid_end_v[mi]-ms;
            const std::pair<int32_t,int32_t>* mp=mcf+ms;
            ++mv;
            for (int32_t r=ci_off[mid], re=ci_off[mid+1]; r<re; ++r) {
                ++ci_sc;
                const int32_t pid=ci_pid[r];
                if (pid<0||pid>max_person_id) continue;
                if (!(hp[(size_t)pid>>6]&((uint64_t)1<<((size_t)pid&63)))) continue;
                ++ci_pp;
                const int32_t rid=ci_rid[r];
                if (rid<0||rid>max_role_id||!rok[rid]) continue;
                ++ci_rp;
                const int32_t pv=pgby[pid];
                const int32_t pc=unpack_pi_count(pv);
                const uint8_t gb=unpack_gbyte(pv);
                ++ci_psp;
                const int64_t contrib=(int64_t)mc1*pc;
                for (int32_t mc_i=0; mc_i<ml; ++mc_i) {
                    const uint64_t ak=make_agg_key(gb,(uint32_t)rid,(uint32_t)mp[mc_i].first);
                    counts.get_or_insert(ak,gb,(uint32_t)rid,(uint32_t)mp[mc_i].first)
                        += contrib*mp[mc_i].second;
                }
            }
        }
        TRACE_COUNT("q11b_ci_rows_scanned",     ci_sc);
        TRACE_COUNT("q11b_ci_rows_role_pass",   ci_rp);
        TRACE_COUNT("q11b_ci_movies_visited",   mv);
        TRACE_COUNT("q11b_ci_rows_pi_pass",     ci_pp);
        TRACE_COUNT("q11b_ci_rows_person_pass", ci_psp);
        TRACE_COUNT("q11b_output_groups",       (int64_t)counts.size_);
    }
    TRACE_COUNT("q11b_query_output_rows", (int64_t)counts.size_);

    // -----------------------------------------------------------------------
    // 10. Output
    // -----------------------------------------------------------------------
    std::vector<std::vector<std::string>> rows;
    rows.push_back({"gender","role","name","count_star()"});

    std::unordered_map<uint8_t,std::string> g2s;
    for (size_t i=0; i<db->name.gender_byte.size(); ++i) {
        uint8_t gb=db->name.gender_byte[i];
        if (!g2s.count(gb)) g2s[gb]=db->name.gender[i];
    }

    struct OutRow { int64_t count; std::string gender, role, company; };
    const auto& cn_names = db->company_name.name_str;
    const size_t cn_n = cn_names.size();
    std::unordered_map<std::string,int64_t> merged;
    merged.reserve(counts.size_);

    counts.for_each([&](const AggEntry& e) {
        auto git=g2s.find(e.gender_byte);
        const std::string& g=(git!=g2s.end())?git->second:"";
        const std::string* rs=(e.role_id<=(uint32_t)max_role_id)?role_str[e.role_id]:nullptr;
        const std::string* cn=(e.name_intern_id<(uint32_t)cn_n)?&cn_names[e.name_intern_id]:nullptr;
        if (!rs||!cn) return;
        merged[g+'\0'+*rs+'\0'+*cn] += e.count;
    });

    std::vector<OutRow> sorted; sorted.reserve(merged.size());
    for (const auto& [mk,cnt] : merged) {
        size_t p1=mk.find('\0'), p2=mk.find('\0',p1+1);
        sorted.push_back({cnt,mk.substr(0,p1),mk.substr(p1+1,p2-p1-1),mk.substr(p2+1)});
    }
    std::sort(sorted.begin(), sorted.end(),
              [](const OutRow& a, const OutRow& b){return a.count>b.count;});
    for (const OutRow& r : sorted)
        rows.push_back({r.gender,r.role,r.company,std::to_string(r.count)});
    TRACE_COUNT("q11b_output_rows_built", (int64_t)sorted.size());

    // -----------------------------------------------------------------------
    // Cleanup: selective zero — O(set entries), not O(array size)
    // -----------------------------------------------------------------------
    {
        PROFILE_SCOPE("q11b_cleanup");
        // Clear ok[] and mi1[] only for valid mids
        for (int32_t mid : ok_mids) { ok[mid]=0; mi1p[mid]=0; }
        // Clear mi1_has bitset
        for (size_t w=0; w<mi1_bsw; ++w) { if (mi1_has[w]) mi1_has[w]=0; }
        // Clear ok_has bitset (same size as mi1_has)
        const size_t ok_bsw2 = g_scr.ok_has.size();
        for (size_t w=0; w<ok_bsw2; ++w) { if (ok_has[w]) ok_has[w]=0; }
        // Clear pcnt[] and pgby[] for persons that had hits
        for (size_t w=0; w<pi_bsw; ++w) {
            uint64_t word = phas[w]; if (!word) continue;
            phas[w]=0;
            while (word) {
                int b=__builtin_ctzll(word); word &= word-1;
                int32_t pid=(int32_t)(w*64+(size_t)b);
                if (pid>max_person_id) break;
                pcnt[pid]=0; pgby[pid]=0;
            }
        }
    }

    return rows;
}
