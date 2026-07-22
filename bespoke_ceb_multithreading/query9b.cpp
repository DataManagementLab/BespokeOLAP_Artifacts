#include "query9b.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
#include <atomic>
#include <numeric>
#include <memory>
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cstdint>
#include <cctype>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Direct-mapped person-id cache for cast_info join.
// Replaces std::unordered_map for O(1) lookups with good cache behavior.
// Lossy (collisions cause eviction) but correct: misses trigger full re-check.
struct alignas(64) PidCache {
    static constexpr int SZ   = (1<<16); // 65536 slots = 512KB
    static constexpr int MASK = SZ - 1;
    int32_t tag[SZ]; // pid, or -1 for empty
    int32_t val[SZ]; // name_row (≥0 valid, -1 invalid/fail)
    void clear() { memset(tag, -1, sizeof(tag)); }
};

// SQL:
/** SELECT  mi1.info, n.name, COUNT(*)
FROM title t, kind_type kt, movie_info mi1, info_type it1,
     cast_info ci, role_type rt, name n, info_type it2, person_info pi
WHERE t.id = ci.movie_id AND t.id = mi1.movie_id
  AND mi1.info_type_id = it1.id AND t.kind_id = kt.id
  AND ci.person_id = n.id AND ci.movie_id = mi1.movie_id
  AND ci.role_id = rt.id AND n.id = pi.person_id
  AND pi.info_type_id = it2.id
  AND (it1.id IN ID1) AND (it2.id IN ID2) AND (mi1.info IN INFO)
  AND (n.name ILIKE NAME) AND (kt.kind IN KIND) AND (rt.role IN ROLE)
  AND (t.production_year <= YEAR1) AND (t.production_year >= YEAR2)
GROUP BY mi1.info, n.name */

std::vector<std::vector<std::string>> run_q9b(Database* db, const Q9bArgs& args) {
    if (!db) throw std::runtime_error("run_q9b: db is null");

    PROFILE_SCOPE("q9b_total");

    auto is_null=[](const std::string&s){return s=="<<NULL>>"||s=="NULL";};

    // ILIKE match (both lc)
    auto ilike_lc=[](const char*tp,size_t tl,const char*pp,size_t pl)->bool{
        const char*te=tp+tl,*pe=pp+pl;
        bool ap=true; for(size_t i=0;i<pl;++i)if(pp[i]!='%'){ap=false;break;} if(ap)return true;
        bool lw=false;
        while(pp<=pe){
            const char*ss=pp,*pc=pp; while(pc<pe&&*pc!='%')++pc;
            size_t sl=(size_t)(pc-ss);
            if(!sl){if(pc<pe){lw=true;pp=pc+1;continue;}return(lw||tp==te);}
            if(!lw){if((size_t)(te-tp)<sl)return false;if(memcmp(tp,ss,sl))return false;tp+=sl;}
            else{bool f=false;while((size_t)(te-tp)>=sl){if(!memcmp(tp,ss,sl)){tp+=sl;f=true;break;}++tp;}if(!f)return false;}
            pp=pc;
            if(pp<pe&&*pp=='%'){lw=true;++pp;}else{lw=false;if(pp==pe)return(tp==te);}
        }
        return(lw||tp==te);
    };

    auto strip_sq=[](const std::string&s)->std::string{
        if(s.size()>=2&&s.front()=='\''&&s.back()=='\'')return s.substr(1,s.size()-2);return s;};

    const std::string npat=[&]{
        std::string r=strip_sq(args.NAME);
        for(char&c:r)c=(char)tolower((unsigned char)c);return r;}();
    const char*pat=npat.c_str(); const size_t plen=npat.size();

    bool simc=false; std::string ndl;
    if(plen>=2&&pat[0]=='%'&&pat[plen-1]=='%'){
        std::string m(pat+1,plen-2);
        if(m.find('%')==std::string::npos){simc=true;ndl=m;}}
    const char*ncp=simc?ndl.c_str():nullptr; const size_t ncl=simc?ndl.size():0;

    auto nmatch=[&](const std::string&ns)->bool{
        if(simc){
            if(ns.size()<ncl)return false;
            for(size_t i=0;i+ncl<=ns.size();++i){
                bool ok=true;
                for(size_t j=0;j<ncl&&ok;++j) ok=((char)tolower((unsigned char)ns[i+j])==ncp[j]);
                if(ok)return true;}
            return false;}
        std::string lc(ns.size(),'\0');
        for(size_t i=0;i<ns.size();++i)lc[i]=(char)tolower((unsigned char)ns[i]);
        return ilike_lc(lc.c_str(),lc.size(),pat,plen);};

    int32_t year1=stoi(args.YEAR1),year2=stoi(args.YEAR2);

    // kind_ids
    std::vector<int32_t> valid_kinds;
    bool kid_neg = false;
    {std::unordered_set<std::string>ks;
     for(const auto&s:args.KIND){if(is_null(s))kid_neg=true;else ks.insert(s);}
     const auto&kt=db->kind_type;
     for(size_t i=0;i<kt.id.size();++i)if(ks.count(kt.kind[i]))valid_kinds.push_back(kt.id[i]);}
    std::sort(valid_kinds.begin(), valid_kinds.end());
    TRACE_COUNT("q9b_valid_kind_ids", (int64_t)valid_kinds.size()+(kid_neg?1:0));

    // role_ids: flat uint8_t array
    int32_t mxr=-1;
    {const auto&rt=db->role_type;for(size_t i=0;i<rt.id.size();++i)if(rt.id[i]>mxr)mxr=rt.id[i];}
    std::vector<uint8_t> rok(mxr+2,0); bool rn1=false;
    {std::unordered_set<std::string>rs;
     for(const auto&s:args.ROLE){if(is_null(s))rn1=true;else rs.insert(s);}
     const auto&rt=db->role_type;
     for(size_t i=0;i<rt.id.size();++i)
         if(rs.count(rt.role[i])&&rt.id[i]>=0&&rt.id[i]<=mxr)rok[(size_t)rt.id[i]]=1;}
    auto chkr=[&](int32_t r)->bool{if(r<0)return rn1;if(r>mxr)return false;return rok[(size_t)r]!=0;};
    {int64_t n=0;for(auto b:rok)if(b)++n;TRACE_COUNT("q9b_valid_role_ids",n);}

    std::vector<int32_t> it1l,it2l;
    for(const auto&s:args.ID1)if(!is_null(s))it1l.push_back(stoi(s));
    for(const auto&s:args.ID2)if(!is_null(s))it2l.push_back(stoi(s));
    TRACE_COUNT("q9b_valid_it1_ids", (int64_t)it1l.size());
    TRACE_COUNT("q9b_valid_it2_ids", (int64_t)it2l.size());

    // Build query intern IDs for the INFO strings
    std::unordered_map<std::string,int32_t> iintern;
    std::vector<std::string> istrs;
    {int32_t n=0;for(const auto&s:args.INFO)
        if(!is_null(s)&&!iintern.count(s)){iintern[s]=n++;istrs.push_back(s);}}
    TRACE_COUNT("q9b_info_intern_size", (int64_t)iintern.size());

    // Build translation: db intern ID -> query intern ID
    const auto& mi_dict_map = db->movie_info.info_dict_map;
    const int32_t db_dict_size = (int32_t)db->movie_info.info_dict_vec.size();
    std::vector<int16_t> db_to_qid(db_dict_size, -1);
    std::vector<std::pair<int32_t,int16_t>> matching_dbi;
    matching_dbi.reserve(16);
    for(const auto& [s, qid] : iintern){
        auto it = mi_dict_map.find(s);
        if(it != mi_dict_map.end()){
            const int32_t dbi = it->second;
            if(dbi>=0 && dbi<db_dict_size){
                db_to_qid[(size_t)dbi] = (int16_t)qid;
                matching_dbi.push_back({dbi, (int16_t)qid});
            }
        }
    }
    TRACE_COUNT("q9b_db_to_qid_built", (int64_t)iintern.size());

    // -----------------------------------------------------------------------
    // STEP 1: Build title bitmap via kind-partitioned title scan (parallel).
    // -----------------------------------------------------------------------
    int32_t mxm=(int32_t)db->title.id_to_row.size()-1;
    const int32_t bm_sz = mxm + 2;
    std::vector<uint8_t> title_bitmap(bm_sz, 0);

    int64_t year_pass_total = 0;
    {
        PROFILE_SCOPE("q9b_title_scan");
        const auto& t = db->title;
        const int32_t* __restrict__ id_ptr   = t.id.data();
        const int32_t* __restrict__ year_ptr = t.production_year.data();
        const int32_t y1=year1, y2=year2, mxm_=mxm;
        const int32_t mkp = (int32_t)t.kind_part_start.size() - 1;
        const int32_t morsel_size = 65536;
        std::vector<std::pair<int32_t,int32_t>> segs;
        segs.reserve(valid_kinds.size() * 4);
        for(int32_t kid : valid_kinds){
            if(kid < 0 || kid > mkp) continue;
            const int32_t ks = t.kind_part_start[(size_t)kid];
            const int32_t ke = t.kind_part_end[(size_t)kid];
            for(int32_t cur = ks; cur < ke; cur += morsel_size)
                segs.push_back({cur, std::min(cur + morsel_size, ke)});
        }
        (void)kid_neg;
        const int32_t n_segs = (int32_t)segs.size();
        std::atomic<int64_t> year_pass_a{0};
        pool.parallel_for([&](int tid, int nt2){
            int64_t year_pass = 0;
            for(int32_t si = tid; si < n_segs; si += nt2){
                const int32_t rs = segs[(size_t)si].first;
                const int32_t re = segs[(size_t)si].second;
                for(int32_t r = rs; r < re; ++r){
                    const int32_t py = year_ptr[r];
                    if(__builtin_expect(py<y2 || py>y1 || py<0, 1)) continue;
                    const int32_t mid = id_ptr[r];
                    if(__builtin_expect(mid<0||mid>mxm_,0)) continue;
                    title_bitmap[(size_t)mid] = 1;
                    ++year_pass;
                }
            }
            year_pass_a.fetch_add(year_pass, std::memory_order_relaxed);
        });
        year_pass_total = year_pass_a.load();
        TRACE_COUNT("q9b_title_rows_year_pass", year_pass_total);
        TRACE_COUNT("q9b_valid_movie_ids", year_pass_total);
    }

    // -----------------------------------------------------------------------
    // STEP 2: Build mi1 CSR.
    // Use parallel scan with morsel work-stealing to populate per-thread triples.
    // Sort each thread's triples in parallel (N×62K instead of 1×498K).
    // Merge per-thread sorted arrays using hierarchical parallel 2-way merge.
    // -----------------------------------------------------------------------
    struct IC { int32_t iid, cnt; };

    // Pack (mid<<5|qid) into uint64_t key + cnt for fast sort
    // mid is at most ~10.6M (24 bits), qid at most 18 (5 bits)
    // Pack as: key = (mid << 5) | qid, value = cnt
    struct KV { uint32_t key; int32_t cnt; }; // key = (mid<<5)|qid, cnt
    const int n_threads_mi1 = pool.num_threads;
    std::vector<std::vector<KV>> thread_kvs(n_threads_mi1);
    for(auto& tv : thread_kvs) tv.reserve(65536);

    std::atomic<int64_t> mi1_rows_scanned_a{0};
    std::atomic<int64_t> mi1_rows_emitted_a{0};

    // Precompute segment list for dynamic work-stealing
    struct Mi1Seg { int32_t it1_idx; int32_t dbi_idx; int32_t r0; int32_t r1; };
    std::vector<Mi1Seg> mi1_segs;
    {
        const auto& mi = db->movie_info;
        const int32_t mt_iid = (int32_t)mi.type_iid_keys.size();
        for(int32_t it1_idx = 0; it1_idx < (int32_t)it1l.size(); ++it1_idx){
            int32_t it1 = it1l[(size_t)it1_idx];
            if(it1 < 0 || it1 >= mt_iid) continue;
            const auto& ikeys    = mi.type_iid_keys[(size_t)it1];
            const auto& ioffsets = mi.type_iid_offsets[(size_t)it1];
            if(ikeys.empty()) continue;
            for(int32_t dbi_idx = 0; dbi_idx < (int32_t)matching_dbi.size(); ++dbi_idx){
                const int32_t dbi = matching_dbi[(size_t)dbi_idx].first;
                auto kit = std::lower_bound(ikeys.begin(), ikeys.end(), dbi);
                if(kit == ikeys.end() || *kit != dbi) continue;
                const int32_t local_idx = (int32_t)(kit - ikeys.begin());
                const int32_t r0 = ioffsets[(size_t)local_idx];
                const int32_t r1 = ioffsets[(size_t)local_idx + 1];
                if(r0 >= r1) continue;
                const int32_t morsel = 32768;
                for(int32_t cur = r0; cur < r1; cur += morsel)
                    mi1_segs.push_back({it1_idx, dbi_idx, cur, std::min(cur + morsel, r1)});
            }
        }
    }
    const int32_t n_mi1_segs = (int32_t)mi1_segs.size();

    {
        PROFILE_SCOPE("q9b_mi1_build");
        const auto& mi = db->movie_info;
        const uint8_t* bm = title_bitmap.data();
        const int32_t bm_size_local = bm_sz;
        std::atomic<int32_t> seg_idx_a{0};

        pool.parallel_for([&](int tid, int nt){
            auto& my_kvs = thread_kvs[(size_t)tid];
            int64_t rows_scanned = 0, rows_emitted = 0;
            while(true){
                const int32_t si = seg_idx_a.fetch_add(1, std::memory_order_relaxed);
                if(si >= n_mi1_segs) break;
                const auto& seg = mi1_segs[(size_t)si];
                const int32_t it1 = it1l[(size_t)seg.it1_idx];
                const auto& irows = mi.type_iid_rows[(size_t)it1];
                const int32_t* ir_ptr = irows.data();
                const int32_t irows_sz = (int32_t)irows.size();
                const uint32_t qid = (uint32_t)matching_dbi[(size_t)seg.dbi_idx].second;
                const int32_t r0 = seg.r0, r1 = seg.r1;
                int32_t ri = r0;
                // Skip partial group at segment start
                if(r0 > 0 && ri < r1 && ir_ptr[ri] == ir_ptr[ri-1]){
                    const int32_t skip_mid = ir_ptr[ri];
                    while(ri < r1 && ir_ptr[ri] == skip_mid) ++ri;
                }
                while(ri < r1){
                    const int32_t rmid = ir_ptr[ri];
                    int32_t cnt = 0;
                    while(ri < r1 && ir_ptr[ri] == rmid){ ++cnt; ++ri; }
                    while(ri < irows_sz && ir_ptr[ri] == rmid){ ++cnt; ++ri; }
                    ++rows_scanned;
                    if(rmid<0||rmid>=bm_size_local||!bm[(size_t)rmid]) continue;
                    rows_emitted += cnt;
                    // Pack mid and qid: mid < 2^24, qid < 32
                    const uint32_t packed_key = ((uint32_t)rmid << 5) | qid;
                    my_kvs.push_back({packed_key, cnt});
                }
            }
            mi1_rows_scanned_a.fetch_add(rows_scanned, std::memory_order_relaxed);
            mi1_rows_emitted_a.fetch_add(rows_emitted, std::memory_order_relaxed);
        });

        TRACE_COUNT("q9b_mi1_rows_scanned", mi1_rows_scanned_a.load());
        TRACE_COUNT("q9b_mi1_rows_emitted", mi1_rows_emitted_a.load());
        int64_t total_t = 0;
        for(auto& tv : thread_kvs) total_t += (int64_t)tv.size();
        TRACE_COUNT("q9b_mi1_flat_entries", total_t);
        TRACE_COUNT("q9b_mi1_rebuild", 0);
    }

    // Sort each thread's KV array in parallel (sort by packed key = (mid<<5)|qid)
    {
        PROFILE_SCOPE("q9b_triples_sort");
        pool.parallel_for([&](int tid, int nt){
            auto& tv = thread_kvs[(size_t)tid];
            std::sort(tv.begin(), tv.end(), [](const KV&a, const KV&b){
                return a.key < b.key;
            });
        });
    }

    // Merge all sorted KV arrays into CSR.
    // Use hierarchical parallel merge with ping-pong buffers to avoid repeated allocation.
    std::vector<int32_t> mi1_mids;
    std::vector<int32_t> mi1_offsets;
    std::vector<IC>      mi1_ics;
    {
        PROFILE_SCOPE("q9b_csr_build");

        const int N = n_threads_mi1;

        // Hierarchical parallel merge: merge adjacent sorted arrays pair-wise,
        // halving the number of arrays each round.
        std::vector<std::vector<KV>> arrays(N);
        for(int i = 0; i < N; ++i) arrays[(size_t)i] = std::move(thread_kvs[(size_t)i]);

        int num_arrays = N;
        while(num_arrays > 2){
            const int pairs = num_arrays / 2;
            std::vector<std::vector<KV>> merged(pairs + (num_arrays % 2));
            std::atomic<int> pair_idx{0};
            pool.parallel_for([&](int tid, int nt){
                while(true){
                    const int pi = pair_idx.fetch_add(1, std::memory_order_relaxed);
                    if(pi >= pairs) break;
                    auto& a = arrays[(size_t)(2*pi)];
                    auto& b = arrays[(size_t)(2*pi+1)];
                    std::vector<KV> m;
                    m.resize(a.size() + b.size());
                    std::merge(a.begin(), a.end(), b.begin(), b.end(), m.begin(),
                               [](const KV&x, const KV&y){ return x.key < y.key; });
                    std::vector<KV>().swap(a);
                    std::vector<KV>().swap(b);
                    merged[(size_t)pi] = std::move(m);
                }
            });
            if(num_arrays % 2 == 1)
                merged[(size_t)pairs] = std::move(arrays[(size_t)(num_arrays-1)]);
            arrays = std::move(merged);
            num_arrays = (int)arrays.size();
        }
        thread_kvs.clear();

        // Final merge: when 2 arrays remain, use parallel merge for all threads.
        // Split A into N chunks; each thread finds its range in B and merges.
        std::vector<KV> final_sorted;
        if(num_arrays == 2){
            const auto& A = arrays[0];
            const auto& B = arrays[1];
            const size_t na = A.size(), nb = B.size();
            final_sorted.resize(na + nb);
            KV* out = final_sorted.data();
            const int nt_final = pool.num_threads;

            pool.parallel_for([&](int tid, int nt){
                // Each thread handles a chunk of A
                const size_t a_start = (size_t)tid * ((na + nt - 1) / nt);
                const size_t a_end   = std::min(a_start + (na + nt - 1) / nt, na);
                if(a_start >= na) return;

                // Binary search B for the rank of A[a_start] and A[a_end]
                // The output position = a_start + rank_in_B(A[a_start])
                auto cmp = [](const KV& x, const KV& y){ return x.key < y.key; };
                const size_t b_start = (size_t)(std::lower_bound(B.begin(), B.end(), A[a_start], cmp) - B.begin());
                const size_t b_end   = (a_end < na)
                    ? (size_t)(std::lower_bound(B.begin(), B.end(), A[a_end], cmp) - B.begin())
                    : nb;

                // Merge A[a_start..a_end) with B[b_start..b_end) → out[a_start+b_start..)
                std::merge(A.data() + a_start, A.data() + a_end,
                           B.data() + b_start, B.data() + b_end,
                           out + a_start + b_start, cmp);
            });
        } else if(num_arrays == 1){
            final_sorted = std::move(arrays[0]);
        }

        const size_t total = final_sorted.size();
        const KV* sorted = final_sorted.data();

        // Build CSR from sorted KV array
        mi1_mids.reserve(total);
        mi1_offsets.reserve(total + 1);
        mi1_ics.reserve(total);
        mi1_offsets.push_back(0);

        int32_t cur_mid = -1, cur_qid = -1, cur_cnt = 0;
        for(size_t i = 0; i < total; ++i){
            const KV& kv = sorted[i];
            const int32_t mid = (int32_t)(kv.key >> 5);
            const int32_t qid = (int32_t)(kv.key & 31u);
            if(mid != cur_mid){
                if(cur_mid >= 0){
                    mi1_ics.push_back({cur_qid, cur_cnt});
                    mi1_offsets.push_back((int32_t)mi1_ics.size());
                }
                mi1_mids.push_back(mid);
                cur_mid = mid; cur_qid = qid; cur_cnt = kv.cnt;
            } else if(qid != cur_qid){
                mi1_ics.push_back({cur_qid, cur_cnt});
                cur_qid = qid; cur_cnt = kv.cnt;
            } else {
                cur_cnt += kv.cnt;
            }
        }
        if(cur_mid >= 0){
            mi1_ics.push_back({cur_qid, cur_cnt});
            mi1_offsets.push_back((int32_t)mi1_ics.size());
        }
    }

    // -----------------------------------------------------------------------
    // STEP 3: Scan person_info → ppic (person_id → pi row count).
    // -----------------------------------------------------------------------
    const int32_t mxp=(int32_t)db->name.id_to_row.size()-1;
    std::vector<uint16_t> ppic(mxp+2, 0);
    {
        PROFILE_SCOPE("q9b_pi_build");
        int64_t rows_scanned=0, rows_emitted=0;
        const auto& pi = db->person_info;
        const auto& tps = pi.type_part_start, &tpe = pi.type_part_end;
        const int32_t mt = (int32_t)tps.size()-1;
        const int32_t* __restrict__ pid_ptr = pi.person_id.data();
        const int32_t mxp_ = mxp;
        for(int32_t it2 : it2l){
            if(it2<0||it2>mt) continue;
            const int32_t pstart=tps[(size_t)it2], pend=tpe[(size_t)it2];
            for(int32_t r=pstart; r<pend; ++r){
                ++rows_scanned;
                const int32_t pid = pid_ptr[r];
                if(__builtin_expect(pid>=0&&pid<=mxp_,1)){
                    ++ppic[(size_t)pid];
                    ++rows_emitted;
                }
            }
        }
        TRACE_COUNT("q9b_pi_rows_scanned", rows_scanned);
        TRACE_COUNT("q9b_pi_rows_emitted", rows_emitted);
        TRACE_COUNT("q9b_pi_person_groups", rows_emitted);
    }

    // -----------------------------------------------------------------------
    // STEP 4: Join cast_info with mi1 CSR (parallel, work-balanced).
    // -----------------------------------------------------------------------
    const auto& nm = db->name;
    const int n_threads = pool.num_threads;
    const int32_t n_mi1 = (int32_t)mi1_mids.size();

    std::vector<std::unordered_map<uint64_t,int64_t>> thread_counts(n_threads);
    for(auto& m : thread_counts) m.reserve(64);

    std::atomic<int64_t> ci_movies_probed_a{0};
    std::atomic<int64_t> ci_rows_scanned_a{0};
    std::atomic<int64_t> ci_rows_role_pass_a{0};
    std::atomic<int64_t> ci_rows_pi_pass_a{0};
    std::atomic<int64_t> ci_rows_name_pass_a{0};

    {
        PROFILE_SCOPE("q9b_cast_info_join");

        const auto& ci  = db->cast_info;
        const int32_t* __restrict__ ci_role_ptr = ci.role_id.data();
        const int32_t* __restrict__ ci_pid_ptr  = ci.person_id.data();
        const IC* ic_ptr                  = mi1_ics.data();
        const int32_t* mi1_mids_ptr       = mi1_mids.data();
        const int32_t* mi1_offsets_ptr    = mi1_offsets.data();
        const uint16_t* ppic_ptr          = ppic.data();
        const int32_t* csr_offsets        = ci.movie_id_csr.offsets.data();
        const int32_t  csr_max_key        = (int32_t)ci.movie_id_csr.offsets.size() - 2;
        const int32_t mxp_ = mxp;

        // Work-balanced partitioning using prefix sums of cast_info rows per movie
        std::vector<int32_t> thread_mi_start(n_threads + 1);
        thread_mi_start[0] = 0;
        thread_mi_start[n_threads] = n_mi1;
        {
            PROFILE_SCOPE("q9b_ci_work_compute");
            std::vector<int64_t> work_prefix(n_mi1 + 1, 0);
            for(int32_t mi = 0; mi < n_mi1; ++mi){
                const int32_t mid = mi1_mids_ptr[(size_t)mi];
                int32_t w = (mid >= 0 && mid <= csr_max_key) ? (csr_offsets[mid+1] - csr_offsets[mid]) : 0;
                work_prefix[(size_t)mi+1] = work_prefix[(size_t)mi] + w;
            }
            const int64_t total_work = work_prefix[(size_t)n_mi1];
            const int64_t wpt = (total_work + n_threads - 1) / n_threads;
            for(int t = 1; t < n_threads; ++t){
                const int64_t target = wpt * t;
                int32_t lo = 0, hi = n_mi1;
                while(lo < hi){
                    int32_t mid_idx = (lo + hi) / 2;
                    if(work_prefix[(size_t)mid_idx+1] <= target) lo = mid_idx + 1;
                    else hi = mid_idx;
                }
                thread_mi_start[(size_t)t] = lo;
            }
        }

        pool.parallel_for([&](int tid, int nt){
            const int32_t mi_start = thread_mi_start[(size_t)tid];
            const int32_t mi_end   = thread_mi_start[(size_t)tid + 1];

            auto& counts = thread_counts[(size_t)tid];
            // Per-thread direct-mapped cache: allocated on heap per-thread to avoid stack overflow
            // 512KB per thread - initialized once per parallel_for call
            std::unique_ptr<PidCache> pcache_ptr(new PidCache());
            pcache_ptr->clear();
            PidCache& pcache = *pcache_ptr;

            int64_t ci_movies_probed=0, ci_rows_scanned=0;
            int64_t ci_rows_role_pass=0, ci_rows_pi_pass=0, ci_rows_name_pass=0;

            for(int32_t mi=mi_start; mi<mi_end; ++mi){
                const int32_t mid = mi1_mids_ptr[(size_t)mi];
                const int32_t ic0 = mi1_offsets_ptr[(size_t)mi];
                const int32_t ic1 = mi1_offsets_ptr[(size_t)mi+1];
                ++ci_movies_probed;
                const int32_t beg   = (mid>=0&&mid<=csr_max_key) ? csr_offsets[mid]   : 0;
                const int32_t end_r = (mid>=0&&mid<=csr_max_key) ? csr_offsets[mid+1] : 0;
                for(int32_t row=beg; row<end_r; ++row){
                    ++ci_rows_scanned;
                    if(!chkr(ci_role_ptr[row])) continue;
                    ++ci_rows_role_pass;
                    const int32_t pid = ci_pid_ptr[row];
                    if(__builtin_expect(pid<0||pid>mxp_,0)) continue;
                    // Direct-mapped cache lookup
                    const uint32_t slot = (uint32_t)((uint32_t)pid * 2654435761u) & PidCache::MASK;
                    int32_t nr_or_fail;
                    if(__builtin_expect(pcache.tag[slot] != pid, 0)){
                        // Cache miss: compute and store
                        if(ppic_ptr[(size_t)pid]==0){
                            pcache.tag[slot] = pid; pcache.val[slot] = -1; continue;
                        }
                        const int32_t nr = nm.id_to_row[pid];
                        if(nr<0){ pcache.tag[slot]=pid; pcache.val[slot]=-1; continue; }
                        if(!nmatch(nm.name_str[(size_t)nr])){
                            pcache.tag[slot]=pid; pcache.val[slot]=-1; continue;
                        }
                        pcache.tag[slot] = pid; pcache.val[slot] = nr;
                        nr_or_fail = nr;
                        ++ci_rows_pi_pass;
                        ++ci_rows_name_pass;
                    } else {
                        nr_or_fail = pcache.val[slot];
                        if(nr_or_fail < 0) continue;
                        ++ci_rows_pi_pass;
                        ++ci_rows_name_pass;
                    }
                    (void)nr_or_fail;
                    const int32_t pc = (int32_t)ppic_ptr[(size_t)pid];
                    for(int32_t ii=ic0; ii<ic1; ++ii){
                        const uint64_t key = ((uint64_t)(uint32_t)ic_ptr[ii].iid << 32)
                                           | (uint64_t)(uint32_t)pid;
                        counts[key] += (int64_t)ic_ptr[ii].cnt * pc;
                    }
                }
            }

            ci_movies_probed_a.fetch_add(ci_movies_probed, std::memory_order_relaxed);
            ci_rows_scanned_a.fetch_add(ci_rows_scanned,   std::memory_order_relaxed);
            ci_rows_role_pass_a.fetch_add(ci_rows_role_pass, std::memory_order_relaxed);
            ci_rows_pi_pass_a.fetch_add(ci_rows_pi_pass,   std::memory_order_relaxed);
            ci_rows_name_pass_a.fetch_add(ci_rows_name_pass, std::memory_order_relaxed);
        });

        TRACE_COUNT("q9b_ci_movies_probed",  ci_movies_probed_a.load());
        TRACE_COUNT("q9b_ci_rows_scanned",   ci_rows_scanned_a.load());
        TRACE_COUNT("q9b_ci_rows_role_pass", ci_rows_role_pass_a.load());
        TRACE_COUNT("q9b_ci_rows_pi_pass",   ci_rows_pi_pass_a.load());
        TRACE_COUNT("q9b_ci_rows_name_pass", ci_rows_name_pass_a.load());
    }

    // Merge per-thread counts
    std::unordered_map<uint64_t,int64_t> counts;
    counts.reserve(256);
    {
        PROFILE_SCOPE("q9b_merge");
        for(int t = 0; t < n_threads; ++t)
            for(auto& [key, val] : thread_counts[(size_t)t])
                counts[key] += val;
    }

    // Rebuild name row lookup (only valid pids in output keys)
    std::unordered_map<int32_t,int32_t> pstate;
    pstate.reserve(counts.size() + 4);
    for(const auto& [key, val] : counts){
        const int32_t pid = (int32_t)(key & 0xFFFFFFFFu);
        if(!pstate.count(pid)) pstate[pid] = nm.id_to_row[pid];
    }

    TRACE_COUNT("q9b_output_groups",     (int64_t)counts.size());
    TRACE_COUNT("q9b_query_output_rows", (int64_t)counts.size());

    // Output
    std::vector<std::vector<std::string>> rows;
    rows.reserve(counts.size()+1);
    rows.push_back({"info","name","count_star()"});
    for(const auto& [key,cnt] : counts){
        const int32_t iid=(int32_t)(key>>32), pid=(int32_t)(key&0xFFFFFFFFu);
        rows.push_back({istrs[(size_t)iid], nm.name_str[(size_t)pstate.at(pid)], std::to_string(cnt)});
    }
    return rows;
}
