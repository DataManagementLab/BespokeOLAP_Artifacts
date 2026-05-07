#pragma once

#include "parquet_reader.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Compressed Sparse Row index  (key → contiguous slice of row_ids)
// ─────────────────────────────────────────────────────────────────────────────
struct CsrIndex {
    std::vector<int32_t> offsets;   // size = max_key + 2
    std::vector<int32_t> row_ids;   // size = total entries
};

// ─────────────────────────────────────────────────────────────────────────────
// lineitem
// ─────────────────────────────────────────────────────────────────────────────
struct LineitemTable {
    int64_t num_rows = 0;

    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    // Sorted permutation of row indices by l_receiptdate ASC.
    // Enables binary-search range scans on receiptdate (used by q12).
    std::vector<int32_t> l_receiptdate_sorted_idx;
    std::vector<uint8_t> l_commitdate_lt_receiptdate;  // precomputed: l_commitdate < l_receiptdate
    std::vector<uint8_t> l_shipdate_lt_commitdate;     // precomputed: l_shipdate < l_commitdate

    // Compact parallel arrays in receiptdate-sorted order.
    // These enable sequential (non-scattered) access during receiptdate range
    // scans, eliminating the cache-miss penalty of the rdate_sorted_idx
    // permutation when accessing column data.
    // Each array li_rds_X[k] corresponds to the row rdate_sorted_idx[k].
    // General-purpose: usable by any query filtering on l_receiptdate range.
    //
    // Memory: 120M rows × 7 bytes ≈ 840 MB at SF=20 (individual arrays).
    std::vector<uint8_t>  li_rds_shipmode;      // l_shipmode  in rdate-sorted order
    std::vector<uint8_t>  li_rds_clt_rd;        // l_commitdate < l_receiptdate (precomputed)
    std::vector<uint8_t>  li_rds_slt_cd;        // l_shipdate < l_commitdate (precomputed)
    std::vector<int32_t>  li_rds_orderkey_i32;  // l_orderkey as int32 in rdate-sorted order
    // Receiptdate value in rdate-sorted order (enables binary search + direct value).
    // Stored as int32 days since epoch (same encoding as l_receiptdate).
    std::vector<int32_t>  li_rds_receiptdate;   // l_receiptdate in rdate-sorted order

    // AoS packed struct for Q12 hot loop: combines all per-row filter fields into
    // a single 8-byte record so one cache line covers 8 complete rows instead of
    // requiring separate cache line fetches from 4 independent arrays.
    // Layout: shipmode(1) + combined_flags(1) + pad(2) + orderkey_i32(4) = 8 bytes.
    //   combined_flags: bit 0 = clt_rd (commitdate < receiptdate)
    //                   bit 1 = slt_cd (shipdate < commitdate)
    //                   bit 2 = prio_is_high: o_orderpriority is '1-URGENT' or '2-HIGH'
    //                           Pre-joined from orders at load time; eliminates the
    //                           orders_row_by_orderkey random-access join in Q12 hot loop.
    // General-purpose: any query doing a receiptdate-range scan that needs
    // (shipmode, commitdate<receiptdate, shipdate<commitdate, prio_is_high) can use
    // this AoS layout for improved cache locality vs. 4 separate SoA arrays.
    struct RdsPackedRow {
        uint8_t  shipmode;       // l_shipmode enum code
        uint8_t  flags;          // bit0=clt_rd, bit1=slt_cd, bit2=prio_is_high (pre-joined)
        uint8_t  pad0;
        uint8_t  pad1;
        int32_t  orderkey_i32;   // l_orderkey as int32 (kept for general-purpose use)
    };
    static_assert(sizeof(RdsPackedRow) == 8, "RdsPackedRow must be 8 bytes");
    // Packed AoS array in receiptdate-sorted order.
    // Memory: 120M rows × 8 bytes = 960 MB at SF=20.
    // Replaces 4 separate arrays (sm=1B, clt=1B, slt=1B, ok=4B = 7B) with better
    // spatial locality: one 64-byte cache line covers 8 complete rows vs. partial
    // coverage of each individual column array.
    std::vector<RdsPackedRow> li_rds_packed;   // AoS in rdate-sorted order

    // Compact 2-byte parallel array for Q12 hot loop.
    // li_rds_sm_flags[k] = (shipmode << 8) | flags for the k-th row in rdate-sorted order.
    // Packs the only two bytes needed for Q12 aggregation into one uint16_t:
    //   high byte = shipmode (l_shipmode enum code)
    //   low  byte = flags (bit0=clt_rd, bit1=slt_cd, bit2=prio_is_high)
    // At 2 bytes/row, a 64-byte cache line covers 32 rows vs. 8 rows for li_rds_packed.
    // This 4× reduction in data volume cuts memory bandwidth from ~48MB to ~12MB
    // for a typical 1-year receiptdate range at SF=20 (≈6M rows).
    // General-purpose: any query filtering on (shipmode, commitdate<receiptdate,
    // shipdate<commitdate, prio_is_high) can use this compact layout.
    std::vector<uint16_t> li_rds_sm_flags;   // (shipmode<<8)|flags in rdate-sorted order

    // Scaled-integer versions for fast filter in q6 (and general use).
    // l_discount_int8: discount * 100, range [0,10] → fits int8_t
    // l_quantity_int8: quantity as integer, range [1,50] → fits int8_t (unsigned)
    // l_tax_int8: tax * 100, range [0,8] → fits uint8_t
    // Storing tax as uint8 (1 byte) instead of double (8 bytes) cuts 7 bytes/row
    // in Q1's hot aggregation loop. General-purpose: any query needing integer tax.
    std::vector<uint8_t> l_tax_int8;    // = round(l_tax * 100), range [0,8]
    std::vector<int8_t>  l_discount_int8;   // = round(l_discount * 100)
    std::vector<uint8_t> l_quantity_int8;   // = (uint8_t)l_quantity

    // Pre-computed integer version of l_extendedprice * 100.
    // TPC-H prices have exactly 2 decimal places; max ep ~104950 → max*100=10495000
    // fits in int32_t. Used by Q10 to avoid llround() in the hot aggregation loop.
    // General-purpose: any query computing revenue/price in integer arithmetic can use it.
    std::vector<int32_t> l_extendedprice_int32;  // = round(l_extendedprice * 100)

    // l_suppkey_i32: suppkey cast to int32_t.
    // TPC-H suppkey ≤ 10000*SF fits easily in int32_t (max 200K at SF=20).
    // Halves memory bandwidth vs. int64_t for queries that scan suppkey in hot loops
    // (e.g. Q15 revenue aggregation by supplier). Built after the shipdate sort so
    // it matches the sorted row order. General-purpose: any query that needs suppkey
    // as an array index or compact value can use this instead of l_suppkey.
    std::vector<int32_t> l_suppkey_i32;  // = (int32_t)l_suppkey[i]

    std::vector<double>  l_quantity;

    // li_cust_nationkey_u8[i] = c_nationkey of the customer for lineitem row i.
    // Built at load time as a general-purpose pre-joined column: eliminates the
    // two-level indirection (l_orderkey → orders_row_by_orderkey → o_cust_nationkey_u8)
    // in the hot Q7 scan loop.  Stores the uint8 nationkey (0-24); 0xFF = unknown.
    //
    // Construction: lineitem rows are stored in l_shipdate order.  For each row i,
    //   int64_t ok = l_orderkey[i];
    //   int32_t ord_row = orders_row_by_orderkey[ok];
    //   li_cust_nationkey_u8[i] = o_cust_nationkey_u8[ord_row];
    //
    // Memory: 120M rows × 1 byte = 120 MB at SF=20.
    // General-purpose: any query that needs the customer nationkey per lineitem row
    // (Q7, Q8, and any similar shipping-lane analysis) can use this column directly
    // instead of the two-step orderkey→order-row→customer-nationkey indirection.
    std::vector<uint8_t> li_cust_nationkey_u8;

    // li_suppkey_nationkey_u8[i] = s_nationkey for l_suppkey[i] (uint8, 0xFF = unknown).
    // Eliminates the suppkey_pair table lookup in the Q7 hot loop.
    // General-purpose: any query needing supplier nationkey per lineitem can use this.
    std::vector<uint8_t> li_suppkey_nationkey_u8;

    // li_suppnk_custnk_u16[i] = (li_suppkey_nationkey_u8[i] << 8) | li_cust_nationkey_u8[i].
    // Packs supplier + customer nationkey per lineitem into a single uint16_t.
    // At 36M rows in the Q7 date range, this halves the cache-line fetches for
    // the two-key filter (64B covers 32 complete rows vs. 64B per array for 64 rows each).
    // Low byte  = customer nationkey (li_cust_nationkey_u8)
    // High byte = supplier nationkey (li_suppkey_nationkey_u8)
    //
    // Memory: 120M rows × 2 bytes = 240 MB at SF=20.
    // General-purpose: any query filtering on both supplier and customer nationkey
    // per lineitem (Q7, Q8, or similar shipping-lane analysis) can use this
    // instead of two separate 1-byte arrays.
    std::vector<uint16_t> li_suppnk_custnk_u16;

    // l_partkey_i32: partkey cast to int32_t.
    // TPC-H partkey ≤ 200000*SF fits easily in int32_t (max 4M at SF=20).
    // Halves memory bandwidth vs. int64_t for queries that scan partkey in hot loops
    // (e.g. Q14 promo revenue scan over date-filtered range).
    // Built after the shipdate sort so it matches the sorted row order.
    // General-purpose: any query that needs partkey as an array index or compact
    // value can use this instead of l_partkey.
    std::vector<int32_t> l_partkey_i32;  // = (int32_t)l_partkey[i]

    // ── Q14 promo-revenue acceleration ──────────────────────────────────────
    //
    // li_is_promo_u8[i]: 1 if the part for lineitem row i has p_type LIKE 'PROMO%',
    //                    0 otherwise.  Stored in l_shipdate-sorted order.
    //
    // Pre-joining this flag eliminates the two-level random-access join in Q14's
    // hot scan loop:
    //   Old: partkey_data[i] → is_promo_by_pk[pk]   (random access into 4MB array)
    //   New: li_is_promo_u8[i]                        (sequential access)
    //
    // Construction: after part_row_by_partkey and p_type_enum are ready,
    //   for each row i: is_promo = (p_type_enum[part_row_by_partkey[l_partkey_i32[i]]] is promo)
    //
    // Memory: 120M rows × 1 byte = 120 MB at SF=20.
    // General-purpose: any query needing per-lineitem promo-type flag (Q14, Q19).
    std::vector<uint8_t> li_is_promo_u8;   // 1 if p_type LIKE 'PROMO%', 0 otherwise

    std::vector<double>  l_extendedprice;
    std::vector<double>  l_discount;
    std::vector<double>  l_tax;

    std::vector<int64_t> l_orderkey;
    std::vector<int64_t> l_partkey;
    std::vector<int64_t> l_suppkey;
    std::vector<int32_t> l_linenumber;

    std::vector<uint8_t> l_returnflag;     // 'A'=0,'N'=1,'R'=2
    std::vector<uint8_t> l_linestatus;     // 'F'=0,'O'=1
    std::vector<uint8_t> l_shipmode;       // 7 values
    std::vector<uint8_t> l_shipinstruct;   // 4 values

    std::vector<std::string> l_comment;

    // Enum decode tables
    std::vector<std::string> returnflag_names;
    std::vector<std::string> linestatus_names;
    std::vector<std::string> shipmode_names;
    std::vector<std::string> shipinstruct_names;
};

// ─────────────────────────────────────────────────────────────────────────────
// orders
// ─────────────────────────────────────────────────────────────────────────────
struct OrdersTable {
    int64_t num_rows = 0;

    std::vector<int32_t> o_orderdate;
    std::vector<int64_t> o_orderkey;
    std::vector<int64_t> o_custkey;
    std::vector<double>  o_totalprice;
    std::vector<int32_t> o_shippriority;

    std::vector<uint8_t> o_orderstatus;
    std::vector<uint8_t> o_orderpriority;

    // Dense per-row CSR slice into li_by_orderkey.row_ids.
    // o_li_csr_lo[i] and o_li_csr_hi[i] are the start/end offsets in
    // li_by_orderkey.row_ids for order row i.  Avoids indexing into the
    // huge (up to ~120M entry) li_by_orderkey.offsets array which does
    // not fit in cache.  These arrays are dense over order rows (30M at
    // SF=20) so sequential access in date-range scans is cache-friendly.
    std::vector<int32_t> o_li_csr_lo;  // = li_by_orderkey.offsets[o_orderkey[i]]
    std::vector<int32_t> o_li_csr_hi;  // = li_by_orderkey.offsets[o_orderkey[i]+1]

    // Per-order lineitem count as uint8_t: o_li_count[i] = hi - lo for order row i.
    // TPC-H guarantees each order has 1..7 lineitems, so uint8_t is sufficient.
    // Reduces memory bandwidth in Q18's hot loop: instead of loading lo (4B) + hi (4B)
    // per order, load lo (4B) + count (1B) = 5B (saving 3B per order = 90MB at SF=20).
    // Combined with o_li_csr_lo, gives the qty slice as [lo, lo+count).
    // General-purpose: any query that only needs the lineitem count per order
    // can use this instead of computing hi - lo.
    std::vector<uint8_t> o_li_count;   // = o_li_csr_hi[i] - o_li_csr_lo[i], range [1,7]

    // ── Flat per-order qty array for sequential + SIMD scan ──────────────────
    // For each order row i (in o_orderdate-sorted order), stores:
    //   o_li_qty_flat[i*8 + 0..cnt-1] = qty values (1-7 bytes, each 1-50)
    //   o_li_qty_flat[i*8 + cnt .. i*8+7] = 0 (zero padding)
    //
    // NO count byte (unlike previous version): padding=0 means the horizontal byte
    // sum of all 8 bytes gives the correct qty total without masking or shifting.
    // Enables both:
    //   (a) Scalar SWAR: load uint64_t, 3-step horizontal byte-sum, 0-overhead.
    //   (b) SIMD AVX2: _mm256_sad_epu8 processes 4 orders per instruction (32B/iter).
    // Eliminates the CSR random-access pattern (li_by_orderkey_qty[lo..lo+cnt])
    // which caused ~30M cache-scattered reads into 120MB at SF=20.
    //
    // Memory: 8 × n_orders = 240 MB at SF=20.
    // General-purpose: any query needing per-order l_quantity sums can use this
    // instead of CSR scatter into li_by_orderkey_qty.
    std::vector<uint8_t> o_li_qty_flat;  // 8 bytes per order: [q0, q1, ..., q6, 0...0]

    std::vector<std::string> o_clerk;
    std::vector<std::string> o_comment;

    // ── Flat comment buffer (Q13 optimisation) ────────────────────────────
    // All o_comment strings concatenated into one contiguous byte array.
    // o_comment_offs[i]..o_comment_offs[i+1] gives row i's comment slice.
    // Eliminates per-string heap pointer chasing during the full-table scan
    // in Q13, converting 30M scattered heap reads into one sequential scan.
    // General-purpose: any query scanning o_comment can use this layout.
    std::vector<char>     o_comment_flat;  // concatenated comment bytes
    // uint64_t offsets: at SF=100, 150M orders × ~40B avg = ~6GB total > uint32_t max (4.29GB).
    std::vector<uint64_t> o_comment_offs;  // size = num_rows + 1

    // ── Narrow custkey (Q13 optimisation) ─────────────────────────────────
    // int32_t version of o_custkey (TPC-H custkey ≤ 150000*SF fits int32).
    // Halves memory bandwidth vs. int64 during the full-orders scan in Q13
    // and any other query that only needs custkey as an array index.
    // General-purpose: usable by any query doing custkey lookup/join.
    std::vector<int32_t> o_custkey_i32;

    // Pre-joined customer nationkey per order row (uint8_t, 0-24).
    // o_cust_nationkey_u8[i] = c_nationkey of the customer for order row i.
    // Eliminates the indirect custkey → nationkey lookup during Q5's order scan:
    // instead of cnk_arr[custkey_arr[oi]] (random access into 3MB map), the query
    // reads this array sequentially.  0xFF is reserved for unknown/invalid.
    // General-purpose: any query that needs the customer's nationkey per order
    // (e.g. Q5, Q7, Q8) can use this instead of joining through customer table.
    std::vector<uint8_t> o_cust_nationkey_u8;

    // ── Per-order EXISTS(l_commitdate < l_receiptdate) flag (Q4 optimisation) ─
    // o_has_clt_rd[i] = 1 if order row i has at least one lineitem where
    // l_commitdate < l_receiptdate, 0 otherwise.
    // This pre-computes the entire EXISTS subquery result per order at load time,
    // reducing the Q4 hot loop from "scan avg 4 bytes of CSR per order" to
    // "load 1 byte per order" — a 4× memory bandwidth reduction.
    // General-purpose: any query needing this EXISTS result can use this flag.
    std::vector<uint8_t> o_has_clt_rd;

    // ── Packed bitset version of o_has_clt_rd ─────────────────────────────
    // o_has_clt_rd_bits[i/8] bit (i%8) = o_has_clt_rd[i].
    // At SF=20: 30M orders → 3.75 MB as uint8 → 468 KB as bitset.
    // A 3-month range spans ~375K orders → ~47KB bitset, fits in L1/L2 cache.
    // 64-bit popcount on bitset words gives branchless counting of qualifying orders.
    // General-purpose: any query doing a boolean per-order check can use this.
    std::vector<uint64_t> o_has_clt_rd_bits;  // ceil(num_rows/64) words

    // ── Per-order multi-suppkey flag (Q21 optimisation) ───────────────────
    // o_has_multi_suppkey[i] = 1 if order row i has lineitems with at least
    // two different suppkeys, 0 otherwise.
    // Pre-computes the EXISTS subquery in Q21:
    //   exists(select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey
    //          and l2.l_suppkey <> l1.l_suppkey)
    // Eliminates the entire Scan-1 (EXISTS check) in Q21's hot loop by replacing
    // it with a single byte load per order.  Built at load time from the
    // li_by_orderkey_suppkey_i32 companion array (which is already built before
    // the orders table augmentation pass).
    // Memory: num_orders × 1 byte = 30 MB at SF=20.
    // General-purpose: any query checking multi-supplier orders can use this flag.
    std::vector<uint8_t> o_has_multi_suppkey;  // 1 if order has ≥2 distinct suppkeys

    // ── Per-order unique-late-suppkey (Q21 optimisation) ──────────────────
    // o_late_unique_suppkey[i] encodes the result of Q21's NOT-EXISTS + unique-
    // late-supplier check per order:
    //   -1 : no lineitem in this order has receiptdate > commitdate (no late entries)
    //    0 : 2+ distinct suppkeys have late entries → NOT-EXISTS fails
    //   >0 : exactly one distinct suppkey sk has late entries → NOT-EXISTS passes;
    //        the value IS the suppkey (int32, matches l_suppkey_i32).
    //
    // This pre-computes the inner NOT-EXISTS scan of Q21 at load time:
    //   NOT EXISTS (SELECT * FROM lineitem l3
    //               WHERE l3.l_orderkey = l1.l_orderkey
    //                 AND l3.l_suppkey <> l1.l_suppkey
    //                 AND l3.l_receiptdate > l3.l_commitdate)
    // which is equivalent to: at most one distinct suppkey appears among late rows.
    //
    // Eliminates the entire inner CSR scan (~40M reads at SF=20) for the NOT-EXISTS
    // check, replacing it with a single int32 load per qualifying order.
    // Memory: num_orders × 4 bytes = 120 MB at SF=20.
    // General-purpose: any query checking "unique late supplier per order"
    // (e.g. supplier reliability queries) can use this pre-joined value.
    std::vector<int32_t> o_late_unique_suppkey;  // -1=none, 0=multiple, >0=unique sk

    std::vector<std::string> orderstatus_names;
    std::vector<std::string> orderpriority_names;
};

// ─────────────────────────────────────────────────────────────────────────────
// customer
// ─────────────────────────────────────────────────────────────────────────────
struct CustomerTable {
    int64_t num_rows = 0;

    std::vector<int64_t> c_custkey;
    std::vector<int32_t> c_nationkey;
    std::vector<double>  c_acctbal;

    std::vector<uint8_t> c_mktsegment;

    // Pre-formatted string representations for fast output formatting.
    // These are general-purpose: any query that outputs c_custkey or
    // c_acctbal as strings can use these directly instead of calling
    // to_string / snprintf per output row.
    std::vector<std::string> c_custkey_str;   // = std::to_string(c_custkey[i])
    std::vector<std::string> c_acctbal_str;   // = sprintf("%.2f", c_acctbal[i])
    std::vector<uint8_t>     c_nationkey_u8;  // = (uint8_t)c_nationkey[i], for dense lookup
    // Pre-joined nation name per customer row.
    // Eliminates the per-row nation name lookup and string construction in Q10.
    // General-purpose: any query outputting nation name alongside customer data can use it.
    std::vector<std::string> c_nation_name;   // = nation.n_name[c_nationkey[i]]

    std::vector<std::string> c_phone_cc;   // first 2 chars of c_phone
    std::vector<std::string> c_phone;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<std::string> c_comment;

    // Packed 2-char phone country code as uint16_t: low byte = c_phone[0], high byte = c_phone[1].
    // Enables fast integer comparison for phone-code filters in Q22 and any other
    // query that matches on the 2-char phone prefix, avoiding heap pointer chasing
    // and string comparison overhead of c_phone_cc[].
    // General-purpose: any query filtering/grouping by phone country code can use this
    // instead of string-based c_phone_cc[].
    std::vector<uint16_t> c_phone_cc_u16;

    // Pre-joined has_order flag: c_has_order[i] = 1 if customer i has at least one
    // order in the orders table (i.e. NOT EXISTS SELECT * FROM orders WHERE o_custkey
    // = c_custkey is false), 0 otherwise.
    // Eliminates the per-row custkey lookup + CSR offset check done at query time.
    // General-purpose: any query doing NOT EXISTS / LEFT ANTI JOIN on orders can use
    // this directly, avoiding construction of an ad-hoc has_order bitmap per query.
    // Memory: num_customer bytes (3 MB at SF=20).
    std::vector<uint8_t> c_has_order;

    std::vector<std::string> mktsegment_names;
};

// ─────────────────────────────────────────────────────────────────────────────
// part
// ─────────────────────────────────────────────────────────────────────────────
struct PartTable {
    int64_t num_rows = 0;

    std::vector<int64_t> p_partkey;
    std::vector<int32_t> p_size;
    std::vector<double>  p_retailprice;

    std::vector<uint8_t> p_brand;
    std::vector<uint8_t> p_mfgr;
    std::vector<uint8_t> p_container;
    std::vector<uint8_t> p_type_enum;   // enum-encoded p_type (general-purpose, fits uint8_t: ≤150 distinct)

    std::vector<std::string> p_type;
    std::vector<std::string> p_name;
    std::vector<std::string> p_comment;

    std::vector<std::string> brand_names;
    std::vector<std::string> mfgr_names;
    std::vector<std::string> container_names;
    std::vector<std::string> type_names;   // decode table for p_type_enum

    // Pre-formatted p_partkey string for fast output (avoids per-row snprintf in Q2).
    // General-purpose: any query outputting p_partkey as string can use this directly.
    std::vector<std::string> p_partkey_str;

    // p_name_word1_u8[i]: enum code for the first word of p_name[i].
    // TPC-H p_name = "word1 word2 word3 word4 word5" where each word is one of 92
    // color names.  Storing the first-word code as uint8_t (92 values fit in uint8_t)
    // enables Q20's LIKE 'COLOR%' filter to be a fast sequential uint8_t comparison
    // instead of string memcmp with pointer indirection into heap-allocated strings.
    //
    // Encoding: p_name_word1_code_table[k] = the k-th distinct first-word string.
    //           p_name_word1_u8[i] = code k such that p_name_word1_code_table[k] == word1.
    //           0xFF = unknown / not found (should not occur in valid TPC-H data).
    //
    // Memory: 800K rows × 1 byte = 800 KB at SF=20 — fits entirely in L2 cache.
    // General-purpose: any query filtering on LIKE 'COLOR%' on p_name (Q20, Q9-variant)
    // can look up the color code once and compare uint8_t array instead of string array.
    std::vector<uint8_t>     p_name_word1_u8;        // enum code for first word of p_name
    std::vector<std::string> p_name_word1_code_table; // code → first-word string
    std::unordered_map<std::string, uint8_t> p_name_word1_to_code; // string → code
};

// ─────────────────────────────────────────────────────────────────────────────
// supplier
// ─────────────────────────────────────────────────────────────────────────────
struct SupplierTable {
    int64_t num_rows = 0;

    std::vector<int64_t> s_suppkey;
    std::vector<int32_t> s_nationkey;
    std::vector<double>  s_acctbal;

    std::vector<std::string> s_name;
    std::vector<std::string> s_address;
    std::vector<std::string> s_phone;
    std::vector<std::string> s_comment;

    // Pre-formatted s_acctbal string ("%.2f") for fast output (avoids per-row snprintf in Q2).
    // General-purpose: any query outputting s_acctbal as string can use this directly.
    std::vector<std::string> s_acctbal_str;

    std::vector<bool> s_has_complaint;
};

// ─────────────────────────────────────────────────────────────────────────────
// partsupp
// ─────────────────────────────────────────────────────────────────────────────
struct PartsuppTable {
    int64_t num_rows = 0;

    std::vector<int64_t> ps_partkey;
    std::vector<int64_t> ps_suppkey;     // original int64 (kept for general use)
    std::vector<int32_t> ps_availqty;
    std::vector<double>  ps_supplycost;
    std::vector<std::string> ps_comment;

    // Derived columns for fast Q11 aggregation:
    // ps_suppkey_i32: suppkey cast to int32_t (TPC-H suppkey ≤ 10*SF*10 = fits int32)
    // Halves the memory footprint of suppkey vs. int64, improving cache utilisation.
    std::vector<int32_t> ps_suppkey_i32;

    // ps_supplycost_cents: supplycost * 100 rounded to integer.
    // TPC-H supplycost ∈ [1.00, 1000.00] with 2 decimal places → fits int32_t.
    // Allows integer multiply (int64 = int32 * int32) in the hot aggregation loop,
    // avoiding slower floating-point multiply.
    std::vector<int32_t> ps_supplycost_cents;

    // ps_partkey_i32: partkey cast to int32_t.
    // TPC-H partkey ≤ 200000*SF fits easily in int32_t (max 4M at SF=20).
    // Halves memory bandwidth vs. int64_t for queries that scan partkey in hot loops.
    // General-purpose: any query that needs partkey as an array index or compact
    // value can use this instead of ps_partkey.
    std::vector<int32_t> ps_partkey_i32;

    // Packed row struct for cache-efficient multi-column scans.
    // Stores the 4 hot integer fields contiguously so a single cache line covers
    // multiple complete rows instead of each field being in a separate 64MB array.
    // General-purpose: any query that needs (suppkey, partkey, availqty, cost_cents)
    // together in a tight loop can use this AoS layout for better spatial locality.
    struct PackedRow {
        int32_t suppkey;       // = ps_suppkey_i32
        int32_t partkey;       // = ps_partkey_i32
        int32_t availqty;      // = ps_availqty
        int32_t cost_cents;    // = ps_supplycost_cents
    };
    std::vector<PackedRow> ps_packed;  // AoS layout, same row order as other arrays
};

// ─────────────────────────────────────────────────────────────────────────────
// nation  (25 rows)
// ─────────────────────────────────────────────────────────────────────────────
struct NationTable {
    int32_t num_rows = 0;

    std::vector<int32_t>     n_nationkey;
    std::vector<int32_t>     n_regionkey;
    std::vector<std::string> n_name;
    std::vector<std::string> n_comment;

    std::unordered_map<std::string, int32_t> name_to_nationkey;
    std::array<int32_t, 25>  nationkey_to_regionkey{};
};

// ─────────────────────────────────────────────────────────────────────────────
// region  (5 rows)
// ─────────────────────────────────────────────────────────────────────────────
struct RegionTable {
    int32_t num_rows = 0;

    std::vector<int32_t>     r_regionkey;
    std::vector<std::string> r_name;
    std::vector<std::string> r_comment;

    std::unordered_map<std::string, int32_t> name_to_regionkey;
};

// ─────────────────────────────────────────────────────────────────────────────
// Database: all tables + cross-table acceleration structures
// ─────────────────────────────────────────────────────────────────────────────
struct Database {
    LineitemTable  lineitem;
    OrdersTable    orders;
    CustomerTable  customer;
    PartTable      part;
    SupplierTable  supplier;
    PartsuppTable  partsupp;
    NationTable    nation;
    RegionTable    region;

    // Direct-indexed lookup: value = row index (-1 = absent)
    std::vector<int32_t> orders_row_by_orderkey;
    std::vector<int32_t> cust_row_by_custkey;
    std::vector<int32_t> supp_row_by_suppkey;
    std::vector<int32_t> part_row_by_partkey;

    // CSR indexes
    CsrIndex li_by_orderkey;    // lineitem rows per orderkey
    // Companion array to li_by_orderkey: for each slot i in row_ids,
    // li_by_orderkey_clt_rd[i] = l_commitdate_lt_receiptdate[row_ids[i]].
    // Co-locating the flag value with the CSR row_ids eliminates the scattered
    // random access to li.l_commitdate_lt_receiptdate[row_ids[i]] during the
    // EXISTS probe in Q4 (and any similar query), converting it to a sequential
    // scan of a compact uint8 array.
    std::vector<uint8_t> li_by_orderkey_clt_rd;

    // Companion array: li_by_orderkey_suppnk[j] = s_nationkey for CSR slot j.
    // Pre-joined suppkey→nationkey at load time, eliminating scattered random
    // access to supp_nationkey_map[suppkey] during Q5's inner lineitem loop.
    std::vector<uint8_t> li_by_orderkey_suppnk;

    // Companion array: li_by_orderkey_qty[j] = l_quantity_int8 for CSR slot j.
    // For each slot j in li_by_orderkey.row_ids, stores the integer quantity of
    // the corresponding lineitem row.  This allows the Q18 subquery aggregation
    // (sum(l_quantity) GROUP BY l_orderkey) to be computed with purely sequential
    // memory access by iterating orders and their CSR slices, eliminating the
    // random access to l_quantity_int8[row_ids[j]].
    // Memory: 120M rows × 1 byte = 120 MB at SF=20.
    // General-purpose: any query that needs l_quantity in CSR (orderkey) order
    // can use this instead of l_quantity_int8[li_by_orderkey.row_ids[j]].
    std::vector<uint8_t> li_by_orderkey_qty;

    // Companion array: li_by_orderkey_suppkey_i32[j] = l_suppkey as int32 for CSR slot j.
    // For each slot j in li_by_orderkey.row_ids, stores the int32-cast suppkey of the
    // corresponding lineitem row.  This allows Q21's inner per-order loop to read suppkeys
    // with purely sequential memory access instead of scattered random reads into the 960MB
    // l_suppkey[] array (accessed via row_ids[j] indirection).
    // Memory: 120M rows × 4 bytes = 480 MB at SF=20.
    // General-purpose: any query iterating orders via CSR that needs l_suppkey
    // can use this instead of l_suppkey[li_by_orderkey.row_ids[j]].
    std::vector<int32_t> li_by_orderkey_suppkey_i32;

    // Companion array: li_by_orderkey_ep_i32[j] = l_extendedprice_int32 for CSR slot j.
    // For each slot j in li_by_orderkey.row_ids, stores the integer extendedprice
    // (= round(l_extendedprice * 100)) of the corresponding lineitem row.
    // Converts scattered random access to l_extendedprice_int32[row_ids[j]] into
    // sequential streaming reads -- critical for Q3 revenue aggregation inner loop.
    // Memory: 120M rows x 4 bytes = 480 MB at SF=20.
    // General-purpose: any query computing revenue in integer arithmetic via the
    // orderkey CSR can use this instead of l_extendedprice_int32[row_ids[j]].
    std::vector<int32_t> li_by_orderkey_ep_i32;

    // Companion array: li_by_orderkey_disc_i8[j] = l_discount_int8 for CSR slot j.
    // For each slot j in li_by_orderkey.row_ids, stores the discount as int8
    // (= round(l_discount * 100)) of the corresponding lineitem row.
    // Converts scattered random access to l_discount_int8[row_ids[j]] into sequential reads.
    // Memory: 120M rows x 1 byte = 120 MB at SF=20.
    // General-purpose: any query computing revenue via orderkey CSR can use this.
    std::vector<int8_t> li_by_orderkey_disc_i8;

    // Companion array: li_by_orderkey_shipdate[j] = l_shipdate for CSR slot j.
    // For each slot j in li_by_orderkey.row_ids, stores l_shipdate of the corresponding row.
    // Converts scattered random access to l_shipdate[row_ids[j]] (binary search in Q3)
    // into sequential reads within each order's CSR block.
    // Memory: 120M rows x 4 bytes = 480 MB at SF=20.
    // General-purpose: any query filtering l_shipdate via orderkey CSR can use this.
    std::vector<int32_t> li_by_orderkey_shipdate;

    // Companion array: li_by_orderkey_rev_i32[j] = precomputed revenue contribution for CSR slot j.
    // Stores ep_i32[row_ids[j]] * (100 - disc_i8[row_ids[j]]) as int32_t.
    // Max value: ~10,495,000 * 100 = 1,049,500,000 < INT32_MAX (2,147,483,647). Safe.
    // Eliminates the multiply in Q3's inner revenue loop and reduces loaded bytes from 5
    // (4B ep + 1B disc) to 4B per slot — same width as the shipdate companion array,
    // so both arrays share the same cache-line layout and access pattern.
    // Memory: 120M rows × 4 bytes = 480 MB at SF=20.
    // General-purpose: any query computing ep*(1-disc) revenue via orderkey CSR benefits.
    std::vector<int32_t> li_by_orderkey_rev_i32;

    // Companion array: li_by_orderkey_returnflag[j] = l_returnflag enum code for CSR slot j.
    // For each slot j in li_by_orderkey.row_ids, stores the returnflag byte of the
    // corresponding lineitem row.  Converts the scattered random access to
    // l_returnflag[row_ids[j]] (accessing a 300MB array at SF=50 via random indices)
    // into a sequential read of a compact 300M-byte companion array.
    //
    // The scattered read pattern: for a 3-month date window the lineitem rows
    // are spread across the entire 300MB l_returnflag array (not clustered by
    // orderdate), causing frequent L3 cache misses.  The companion array stores
    // them in CSR order so the inner loop can stream sequentially.
    //
    // Memory: ~300M rows × 1 byte = 300 MB at SF=50.
    // General-purpose: any query filtering by l_returnflag via the orderkey CSR
    // (Q10, and any similar customer-revenue query) benefits.
    std::vector<uint8_t> li_by_orderkey_returnflag;

    CsrIndex li_by_partkey;     // lineitem rows per partkey
    CsrIndex ps_by_partkey;     // partsupp rows per partkey
    CsrIndex ord_by_custkey;    // orders rows per custkey

    // CSR index: customer rows per 2-char phone country code (uint16 key).
    // cust_by_phone_cc.offsets[k] is the start index in row_ids for phone code k
    // (where k = c_phone_cc_u16 value).  Offsets array is sized to max_code + 2.
    // row_ids[] stores customer table row indices (int32_t) for all phone codes,
    // in contiguous blocks per code.
    //
    // Enables Q22 (and any query filtering by phone country code) to iterate
    // ONLY the matching ~840K customer rows (28% of 3M) instead of scanning
    // the full customer table.  Memory accessed reduces from 54 MB to ~11 MB.
    //
    // Companion array cust_by_pcc_bal[j]: c_acctbal value for row_ids[j].
    // Companion array cust_by_pcc_ho[j]: c_has_order flag for row_ids[j].
    // These allow fully sequential access during the Q22 aggregation loop
    // (no random gather into c_acctbal[] / c_has_order[] needed).
    //
    // Memory: offsets ~ 75 × 4B = 300B; row_ids = 3M × 4B = 12 MB;
    //         cust_by_pcc_bal = 3M × 8B = 24 MB; cust_by_pcc_ho = 3M × 1B = 3 MB.
    // General-purpose: any query filtering by c_phone country code benefits.
    CsrIndex             cust_by_phone_cc;      // customer rows per phone code key
    std::vector<double>  cust_by_pcc_bal;       // c_acctbal in CSR order
    std::vector<uint8_t> cust_by_pcc_ho;        // c_has_order in CSR order

    // Companion arrays for ps_by_partkey CSR (Q2 optimization).
    // For each slot j in ps_by_partkey.row_ids:
    //   ps_by_pk_suppnk[j]  = s_nationkey of the supplier for this partsupp row (uint8_t, 0-24)
    //   ps_by_pk_sr[j]      = row index in supplier table (int32_t, -1 if unknown)
    //   ps_by_pk_cost[j]    = ps_supplycost (double)
    // These companion arrays allow the Q2 inner loop to resolve supplier-nation
    // information with sequential access instead of scattered random reads through
    // suppkey → supp_row_by_suppkey → s_nationkey indirection.
    // General-purpose: any query joining partsupp with supplier via the partkey CSR
    // can use these companion arrays instead of chasing pointers.
    std::vector<uint8_t>  ps_by_pk_suppnk;  // s_nationkey for each CSR slot (0xFF = invalid)
    std::vector<int32_t>  ps_by_pk_sr;      // supplier row index for each CSR slot (-1 = invalid)
    std::vector<double>   ps_by_pk_cost;    // ps_supplycost for each CSR slot
    // AoS packed struct for ps_by_partkey companion data (Q2 hot inner loop).
    // Packs {suppnk, sr, cost} into a 16-byte struct so that a single cache line
    // covers 4 complete entries — exactly matching TPC-H's 4-suppliers-per-part.
    // The current SoA layout requires 3 separate cache lines (1 per array) for
    // each part's 4-entry slice; the AoS layout requires only 1 cache line.
    // Memory: 16M entries × 16B = 256MB at SF=20.
    // General-purpose: any query joining partsupp+supplier via ps_by_partkey CSR
    // that needs (nationkey, supplier_row, cost) benefits from this layout.
    struct PsByPkPacked {
        uint8_t  suppnk;   // s_nationkey (0xFF = invalid)
        uint8_t  _pad0;    // padding
        uint8_t  _pad1;    // padding
        uint8_t  _pad2;    // padding
        int32_t  sr;       // supplier row index (-1 = invalid)
        double   cost;     // ps_supplycost
    };
    static_assert(sizeof(PsByPkPacked) == 16, "PsByPkPacked must be 16 bytes");
    std::vector<PsByPkPacked> ps_by_pk_packed;  // AoS companion array

    // ps_by_pk_suppkey_i32[j] = ps_suppkey cast to int32 for ps_by_partkey CSR slot j.
    // Enables O(1) suppkey matching in the inner loop of Q9 without a linear search through
    // ps.ps_suppkey[row_ids[j]]. General-purpose: any query matching (partkey,suppkey) pairs
    // via the ps_by_partkey CSR can use this instead of ps.ps_suppkey[row_ids[j]].
    std::vector<int32_t>  ps_by_pk_suppkey_i32;  // ps_suppkey as int32 for each CSR slot

    // ── Companion arrays for li_by_partkey CSR (Q9 optimisation) ─────────────
    // For each slot j in li_by_partkey.row_ids, store pre-joined column values to
    // convert the scattered random-access reads in Q9's inner loop into sequential
    // streaming reads of compact companion arrays.
    //
    // li_by_pk_suppkey_i32[j] = l_suppkey as int32 for CSR slot j.
    //   Eliminates scattered reads into the 960MB l_suppkey[] array.
    // li_by_pk_year[j]        = year extracted from o_orderdate for CSR slot j.
    //   Eliminates both the orderkey→order-row lookup and the days_to_year() call.
    //   Stored as int16_t (fits year range 0..9999).
    // li_by_pk_ep[j]          = l_extendedprice for CSR slot j.
    //   Eliminates scattered reads into the 960MB l_extendedprice[] array.
    // li_by_pk_disc[j]        = l_discount for CSR slot j.
    //   Eliminates scattered reads into the 960MB l_discount[] array.
    // li_by_pk_qty[j]         = l_quantity for CSR slot j.
    //   Eliminates scattered reads into the 960MB l_quantity[] array.
    //
    // Memory: 120M rows × (4+2+8+8+8) = 30 bytes ≈ 3.6 GB at SF=20.
    // (SF=2: 12M × 30 = 360 MB)
    // General-purpose: any query iterating lineitem rows via partkey CSR and needing
    // these columns can use these arrays instead of chasing row_ids[] indirection.
    std::vector<int32_t> li_by_pk_suppkey_i32;  // l_suppkey as int32 for each CSR slot
    std::vector<int16_t> li_by_pk_year;          // order year for each CSR slot
    std::vector<float>   li_by_pk_ep;            // l_extendedprice for each CSR slot
    std::vector<float>   li_by_pk_disc;          // l_discount for each CSR slot
    std::vector<float>   li_by_pk_qty;           // l_quantity for each CSR slot

    // li_by_pk_cost[j]: pre-joined ps_supplycost for li_by_partkey CSR slot j.
    // For each lineitem slot j (with partkey pk and suppkey sk), this stores
    // partsupp.ps_supplycost where ps_partkey=pk AND ps_suppkey=sk.
    // Set to 0.0f when no matching partsupp row exists (should not happen in valid TPC-H).
    // Eliminates the per-slot linear scan over ps_by_partkey CSR in Q9's inner loop.
    // Memory: 120M rows × 4 bytes = 480 MB at SF=20.
    // General-purpose: any query needing ps_supplycost per lineitem row via partkey CSR.
    std::vector<float>   li_by_pk_cost;          // ps_supplycost for each CSR slot

    // li_by_pk_suppnk[j]: supplier nationkey for li_by_partkey CSR slot j.
    // = (li_by_pk_nation_u16[j] >> 8) & 0xFF — extracted for direct use.
    // 0xFF = unknown/invalid supplier nationkey.
    // Memory: 120M rows × 1 byte = 120 MB at SF=20.
    // General-purpose: any query needing supplier nationkey per lineitem via partkey CSR.
    std::vector<uint8_t> li_by_pk_suppnk;        // supplier nationkey for each CSR slot

    // li_by_pk_amount[j]: pre-computed profit amount for li_by_partkey CSR slot j.
    // = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
    // Stored as double (full precision, same as TPC-H reference).
    // Pre-computes the entire arithmetic expression at load time so Q9's inner loop
    // only needs to read {suppnk, yr_idx, amount} instead of {suppnk, yr, ep, disc, qty, cost}.
    // Reduces per-slot hot loop data from 6 values (19 bytes) to 3 values (11 bytes),
    // cutting memory bandwidth by ~2x for Q9.
    // Memory: 120M rows × 8 bytes = 960 MB at SF=20.
    // General-purpose: any query computing lineitem revenue minus supply cost can use this.
    std::vector<double>  li_by_pk_amount;         // pre-computed amount for each CSR slot

    // Packed Q9 slot: suppnk(8) + yr_idx(8) = 16-bit key for fast filtering.
    // li_by_pk_snk_yr[j] = (suppnk << 8) | yr_idx, where yr_idx = year - 1992 (0xFF if invalid).
    // Allows the Q9 hot loop to filter invalid slots with a single 16-bit load+compare.
    // Memory: 120M rows × 2 bytes = 240 MB at SF=20.
    std::vector<uint16_t> li_by_pk_snk_yr;        // packed (suppnk<<8)|yr_idx for each CSR slot

    // li_by_pk_nation_u16[j]: packed nationkey pair for li_by_partkey CSR slot j.
    // Layout mirrors li_suppnk_custnk_u16:
    //   high byte = s_nationkey of the lineitem's supplier (li_suppkey_nationkey_u8)
    //   low  byte = c_nationkey of the customer of the order (li_cust_nationkey_u8)
    // 0xFF in either byte = unknown/invalid.
    //
    // Enables Q8's inner hot loop to read supplier+customer nationkey with a single
    // sequential load instead of two scattered random accesses into the full-row
    // nationkey arrays.  Together with li_by_pk_year, li_by_pk_ep, li_by_pk_disc,
    // this makes Q8's per-slot work entirely sequential (zero random accesses).
    //
    // Memory: 120M rows × 2 bytes = 240 MB at SF=20.
    // General-purpose: any query iterating via li_by_partkey CSR that needs both
    // supplier and customer nationkey can use this instead of two indirect lookups.
    std::vector<uint16_t> li_by_pk_nation_u16;  // (suppnk<<8)|custnk in li_by_partkey CSR order

    // li_by_pk_sm_si[j]: packed (shipmode | (shipinstruct << 4)) for li_by_partkey CSR slot j.
    // Eliminates scattered reads into l_shipmode[] and l_shipinstruct[] (each 120MB at SF=20)
    // when iterating via the partkey CSR in Q19 (and any future query needing these columns).
    // shipmode occupies the low 4 bits (7 enum values fit in 4 bits), shipinstruct the high 4 bits
    // (4 enum values fit in 4 bits), so the whole thing fits in one uint8_t.
    //
    // Memory: 120M rows × 1 byte = 120 MB at SF=20.
    // General-purpose: any query iterating via li_by_partkey CSR that needs shipmode or
    // shipinstruct can use this instead of scattered l_shipmode[row_ids[j]] lookups.
    std::vector<uint8_t> li_by_pk_sm_si;  // packed (shipmode | shipinstruct<<4) in li_by_partkey CSR order

    // li_by_pk_shipdate_i16[j]: l_shipdate stored as int16_t offset from LI_PK_DATE_BASE
    // for li_by_partkey CSR slot j.
    // LI_PK_DATE_BASE = 8035 (approx 1992-01-01 in days-since-epoch encoding).
    // TPC-H shipdates span 1992-01-02 to 1998-12-01 ≈ 2556 days → fits in int16_t.
    // int16_t uses only 2 bytes vs. int32_t's 4 bytes → saves 240 MB at SF=20.
    // Enables Q20 (and any shipdate-range query via partkey CSR) to read shipdate
    // sequentially without scattering through row_ids into the 480MB l_shipdate array.
    // General-purpose: any query filtering on l_shipdate via the li_by_partkey CSR.
    static constexpr int32_t LI_PK_DATE_BASE = 8035;  // days since epoch for ~1992-01-01
    std::vector<int16_t> li_by_pk_shipdate_i16;  // (l_shipdate - LI_PK_DATE_BASE) in li_by_partkey CSR order

    // li_by_pk_qty_i8[j]: l_quantity as uint8_t for li_by_partkey CSR slot j.
    // TPC-H l_quantity in [1,50], fits in uint8_t.
    // 1 byte vs. 4 bytes for the existing float li_by_pk_qty — saves 360 MB at SF=20.
    // Enables Q20 (and any qty-accumulation query via partkey CSR) to read quantity
    // sequentially with minimal memory bandwidth.
    // Note: li_by_pk_qty (float) remains for Q17/Q19 which need float arithmetic.
    // General-purpose: any query accumulating integer quantity sums via partkey CSR.
    std::vector<uint8_t> li_by_pk_qty_i8;  // l_quantity as uint8 in li_by_partkey CSR order


    // General-purpose part index by p_size (p_size values 1..50 in TPC-H)
    CsrIndex part_by_size;      // part rows per p_size value

    // Companion arrays for part_by_size CSR (Q2 optimisation).
    // For each slot j in part_by_size CSR order (same as row_ids[j] = pi):
    //   psz_type_enum[j]  = p_type_enum[pi]   — converts random→sequential scan
    //   psz_partkey_i32[j]= (int32_t)p_partkey[pi] — converts random→sequential scan
    // These turn the hot 16K-iteration type-filter scan from random access into
    // a cache-friendly sequential scan of compact arrays.
    // General-purpose: any query filtering parts by size and needing type/partkey
    // can use these instead of indirect part-table access via sz_csr.row_ids[j].
    std::vector<uint8_t>  psz_type_enum;   // p_type_enum in part_by_size CSR order
    std::vector<int32_t>  psz_partkey_i32; // p_partkey as int32 in part_by_size CSR order
    // psz_brand[j] = p_brand[pi] in part_by_size CSR order.
    // Converts random access into p_brand[] (4MB) to sequential scan.
    // General-purpose: any query filtering by size+brand (e.g. Q16, Q19) can use this
    // instead of indirect p_brand[row_ids[j]] random access.
    std::vector<uint8_t>  psz_brand;       // p_brand in part_by_size CSR order
    // Pre-joined ps_by_partkey CSR slice bounds for each part_by_size slot.
    // psz_ps_start[j] = ps_by_partkey.offsets[p_partkey[part_row]]
    // psz_ps_end[j]   = ps_by_partkey.offsets[p_partkey[part_row] + 1]
    // Eliminates random access into the 16MB ps_by_partkey.offsets array during
    // Q2's main loop (500 random reads → sequential stream).
    // General-purpose: any query joining part+partsupp via part_by_size can use these.
    std::vector<int32_t>  psz_ps_start;    // ps_by_partkey start offset for each slot
    std::vector<int32_t>  psz_ps_end;      // ps_by_partkey end offset for each slot

    // General-purpose part index by (p_brand * 256 + p_container) composite key
    CsrIndex part_by_brand_container;   // part rows per brand×container combination

    // ── ps_by_suppkey CSR + companion arrays (Q11 optimisation) ──────────────
    //
    // Motivation: Q11 joins partsupp⋈supplier⋈nation, filtering on n_name='[NATION]'.
    // Only ~1/25 of suppkeys pass the nation filter (≈640K of 16M rows at SF=20).
    // The current approach scans all 16M rows (256 MB) to find the ~640K matching
    // ones — wasting ~96% of memory bandwidth.
    //
    // ps_by_suppkey lets Q11 iterate ONLY the rows for matching suppkeys, reading
    // ~5 MB instead of 256 MB — a ~50× reduction in memory traffic.
    //
    // Layout:
    //   ps_by_suppkey.offsets[sk]   → start index in ps_bysuppkey_partkey_i32[]
    //   ps_by_suppkey.offsets[sk+1] → end   index
    //   ps_bysuppkey_partkey_i32[j] → ps_partkey as int32 for slot j
    //   ps_bysuppkey_value_i64[j]   → ps_supplycost_cents * ps_availqty for slot j
    //                                  (per-row product, NOT pre-aggregated)
    //
    // Memory: offsets = 200K+1 × 4B ≈ 0.8 MB; row_ids (row indices) = 16M × 4B = 64 MB;
    //         slots (AoS) = 16M × 8B = 128 MB.
    //         Total new allocation ≈ 192 MB (64 MB row_ids + 128 MB slots).
    //
    // slot.value = cost_cents * availqty as int32_t (fits: max 100K*9999 = 999.9M < INT32_MAX).
    // Accumulators in query11.cpp use int64_t for summation safety.
    //
    // General-purpose: any query that needs to iterate partsupp rows for a given
    // suppkey set can use this CSR instead of a full partsupp scan.
    CsrIndex ps_by_suppkey;                        // partsupp rows per suppkey
    // (SoA arrays removed; use AoS ps_bysuppkey_slots instead for better cache efficiency)

    // AoS packed struct for Q11 hot loop: combines (partkey, value) into 8-byte record.
    // One cache line covers 8 complete entries instead of needing 2 separate cache lines.
    // Memory: 16M × 8B = 128 MB at SF=20.
    // General-purpose: any query needing (partkey, cost×qty) per suppkey-CSR slot.
    struct PsBySuppkeySlot {
        int32_t partkey;    // ps_partkey_i32
        int32_t value;      // cost_cents * availqty (fits int32)
    };
    static_assert(sizeof(PsBySuppkeySlot) == 8, "PsBySuppkeySlot must be 8 bytes");
    std::vector<PsBySuppkeySlot> ps_bysuppkey_slots; // AoS (partkey, value) in suppkey-CSR order
};

Database* build(ParquetTables*);
void destroy_database(Database*);