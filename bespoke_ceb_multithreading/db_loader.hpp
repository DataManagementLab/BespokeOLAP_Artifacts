#pragma once

#include "parquet_reader.hpp"
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

// ---------------------------------------------------------------------------
// CSR (Compressed Sparse Row) side index
// For key k, rows are in values[offsets[k] .. offsets[k+1]).
// offsets has size max_key+2.
// ---------------------------------------------------------------------------
struct Csr {
    std::vector<int32_t> offsets; // size = max_key + 2
    std::vector<int32_t> values;  // row indices

    // Return [begin, end) row index range for key k.
    // Safe: returns empty range if k out of bounds.
    inline std::pair<int32_t,int32_t> range(int32_t k) const {
        if (k < 0 || k + 1 >= (int32_t)offsets.size()) return {0, 0};
        return {offsets[k], offsets[k+1]};
    }
    bool empty() const { return offsets.empty(); }
};

// ---------------------------------------------------------------------------
// Micro tables (< 200 rows each)
// ---------------------------------------------------------------------------
struct KindTypeTable {
    std::vector<int32_t>     id;
    std::vector<std::string> kind;
};

struct RoleTypeTable {
    std::vector<int32_t>     id;
    std::vector<std::string> role;
};

struct InfoTypeTable {
    std::vector<int32_t>     id;
    std::vector<std::string> info;
};

struct CompanyTypeTable {
    std::vector<int32_t>     id;
    std::vector<std::string> kind;
};

struct CompCastTypeTable {
    std::vector<int32_t>     id;
    std::vector<std::string> kind;
};

struct LinkTypeTable {
    std::vector<int32_t>     id;
    std::vector<std::string> link;
};

// ---------------------------------------------------------------------------
// title (~2.5 M rows)
// ---------------------------------------------------------------------------
struct TitleTable {
    std::vector<int32_t>     id;
    std::vector<std::string> title_str;       // "title" column
    std::vector<int32_t>     kind_id;         // -1 if null
    std::vector<int32_t>     production_year; // -1 if null
    // FK lookup: id -> row position
    std::vector<int32_t>     id_to_row;       // size = max_id+1, -1 = absent
    // Kind-id partition index: rows sorted by kind_id so we can scan only
    // the relevant kind partition without touching the full 5M-row table.
    // kind_part_start[k] / kind_part_end[k] give the [begin, end) row range
    // for kind_id == k.  Size = max_kind_id + 2.  General-purpose: allows
    // any query to restrict its title scan to specific kind values.
    std::vector<int32_t>     kind_part_start;
    std::vector<int32_t>     kind_part_end;
};

// ---------------------------------------------------------------------------
// cast_info (~36 M rows) — sorted by movie_id
// ---------------------------------------------------------------------------
struct CastInfoTable {
    std::vector<int32_t>     person_id;
    std::vector<int32_t>     movie_id;
    std::vector<int32_t>     person_role_id; // -1 if null
    std::vector<std::string> note;            // "" if null
    std::vector<int32_t>     role_id;         // -1 if null
    // Side structures
    Csr movie_id_csr;   // movie_id -> row indices (rows sorted by movie_id)
    Csr person_id_csr;  // person_id -> row indices (built unsorted)
};

// ---------------------------------------------------------------------------
// movie_info (~15 M rows) and movie_info_idx (~1.4 M rows)
// Both share this struct. Sorted by (info_type_id, movie_id).
// ---------------------------------------------------------------------------
#include <unordered_set>
struct MovieInfoTable {
    std::vector<int32_t>     movie_id;
    std::vector<int32_t>     info_type_id;
    std::vector<std::string> info_str;   // "info" column
    std::vector<std::string> note;       // "" if null
    std::vector<float>       info_float; // NaN if non-numeric; only for movie_info_idx
    // String interning: info_str[i] has interned integer ID info_id[i].
    // info_dict_map maps string -> intern ID; info_dict_vec[id] = string.
    // Allows O(1) integer-based lookups instead of string hash map lookups.
    std::vector<int32_t>     info_id;       // intern ID for each row's info_str
    std::vector<std::string> info_dict_vec; // intern ID -> string
    std::unordered_map<std::string, int32_t> info_dict_map; // string -> intern ID
    // Side structures
    Csr movie_id_csr;
    // Per info_type_id partition: [type_part_start[t], type_part_end[t])
    std::vector<int32_t> type_part_start; // size = max_info_type_id + 2
    std::vector<int32_t> type_part_end;
    // Per info_type_id: set of unique info_str values (for fast existence checks)
    // type_unique_info[t] contains all distinct info values for info_type_id == t
    std::vector<std::unordered_set<std::string>> type_unique_info;
    // Cache-friendly flat vector version of type_unique_info for iteration
    std::vector<std::vector<std::string>> type_unique_info_vec;
    // Per (type, intern_id): flat inverted index of row indices.
    // type_iid_inv[t] is a Csr where:
    //   keys are "local IID index" (position in type_iid_keys[t])
    //   type_iid_keys[t] is sorted list of intern IDs present in type t
    //   type_iid_offsets[t][i] = start of movie_id list for type_iid_keys[t][i]
    //   type_iid_rows[t] = flat sorted list of movie_ids (NOT row indices) for each intern ID
    //     Sorted: for a given intern ID, movie_ids are in ascending order.
    // Allows O(log N_distinct) lookup + sequential scan of movie_ids for a (type, intern_id).
    std::vector<std::vector<int32_t>> type_iid_keys;    // per type: sorted intern IDs
    std::vector<std::vector<int32_t>> type_iid_offsets; // per type: offsets into type_iid_rows
    std::vector<std::vector<int32_t>> type_iid_rows;    // per type: flat sorted movie_id list
};

// ---------------------------------------------------------------------------
// movie_keyword (~5 M rows) — sorted by movie_id
// ---------------------------------------------------------------------------
struct MovieKeywordTable {
    std::vector<int32_t> movie_id;
    std::vector<int32_t> keyword_id;
    // Side structures
    Csr movie_id_csr;
    Csr keyword_id_csr;
};

// ---------------------------------------------------------------------------
// movie_companies (~2.6 M rows) — sorted by movie_id
// ---------------------------------------------------------------------------
struct MovieCompaniesTable {
    std::vector<int32_t>     movie_id;
    std::vector<int32_t>     company_id;
    std::vector<int32_t>     company_type_id; // -1 if null
    std::vector<std::string> note;             // "" if null
    // Side structures
    Csr movie_id_csr;
    Csr company_id_csr;  // company_id -> row indices (unsorted)
};

// ---------------------------------------------------------------------------
// name (~4.2 M rows)
// ---------------------------------------------------------------------------
struct NameTable {
    std::vector<int32_t>     id;
    std::vector<std::string> name_str;       // "name" column
    std::vector<std::string> gender;         // "" if null
    std::vector<std::string> name_pcode_cf;  // "" if null
    std::vector<std::string> name_pcode_nf;  // "" if null
    // Compact single-byte gender: first character of gender string, or 0 for empty.
    // Allows fast vectorizable scan over 4M+ rows without std::string overhead.
    std::vector<uint8_t>     gender_byte;    // 0=empty/'', 'f', 'm', etc.
    // FK lookup: id -> row position
    std::vector<int32_t>     id_to_row;      // size = max_id+1, -1 = absent
};

// ---------------------------------------------------------------------------
// person_info (~2.9 M rows) — sorted by (info_type_id, person_id)
// ---------------------------------------------------------------------------
struct PersonInfoTable {
    std::vector<int32_t>     person_id;
    std::vector<int32_t>     info_type_id;
    std::vector<std::string> info_str;
    std::vector<std::string> note; // "" if null
    // Side structures
    Csr person_id_csr;
    std::vector<int32_t> type_part_start;
    std::vector<int32_t> type_part_end;
};

// ---------------------------------------------------------------------------
// aka_name (~901 K rows) — sorted by person_id
// ---------------------------------------------------------------------------
struct AkaNameTable {
    std::vector<int32_t>     person_id;
    std::vector<std::string> name_str; // "" if null
    // Side structures
    Csr person_id_csr;
};

// ---------------------------------------------------------------------------
// keyword (~134 K rows)
// ---------------------------------------------------------------------------
struct KeywordTable {
    std::vector<int32_t>     id;
    std::vector<std::string> keyword_str;
    // FK lookup: id -> row position
    std::vector<int32_t>     id_to_row;
    // Reverse lookup: keyword_str -> id (for O(1) lookup by keyword name)
    // Avoids O(n) linear scan of all 134K keywords when filtering by name.
    // Stores ALL ids for a given keyword string (duplicates may exist at higher SF).
    std::unordered_map<std::string, std::vector<int32_t>> str_to_ids;
};

// ---------------------------------------------------------------------------
// company_name (~234 K rows)
// ---------------------------------------------------------------------------
struct CompanyNameTable {
    std::vector<int32_t>     id;
    std::vector<std::string> name_str;
    std::vector<std::string> country_code; // "" if null
    // Pre-lowercased company names: avoids per-character tolower() in ILIKE scans.
    // Same data as name_str but with all characters lowercased.
    // General-purpose: benefits any query doing case-insensitive matching on cn.name.
    std::vector<std::string> name_str_lower;
    // Compact country code: store as uint64 by copying up to 8 bytes directly.
    // This allows fast integer comparison in queries without string hashing.
    // 0 = null/empty. Same information as country_code, just in a scalar form.
    std::vector<uint64_t>    country_code_u64;
    // FK lookup: id -> row position
    std::vector<int32_t>     id_to_row;
    // Reverse lookup: name_str -> company id(s).
    // General-purpose: allows O(1) name->id lookup instead of O(N) scan.
    // Mirrors the str_to_ids pattern used in KeywordTable.
    // Maps exact company name -> list of matching ids (duplicates possible at high SF).
    std::unordered_map<std::string, std::vector<int32_t>> name_to_ids;
};

// ---------------------------------------------------------------------------
// Top-level Database struct
// ---------------------------------------------------------------------------
struct Database {
    TitleTable           title;
    CastInfoTable        cast_info;
    MovieInfoTable       movie_info;
    MovieInfoTable       movie_info_idx;
    MovieKeywordTable    movie_keyword;
    MovieCompaniesTable  movie_companies;
    NameTable            name;
    PersonInfoTable      person_info;
    AkaNameTable         aka_name;
    KeywordTable         keyword;
    CompanyNameTable     company_name;
    KindTypeTable        kind_type;
    RoleTypeTable        role_type;
    InfoTypeTable        info_type;
    CompanyTypeTable     company_type;
    CompCastTypeTable    comp_cast_type;
    LinkTypeTable        link_type;
};

Database* build(ParquetTables*);
void destroy_database(Database*);

// ---------------------------------------------------------------------------
// parse_in_list: parses a Python-style tuple of single-quoted strings from a
// stream.  The on-wire format produced by format_args_string() is:
//
//   ('val1', 'val2', 'val3')
//
// Single-quoted strings may contain any character except an unescaped single
// quote.  The entire tuple is consumed as one logical token; the stream is
// left positioned after the closing ')'.
//
// Returns the list of decoded string values (without surrounding quotes).
// An empty tuple "()" or a missing token yields an empty vector.
//
// Also handles the legacy double-quoted format ("val1" "val2" ...) so that
// existing tests continue to work.
// ---------------------------------------------------------------------------
#include <iomanip>
#include <sstream>

inline std::vector<std::string> parse_in_list(std::istringstream& iss) {
    std::vector<std::string> result;
    if (!(iss >> std::ws)) return result;

    int next = iss.peek();

    // -----------------------------------------------------------------------
    // New format: ('val1', 'val2', ...)
    // -----------------------------------------------------------------------
    if (next == '(') {
        iss.get(); // consume '('
        while (true) {
            if (!(iss >> std::ws)) break;
            int c = iss.peek();
            if (c == ')') { iss.get(); break; }  // end of tuple
            if (c == ',') { iss.get(); continue; } // separator
            if (c == '\'') {
                iss.get(); // consume opening quote
                std::string tok;
                // read until closing single-quote (no escape handling needed
                // for the values we expect)
                while (iss.good()) {
                    int ch = iss.get();
                    if (ch == EOF) break;
                    if (ch == '\'') break; // end of token
                    tok += (char)ch;
                }
                result.push_back(std::move(tok));
            } else {
                // Unexpected character — stop gracefully
                break;
            }
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // Legacy format: "val1" "val2" ...
    // -----------------------------------------------------------------------
    while (next == '"') {
        std::string tok;
        if (!(iss >> std::quoted(tok))) break;
        result.push_back(std::move(tok));
        if (!(iss >> std::ws)) break;
        next = iss.peek();
    }
    return result;
}
