#include "query_impl.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "args_parser.hpp"
#include "cpu_affinity.hpp"
#include "query_q1a.hpp"
#include "query_q1a.cpp"
#include "query_q2a.hpp"
#include "query_q2a.cpp"
#include "query_q2b.hpp"
#include "query_q2b.cpp"
#include "query_q2c.hpp"
#include "query_q2c.cpp"
#include "query_q3a.hpp"
#include "query_q3a.cpp"
#include "query_q3b.hpp"
#include "query_q3b.cpp"
#include "query_q4a.hpp"
#include "query_q4a.cpp"
#include "query_q5a.hpp"
#include "query_q5a.cpp"
#include "query_q6a.hpp"
#include "query_q6a.cpp"
#include "query_q7a.hpp"
#include "query_q7a.cpp"
#include "query_q8a.hpp"
#include "query_q8a.cpp"
#include "query_q9a.hpp"
#include "query_q9a.cpp"
#include "query_q9b.hpp"
#include "query_q9b.cpp"
#include "query_q10a.hpp"
#include "query_q10a.cpp"
#include "query_q11a.hpp"
#include "query_q11a.cpp"
#include "query_q11b.hpp"
#include "query_q11b.cpp"

namespace {

struct CpuAffinityGuard {
    explicit CpuAffinityGuard(int cpu_id) { pin_process_to_cpu(cpu_id); }
    ~CpuAffinityGuard() { unpin_process_from_cpus(); }
};

void write_stub_result(size_t run_idx, const std::string& query_id) {
    std::ostringstream filename;
    filename << "result" << run_idx << ".csv";
    std::ofstream out(filename.str());
    out << "result\n";
    out << "0\n";
    (void)query_id;
}

void write_count_result(size_t run_idx, int64_t count) {
    std::ostringstream filename;
    filename << "result" << run_idx << ".csv";
    std::ofstream out(filename.str());
    out << "count_star()\n";
    out << count << "\n";
}

void write_csv_field(std::ofstream& out, const std::string& value) {
    bool needs_quote = false;
    for (char c : value) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r' || c == '\\') {
            needs_quote = true;
            break;
        }
    }
    if (!needs_quote) {
        out << value;
        return;
    }
    out << '"';
    for (char c : value) {
        if (c == '"' || c == '\\') {
            out << '\\';
        }
        out << c;
    }
    out << '"';
}

void write_q3b_result(size_t run_idx, const Database& db,
                      const std::vector<Q3bResultRow>& rows) {
    std::ostringstream filename;
    filename << "result" << run_idx << ".csv";
    std::ofstream out(filename.str());
    out << "title,name,name_1,count_star()\n";
    for (const auto& row : rows) {
        write_csv_field(out, db.string_pool.get(row.title_id));
        out << ",";
        write_csv_field(out, db.string_pool.get(row.person_name_id));
        out << ",";
        write_csv_field(out, db.string_pool.get(row.company_name_id));
        out << ",";
        out << row.count;
        out << "\n";
    }
}

void write_q9a_result(size_t run_idx, const Database& db,
                      const std::vector<Q9aResultRow>& rows) {
    std::ostringstream filename;
    filename << "result" << run_idx << ".csv";
    std::ofstream out(filename.str());
    out << "info,info_1,count_star()\n";
    for (const auto& row : rows) {
        write_csv_field(out, db.string_pool.get(row.movie_info_id));
        out << ",";
        write_csv_field(out, db.string_pool.get(row.person_info_id));
        out << ",";
        out << row.count;
        out << "\n";
    }
}

void write_q9b_result(size_t run_idx, const Database& db,
                      const std::vector<Q9bResultRow>& rows) {
    std::ostringstream filename;
    filename << "result" << run_idx << ".csv";
    std::ofstream out(filename.str());
    out << "info,name,count_star()\n";
    for (const auto& row : rows) {
        write_csv_field(out, db.string_pool.get(row.movie_info_id));
        out << ",";
        write_csv_field(out, db.string_pool.get(row.person_name_id));
        out << ",";
        out << row.count;
        out << "\n";
    }
}

void write_q10a_result(size_t run_idx, const Database& db,
                       const std::vector<Q10aResultRow>& rows) {
    std::ostringstream filename;
    filename << "result" << run_idx << ".csv";
    std::ofstream out(filename.str());
    out << "name,info,min(t.production_year),max(t.production_year)\n";
    for (const auto& row : rows) {
        write_csv_field(out, db.string_pool.get(row.person_name_id));
        out << ",";
        write_csv_field(out, db.string_pool.get(row.movie_info_id));
        out << ",";
        if (row.has_year) {
            out << row.min_year;
        }
        out << ",";
        if (row.has_year) {
            out << row.max_year;
        }
        out << "\n";
    }
}

void write_q11a_result(size_t run_idx, const Database& db,
                       const std::vector<Q11aResultRow>& rows) {
    std::ostringstream filename;
    filename << "result" << run_idx << ".csv";
    std::ofstream out(filename.str());
    out << "gender,role,name,count_star()\n";
    for (const auto& row : rows) {
        if (row.gender_id >= 0) {
            write_csv_field(out, db.string_pool.get(row.gender_id));
        }
        out << ",";
        write_csv_field(out, db.string_pool.get(row.role_id));
        out << ",";
        write_csv_field(out, db.string_pool.get(row.company_name_id));
        out << ",";
        out << row.count;
        out << "\n";
    }
}

void write_q11b_result(size_t run_idx, const Database& db,
                       const std::vector<Q11bResultRow>& rows) {
    std::ostringstream filename;
    filename << "result" << run_idx << ".csv";
    std::ofstream out(filename.str());
    out << "gender,role,name,count_star()\n";
    for (const auto& row : rows) {
        if (row.gender_id >= 0) {
            write_csv_field(out, db.string_pool.get(row.gender_id));
        }
        out << ",";
        write_csv_field(out, db.string_pool.get(row.role_id));
        out << ",";
        write_csv_field(out, db.string_pool.get(row.company_name_id));
        out << ",";
        out << row.count;
        out << "\n";
    }
}

void parse_request_stub(const QueryRequest& req) {
    if (req.id == "1a") {
        (void)parse_q1a(req);
    } else if (req.id == "2a") {
        (void)parse_q2a(req);
    } else if (req.id == "2b") {
        (void)parse_q2b(req);
    } else if (req.id == "2c") {
        (void)parse_q2c(req);
    } else if (req.id == "3a") {
        (void)parse_q3a(req);
    } else if (req.id == "3b") {
        (void)parse_q3b(req);
    } else if (req.id == "4a") {
        (void)parse_q4a(req);
    } else if (req.id == "5a") {
        (void)parse_q5a(req);
    } else if (req.id == "6a") {
        (void)parse_q6a(req);
    } else if (req.id == "7a") {
        (void)parse_q7a(req);
    } else if (req.id == "8a") {
        (void)parse_q8a(req);
    } else if (req.id == "9a") {
        (void)parse_q9a(req);
    } else if (req.id == "9b") {
        (void)parse_q9b(req);
    } else if (req.id == "10a") {
        (void)parse_q10a(req);
    } else if (req.id == "11a") {
        (void)parse_q11a(req);
    } else if (req.id == "11b") {
        (void)parse_q11b(req);
    } else {
        throw std::runtime_error("Unsupported query id: " + req.id);
    }
}

}  // namespace

void query(Database* db) {
    CpuAffinityGuard affinity_guard(3);
    std::vector<QueryRequest> requests;
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) {
            break;
        }
        std::istringstream iss(line);
        std::string query_id = "0";
        iss >> query_id;
        if (!iss) {
            continue;
        }
        requests.push_back(QueryRequest{query_id, line});
    }

    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& req = requests[i];
        if (req.id == "1a") {
            const auto args = parse_q1a(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q1a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "2a") {
            const auto args = parse_q2a(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q2a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "2b") {
            const auto args = parse_q2b(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q2b(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "2c") {
            const auto args = parse_q2c(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q2c(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "3a") {
            const auto args = parse_q3a(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q3a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "3b") {
            const auto args = parse_q3b(req);
            const auto start = std::chrono::steady_clock::now();
            const auto rows = run_q3b(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_q3b_result(i + 1, *db, rows);
            continue;
        }
        if (req.id == "4a") {
            const auto args = parse_q4a(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q4a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "5a") {
            const auto args = parse_q5a(req);
            prepare_q5a(*db);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q5a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "6a") {
            const auto args = parse_q6a(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q6a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "7a") {
            const auto args = parse_q7a(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q7a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "8a") {
            const auto args = parse_q8a(req);
            const auto start = std::chrono::steady_clock::now();
            const int64_t count = run_q8a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_count_result(i + 1, count);
            continue;
        }
        if (req.id == "9a") {
            const auto args = parse_q9a(req);
            const auto start = std::chrono::steady_clock::now();
            const auto rows = run_q9a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_q9a_result(i + 1, *db, rows);
            continue;
        }
        if (req.id == "9b") {
            const auto args = parse_q9b(req);
            const auto start = std::chrono::steady_clock::now();
            const auto rows = run_q9b(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_q9b_result(i + 1, *db, rows);
            continue;
        }
        if (req.id == "10a") {
            const auto args = parse_q10a(req);
            const auto start = std::chrono::steady_clock::now();
            const auto rows = run_q10a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_q10a_result(i + 1, *db, rows);
            continue;
        }
        if (req.id == "11a") {
            const auto args = parse_q11a(req);
            const auto start = std::chrono::steady_clock::now();
            const auto rows = run_q11a(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_q11a_result(i + 1, *db, rows);
            continue;
        }
        if (req.id == "11b") {
            const auto args = parse_q11b(req);
            const auto start = std::chrono::steady_clock::now();
            const auto rows = run_q11b(*db, args);
            const auto end = std::chrono::steady_clock::now();
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
            write_q11b_result(i + 1, *db, rows);
            continue;
        }
        const auto start = std::chrono::steady_clock::now();
        parse_request_stub(req);
        const auto end = std::chrono::steady_clock::now();
        const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        std::cout << (i + 1) << " | Execution ms: " << elapsed_ms << "\n";
        write_stub_result(i + 1, req.id);
    }
}


// Example code for how to use the parse functions together:
//for (const auto& req : requests) {
//    switch (req.id) {
//        case "1a": {
//            Q1aArgs args = parse_q1a(req); 
//            run_q1a(db, args);
//            break;
//        }
//        case "2a": {
//            Q2aArgs args = parse_q2a(req); 
//            run_q2a(db, args);
//            break;
//        }
//        ...
//        case "11b": {
//            Q11bArgs args = parse_q11b(req); 
//            run_q11b(db, args);
//            break;
//        }
//    }
//}
