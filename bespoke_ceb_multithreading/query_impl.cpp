#include "query_impl.hpp"
#include "cpu_affinity.hpp"
#include "thread_pool.hpp"
#include "trace.hpp"

// Increment file version to invalidate cache when this file is changed. This is needed because this file is included in the generated code and changes to it should trigger regeneration of all code that includes it.
// FILE_VERSION: 6


#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>
#include <cstdlib>

// ── Shared thread pool (get_query_pool) ───────────────────────────────────────
// Initialized once at program start; warm-up dispatches a no-op parallel_for
// so all worker threads are alive and spinning before the first query arrives.
ThreadPool& get_query_pool() {
    static struct Holder {
        ThreadPool pool;
        Holder() {
            PROFILE_SCOPE("thread_pool_init");
            init_thread_pool(pool);
            pool.parallel_for([](int, int) {});  // warm-up
        }
    } h;
    return h.pool;
}

#include "args_parser.hpp"
#include "query1a.hpp"
#include "query2a.hpp"
#include "query2b.hpp"
#include "query2c.hpp"
#include "query3a.hpp"
#include "query3b.hpp"
#include "query4a.hpp"
#include "query5a.hpp"
#include "query6a.hpp"
#include "query7a.hpp"
#include "query8a.hpp"
#include "query9a.hpp"
#include "query9b.hpp"
#include "query10a.hpp"
#include "query11a.hpp"
#include "query11b.hpp"


void write_csv(const std::string& filename, const std::vector<std::vector<std::string>>& rows) {
    std::filesystem::create_directories("results");
    std::ofstream out("results/" + filename);
    for (const auto& row : rows) {
        for (std::size_t i = 0; i < row.size(); ++i) {
            if (i) out << ',';
            out << '"';
            for (char c : row[i]) {
                if (c == '"' || c == '\\') out << '\\';
                out << c;
            }
            out << '"';
        }
        out << '\n';
    }
}

void drop_buffer_and_os_caches(Database* db) {
    // clear the buffer pool
    	db->pool->clear();
    sync();  // flush dirty pages before drop, otherwise kernel may skip or do partial drop

    // try direct write first (works if running as root)
    {
        std::ofstream out("/proc/sys/vm/drop_caches");
        if (out) {
            out << "3\n";
            out.close();
            if (!out.fail()) return;
        }
    }

    // fall back to sudo tee
    int rc = std::system("echo 3 | sudo -n tee /proc/sys/vm/drop_caches > /dev/null 2>&1");
    if (rc != 0) {
        throw std::runtime_error(
            "drop_buffer_and_os_caches: failed to drop caches (not root and sudo -n tee failed). "
            "Add to sudoers: 'youruser ALL=(ALL) NOPASSWD: /usr/bin/tee /proc/sys/vm/drop_caches'"
        );
    }
}

std::vector<QueryResult> query(Database* db, const std::vector<std::string>& query_lines) {
    std::vector<QueryResult> results;
    std::vector<QueryRequest> requests;
    for (const auto& line : query_lines) {
        std::istringstream iss(line);
        std::string query_id =  "0";
        iss >> query_id;
        if (!iss) {
            continue;
        }
        requests.push_back(QueryRequest{query_id, line});
    }

    

    // Call query implementations
    for (std::size_t i = 0; i < requests.size(); ++i) {
        const auto& req = requests[i];
        			drop_buffer_and_os_caches(db);
        TRACE_RESET();
        long long elapsed_ms = -1;
        std::string error;
        const std::string prefix =
            "run #" + std::to_string(i + 1) + " Q" + req.id + ": ";
        try {
            if (req.id == "1a") {
                Q1aArgs args = parse_q1a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q1a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "2a") {
                Q2aArgs args = parse_q2a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q2a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "2b") {
                Q2bArgs args = parse_q2b(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q2b(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "2c") {
                Q2cArgs args = parse_q2c(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q2c(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "3a") {
                Q3aArgs args = parse_q3a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q3a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "3b") {
                Q3bArgs args = parse_q3b(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q3b(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "4a") {
                Q4aArgs args = parse_q4a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q4a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "5a") {
                Q5aArgs args = parse_q5a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q5a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "6a") {
                Q6aArgs args = parse_q6a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q6a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "7a") {
                Q7aArgs args = parse_q7a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q7a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "8a") {
                Q8aArgs args = parse_q8a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q8a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "9a") {
                Q9aArgs args = parse_q9a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q9a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "9b") {
                Q9bArgs args = parse_q9b(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q9b(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "10a") {
                Q10aArgs args = parse_q10a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q10a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "11a") {
                Q11aArgs args = parse_q11a(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q11a(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else if (req.id == "11b") {
                Q11bArgs args = parse_q11b(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q11b(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }
            else {
                throw std::runtime_error("Unsupported query id: " + req.id);
            }
        } catch (const std::exception& e) {
            error = prefix + e.what();
        } catch (...) {
            error = prefix + "unknown exception";
        }
        TRACE_FLUSH();
        results.push_back({trace_get_and_clear(), elapsed_ms, error});
    }
    return results;
}