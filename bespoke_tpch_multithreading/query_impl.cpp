#include "query_impl.hpp"
#include "thread_pool.hpp"
#include "trace.hpp"



#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

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
#include "query1.hpp"
#include "query2.hpp"
#include "query3.hpp"
#include "query4.hpp"
#include "query5.hpp"
#include "query6.hpp"
#include "query7.hpp"
#include "query8.hpp"
#include "query9.hpp"
#include "query10.hpp"
#include "query11.hpp"
#include "query12.hpp"
#include "query13.hpp"
#include "query14.hpp"
#include "query15.hpp"
#include "query16.hpp"
#include "query17.hpp"
#include "query18.hpp"
#include "query19.hpp"
#include "query20.hpp"
#include "query21.hpp"
#include "query22.hpp"


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
    try{
        // call query implementations
        for (std::size_t i = 0; i < requests.size(); ++i) {
            const auto& req = requests[i];
                        // TRACE_RESET();
            long long elapsed_ms = -1;

            if (req.id == "1") {
                Q1Args args = parse_q1(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q1(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "2") {
                Q2Args args = parse_q2(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q2(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "3") {
                Q3Args args = parse_q3(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q3(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "4") {
                Q4Args args = parse_q4(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q4(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "5") {
                Q5Args args = parse_q5(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q5(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "6") {
                Q6Args args = parse_q6(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q6(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "7") {
                Q7Args args = parse_q7(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q7(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "8") {
                Q8Args args = parse_q8(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q8(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "9") {
                Q9Args args = parse_q9(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q9(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "10") {
                Q10Args args = parse_q10(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q10(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "11") {
                Q11Args args = parse_q11(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q11(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "12") {
                Q12Args args = parse_q12(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q12(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "13") {
                Q13Args args = parse_q13(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q13(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "14") {
                Q14Args args = parse_q14(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q14(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "15") {
                Q15Args args = parse_q15(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q15(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "16") {
                Q16Args args = parse_q16(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q16(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "17") {
                Q17Args args = parse_q17(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q17(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "18") {
                Q18Args args = parse_q18(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q18(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "19") {
                Q19Args args = parse_q19(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q19(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "20") {
                Q20Args args = parse_q20(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q20(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "21") {
                Q21Args args = parse_q21(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q21(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else if (req.id == "22") {
                Q22Args args = parse_q22(req);
                std::vector<std::vector<std::string>> rows;
                auto start = std::chrono::steady_clock::now();
                rows = run_q22(db, args);
                auto end = std::chrono::steady_clock::now();
                elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                const std::string filename = "result" + std::to_string(i + 1) + ".csv";
                write_csv(filename, rows);
            }

            else {
                throw std::runtime_error("Unsupported query id: " + req.id);
            }
                // TRACE_FLUSH();
            results.push_back({"", elapsed_ms});

        }
    } catch (...) {
        throw;
    }
    return results;
}