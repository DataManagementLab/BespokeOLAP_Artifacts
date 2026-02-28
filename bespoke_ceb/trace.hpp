#pragma once

#include <cstdint>

#ifdef TRACE
#include <array>
#include <chrono>
#include <fstream>

inline uint64_t get_time_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct TraceEntry {
    const char* name;
    uint64_t value;
};

class TraceRecorder {
public:
    void record_timing(const char* name, uint64_t ns) {
        timings[timing_count++] = TraceEntry{name, ns};
    }

    void dump_timings() const {
        std::ofstream& out = trace_output_stream();
        for (size_t i = 0; i < timing_count; ++i) {
            out << "PROFILE " << timings[i].name << " " << timings[i].value << "\n";
        }
        out.flush();
    }

    static std::ofstream& trace_output_stream() {
        static std::ofstream out("tracing_output.log", std::ios::app);
        return out;
    }

private:
    std::array<TraceEntry, 128> timings{};
    size_t timing_count = 0;
};

class ScopedTimer {
public:
    ScopedTimer(TraceRecorder* recorder, const char* name)
        : recorder(recorder), name(name), start_ns(get_time_ns()) {}
    ~ScopedTimer() { recorder->record_timing(name, get_time_ns() - start_ns); }

private:
    TraceRecorder* recorder;
    const char* name;
    uint64_t start_ns;
};

inline void print_count(const char* name, uint64_t value) {
    std::ofstream& out = TraceRecorder::trace_output_stream();
    out << "COUNT " << name << " " << value << "\n";
    out.flush();
}

#define PROFILE_SCOPE(recorder, name) ScopedTimer scoped_timer_##__LINE__(recorder, name)
#else
inline void print_count(const char*, uint64_t) {}
#define PROFILE_SCOPE(recorder, name) ((void)0)
#endif