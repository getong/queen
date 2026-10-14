/**
 * Queen C++ Client - binary push body and framed pop answer, wire tests
 *
 * WHAT THIS FILE IS FOR.
 * ClientConfig has three options for a broker that has the binary push body
 * and the framed pop answer: binary_push, push_compression and framed_pop. All
 * three are off by default. What is at stake:
 *
 *   1. THE DEFAULT. A client that sets none of them must send the JSON requests
 *      every released broker knows.
 *   2. THE BYTES. The binary push body and its framed form are read here field
 *      by field, as the broker reads them.
 *   3. THE SAME MESSAGES. A consumer that sets framed_pop must be handed
 *      exactly the messages the JSON answer gives it, with the answer's own
 *      keys (the conflation echo, the autopilot's choice) where they always
 *      were.
 *   4. THE ORDER OF A TRANSACTION'S RESULTS. With binary_push and
 *      push_compression a bundle that pushes goes out as the binary
 *      transaction body, and the broker lists the head's acks before the
 *      pushes: the caller must get the results back in the order of its own
 *      operations.
 *
 * push_compression and framed_pop need zstd: this file builds without it too
 * (the Makefile adds -DQUEEN_CLIENT_ZSTD when the library is installed) and
 * then skips the cases that need it.
 *
 * Like its siblings this runs against an in-process httplib::Server -- no
 * broker:
 *
 *   make wireforms && ./bin/test_wire_forms
 *
 * Siblings: test_autopilot.cpp, test_conflation.cpp, test_buffer.cpp.
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>
#include <set>
#include <string>
#include <functional>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define YELLOW "\033[33m"
#define RESET "\033[0m"

static const std::string TXN = "0190aaaa-0000-7000-8000-000000000001";
static const std::string TRACE = "0190aaaa-0000-4000-8000-000000000001";

// The JSON answer of two messages of partition 7 ("p7") of "orders", as the
// broker renders it.
static const std::string TWO_MESSAGES_JSON =
    R"({"success":true,"queue":"orders","partition":"p7","partitionId":"7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","messages":[)"
    R"({"id":"01000000-0000-7000-8000-000000000000","transactionId":"t-10","traceId":null,"data":{"n":10},"producerSub":null,"createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":10},)"
    R"({"id":"02000000-0000-7000-8000-000000000000","transactionId":"t-11","traceId":null,"data":{"n":11,"s":"x"},"producerSub":"svc","createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":11})"
    R"(],"partitionsClaimed":1})";

// ============================================================================
// Test double
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string target;        // raw request target, query string included
    std::string content_type;
    std::string body;          // as bytes: a binary push body is not text
    bool asked_frames = false; // the pop carried frames=true
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

inline Responder answer(const std::string& body, const std::string& content_type = "application/json") {
    return [body, content_type](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(body, content_type);
    };
}

inline Responder queued(size_t count = 1) {
    json items = json::array();
    for (size_t i = 0; i < count; ++i) items.push_back({{"status", "queued"}});
    return answer(items.dump());
}

inline Responder framed(const std::string& body) {
    return answer(body, wire::POP_FRAMES_CONTENT_TYPE);
}

inline Responder nothing() {
    return [](const httplib::Request&, httplib::Response& res) { res.status = 204; };
}

class StubServer {
private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    Responder responder_;

    mutable std::mutex mutex_;
    std::vector<RecordedCall> calls_;

    std::vector<RecordedCall> matching(const std::string& method, const std::string& prefix) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<RecordedCall> out;
        for (const auto& call : calls_) {
            if (call.method == method && call.target.rfind(prefix, 0) == 0) out.push_back(call);
        }
        return out;
    }

public:
    explicit StubServer(Responder responder) : responder_(std::move(responder)) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                calls_.push_back({req.method, req.target, req.get_header_value("Content-Type"), req.body,
                                  req.get_param_value("frames") == "true"});
            }
            responder_(req, res);
        };

        server_.Get(".*", handler);
        server_.Post(".*", handler);
        server_.Put(".*", handler);
        server_.Delete(".*", handler);

        port_ = server_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this]() { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    ~StubServer() {
        server_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    std::vector<RecordedCall> pushes() const { return matching("POST", "/api/v1/push"); }
    std::vector<RecordedCall> transactions() const { return matching("POST", "/api/v1/transaction"); }
    std::vector<RecordedCall> pops() const { return matching("GET", "/api/v1/pop"); }
};

/// Raises a stop signal after a budget, so a consume loop that fails to stop on
/// its own FAILS its test instead of hanging it.
class StopAfter {
private:
    std::atomic<bool> flag_{false};
    std::atomic<bool> done_{false};
    std::thread timer_;

public:
    explicit StopAfter(int budget_millis) {
        timer_ = std::thread([this, budget_millis]() {
            auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(budget_millis);
            while (!done_.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            flag_.store(true);
        });
    }

    ~StopAfter() {
        done_.store(true);
        if (timer_.joinable()) timer_.join();
    }

    std::atomic<bool>* signal() { return &flag_; }
};

// ============================================================================
// Assertions
// ============================================================================

static int failures = 0;
static int skipped = 0;

void check(bool condition, const std::string& what) {
    if (!condition) {
        ++failures;
        std::cout << RED << "    x " << what << RESET << std::endl;
    }
}

void run_test(const std::string& name, std::function<void()> test_fn) {
    std::cout << BLUE << "Running: " << name << RESET << std::endl;
    int before = failures;

    try {
        test_fn();
    } catch (const std::exception& e) {
        ++failures;
        std::cout << RED << "    x unexpected exception: " << e.what() << RESET << std::endl;
    }

    if (failures == before) {
        std::cout << GREEN << "PASS: " << name << RESET << std::endl;
    } else {
        std::cout << RED << "FAIL: " << name << RESET << std::endl;
    }
}

/// A test of push_compression or framed_pop: run where the build has zstd.
void run_zstd_test(const std::string& name, std::function<void()> test_fn) {
    if (wire::zstd_available()) {
        run_test(name, std::move(test_fn));
        return;
    }
    ++skipped;
    std::cout << YELLOW << "SKIP: " << name << " (built without zstd)" << RESET << std::endl;
}

ClientConfig config_with(bool binary_push, bool push_compression, bool framed_pop) {
    ClientConfig config;
    config.retry_attempts = 1;
    config.binary_push = binary_push;
    config.push_compression = push_compression;
    config.framed_pop = framed_pop;
    return config;
}

// ============================================================================
// Reading a binary push body
// ============================================================================

/// Reads the fields of a binary push body in order.
struct Reader {
    std::string data;
    size_t at = 0;

    explicit Reader(std::string bytes) : data(std::move(bytes)) {}

    std::string take(size_t n) {
        if (at + n > data.size()) throw std::runtime_error("a binary push body ends early");
        at += n;
        return data.substr(at - n, n);
    }

    size_t number(size_t width) {
        const std::string b = take(width);
        size_t value = 0;
        for (size_t i = 0; i < width; ++i) value |= static_cast<size_t>(static_cast<unsigned char>(b[i])) << (8 * i);
        return value;
    }

    size_t u16() { return number(2); }
    size_t u32() { return number(4); }
    std::string text16() { return take(u16()); }
    size_t left() const { return data.size() - at; }
};

struct PlainGroup {
    std::string queue;
    std::string partition;
    std::vector<std::pair<std::string, std::string>> items;  // (transaction id, payload)
};

/// The groups of a plain binary push body (after magic and flags), every item
/// with its transaction id.
std::vector<PlainGroup> plain_groups(const std::string& bytes) {
    Reader r(bytes);
    std::vector<PlainGroup> groups;
    for (size_t g = r.u32(); g > 0; --g) {
        PlainGroup group;
        group.queue = r.text16();
        group.partition = r.text16();
        for (size_t n = r.u32(); n > 0; --n) {
            std::string txn = r.text16();
            group.items.emplace_back(std::move(txn), r.take(r.u32()));
        }
        groups.push_back(std::move(group));
    }
    check(r.left() == 0, "bytes left after the last payload");
    return groups;
}

struct StoredFrame {
    int flags = 0;
    std::string id;
    std::string txn;
    std::string payload;
};

struct FramedGroup {
    std::string queue;
    std::string partition;
    size_t count = 0;
    std::vector<StoredFrame> frames;
};

/// The groups of a framed binary push body, each with its frames unpacked.
std::vector<FramedGroup> framed_groups(const std::string& bytes) {
    Reader r(bytes);
    std::vector<FramedGroup> groups;
    for (size_t g = r.u32(); g > 0; --g) {
        FramedGroup group;
        group.queue = r.text16();
        group.partition = r.text16();
        group.count = r.u32();
        Reader f(wire::zstd_decompress(r.take(r.u32())));
        while (f.left() > 0) {
            Reader body(f.take(f.u32()));
            StoredFrame frame;
            frame.flags = static_cast<unsigned char>(body.take(1)[0]);
            frame.id = body.take(16);
            frame.txn = body.text16();
            frame.payload = body.take(body.left());
            group.frames.push_back(std::move(frame));
        }
        groups.push_back(std::move(group));
    }
    check(r.left() == 0, "bytes left after the last group");
    return groups;
}

// ============================================================================
// Push
// ============================================================================

void test_a_push_is_json_by_default() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with(false, false, false));

    json result = client.queue("orders").partition("p1").push({{{"data", {{"a", 1}}}}});

    check(result[0]["status"] == "queued", "the push answered " + result.dump());
    const auto sent = server.pushes();
    check(sent.size() == 1, "one push request");
    check(sent[0].content_type == "application/json", "content type " + sent[0].content_type);
    json body = json::parse(sent[0].body);
    check(body.size() == 1 && body.contains("items"), "the body is {items}");
    check(body["items"][0]["payload"] == json({{"a", 1}}), "the payload travels as JSON");
}

void test_binary_push_sends_the_binary_body_with_each_transaction_id() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with(true, false, false));

    json result = client.queue("orders").partition("p1")
        .push({{{"data", {{"a", 1}}}, {"transactionId", TXN}}});

    check(result[0]["status"] == "queued", "the push answered " + result.dump());
    const auto sent = server.pushes();
    check(sent.size() == 1, "one push request");
    check(sent[0].content_type == "application/x-queen-push", "content type " + sent[0].content_type);
    check(sent[0].body.substr(0, 5) == std::string("QPB1\x04", 5), "a plain binary push body with transaction ids");
    const auto groups = plain_groups(sent[0].body.substr(5));
    check(groups.size() == 1 && groups[0].queue == "orders" && groups[0].partition == "p1",
          "one group for orders/p1");
    check(groups[0].items.size() == 1 && groups[0].items[0].first == TXN &&
              groups[0].items[0].second == R"({"a":1})",
          "the item with its transaction id and payload");
}

void test_consecutive_items_of_one_partition_share_a_group() {
    auto item = [](const std::string& partition, int payload) {
        return json{{"queue", "q"}, {"partition", partition}, {"payload", payload},
                    {"transactionId", "t" + std::to_string(payload)}};
    };
    const auto bytes = wire::binary_push_body(
        json::array({item("a", 1), item("a", 2), item("b", 3), item("a", 4)}), false);

    check(bytes.has_value() && bytes->substr(0, 5) == std::string("QPB1\x04", 5), "a plain binary push body");
    const auto groups = plain_groups(bytes->substr(5));
    check(groups.size() == 3, "three groups, in the order of the items");
    check(groups[0].partition == "a" && groups[0].items.size() == 2 &&
              groups[0].items[0].first == "t1" && groups[0].items[1].second == "2",
          "a: t1, t2");
    check(groups[1].partition == "b" && groups[1].items.size() == 1 && groups[1].items[0].first == "t3", "b: t3");
    check(groups[2].partition == "a" && groups[2].items.size() == 1 && groups[2].items[0].first == "t4", "a: t4");
}

void test_binary_push_carries_payloads_that_are_not_objects_and_non_ascii_text() {
    StubServer server(queued(5));
    QueenClient client({server.url()}, config_with(true, false, false));
    const std::vector<json> payloads = {
        "testo \"con\" virgolette\n", json::array({1, 2}), 7, nullptr, json{{"è", "€"}}};
    std::vector<json> items;
    for (const auto& payload : payloads) items.push_back({{"data", payload}});

    client.queue("ordini").partition("città").push(items);

    const auto groups = plain_groups(server.pushes()[0].body.substr(5));
    check(groups.size() == 1 && groups[0].partition == "città", "one group, its name in UTF-8");
    check(groups[0].items.size() == payloads.size(), "every payload travelled");
    for (size_t i = 0; i < payloads.size() && i < groups[0].items.size(); ++i) {
        check(json::parse(groups[0].items[i].second) == payloads[i], "payload " + std::to_string(i));
        check(groups[0].items[i].first.size() == 36, "the client mints a transaction id");
    }
}

void test_an_item_with_a_trace_id_stays_json() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with(true, false, false));

    client.queue("orders").push({{{"data", {{"a", 1}}}, {"traceId", TRACE}}});

    const auto sent = server.pushes();
    check(sent[0].content_type == "application/json", "an item with a trace id travels as JSON");
    check(json::parse(sent[0].body)["items"][0]["traceId"] == TRACE, "with its trace id");
}

void test_an_item_the_binary_body_does_not_carry_leaves_the_push_to_json() {
    // The binary body is not built; the JSON request then answers as it always did.
    check(!wire::binary_push_body(json::array({{{"queue", "q"}, {"partition", std::string(70000, 'p')}, {"payload", 1}}}), false),
          "a name over 65535 bytes");
    check(!wire::binary_push_body(json::array({{{"queue", "q"}, {"partition", "p"}, {"payload", 1}, {"transactionId", 5}}}), false),
          "a transaction id that is not a string");
    check(!wire::binary_push_body(json::array({{{"queue", "q"}, {"partition", "p"}}}), false), "no payload");
    check(!wire::binary_push_body(json::array({{{"queue", "q"}, {"partition", "p"}, {"payload", std::string("\xff\xfe")}}}), false),
          "a payload that is not UTF-8");
    check(!wire::binary_push_body(json::array(), false), "no items");
}

void test_a_buffered_push_is_binary_too() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with(true, false, false));
    BufferOptions options;
    options.message_count = 10;
    options.time_millis = 60000;

    client.queue("orders").partition("p1").buffer(options).push({{{"data", {{"a", 1}}}}});
    client.flush_all_buffers();

    const auto sent = server.pushes();
    check(sent.size() == 1, "one push request");
    check(sent[0].content_type == "application/x-queen-push", "content type " + sent[0].content_type);
    check(sent[0].body.substr(0, 5) == std::string("QPB1\x04", 5), "a plain binary push body");
    check(plain_groups(sent[0].body.substr(5))[0].items[0].second == R"({"a":1})", "the payload");
}

void test_the_counts_flag_and_mismatched_transaction_ids() {
    const std::vector<wire::PushBatch> batches = {{"q", "p", {"1"}, std::nullopt}};
    check(wire::encode_push_binary(batches, false, true).substr(0, 5) == std::string("QPB1\x02", 5), "flag 2 asks for the counts");
    check(wire::encode_push_binary(batches).substr(0, 5) == std::string("QPB1\x00", 5), "no flags by default");

    bool refused = false;
    try {
        wire::encode_push_binary({{"q", "p", {"1", "2"}, std::vector<std::string>{"t"}}});
    } catch (const std::invalid_argument& e) {
        refused = std::string(e.what()) == "1 transaction ids for 2 payloads";
    }
    check(refused, "transaction ids must match the payloads");
}

void test_the_zstd_flag_compresses_the_whole_body() {
    const std::vector<wire::PushBatch> batches = {{"q", "p", {"1"}, std::nullopt}};
    const std::string plain = wire::encode_push_binary(batches);
    const std::string packed = wire::encode_push_binary(batches, true);

    check(packed.substr(0, 5) == std::string("QPB1\x01", 5), "flag 1 says the body is one zstd frame");
    check(Reader(packed.substr(5, 4)).u32() == plain.size() - 5, "its raw length in front");
    check(wire::zstd_decompress(packed.substr(9)) == plain.substr(5), "the frame is the plain body");
    check(static_cast<unsigned char>(wire::encode_push_frames(batches, true)[4]) == (8 | 2), "frames with the counts answer");
}

void test_push_compression_sends_each_partition_as_one_zstd_frame_of_stored_frames() {
    StubServer server(queued(3));
    QueenClient client({server.url()}, config_with(true, true, false));

    client.queue("orders").partition("p1").push({
        {{"data", {{"a", 1}}}, {"transactionId", TXN}},
        {{"data", {{"a", 2}}}},
        {{"data", "x"}}});

    const auto sent = server.pushes();
    check(sent[0].content_type == "application/x-queen-push", "content type " + sent[0].content_type);
    check(sent[0].body.substr(0, 5) == std::string("QPB1\x08", 5), "a framed binary push body");
    const auto groups = framed_groups(sent[0].body.substr(5));
    check(groups.size() == 1 && groups[0].queue == "orders" && groups[0].partition == "p1" && groups[0].count == 3,
          "one group of three for orders/p1");
    const auto& frames = groups[0].frames;
    check(frames.size() == 3, "three frames");
    if (frames.size() != 3) return;
    check(frames[0].payload == R"({"a":1})" && frames[1].payload == R"({"a":2})" && frames[2].payload == R"("x")",
          "the payloads, in order");
    check(frames[0].txn == TXN, "a message keeps its own transaction id");
    std::set<std::string> ids;
    for (const auto& frame : frames) {
        check(frame.flags == 0, "no frame flags");
        check((static_cast<unsigned char>(frame.id[6]) >> 4) == 7, "the message id is a version-7 UUID");
        check((static_cast<unsigned char>(frame.id[8]) & 0xC0) == 0x80, "with the RFC variant");
        check(frame.txn.size() == 36, "a transaction id of 36 characters");
        ids.insert(frame.id);
    }
    check(ids.size() == 3, "three distinct message ids");
}

void test_a_frame_without_a_transaction_id_gets_the_text_of_its_id() {
    const std::string bytes = wire::encode_push_frames({{"q", "p", {R"({"a":1})"}, std::nullopt}});
    const auto frame = framed_groups(bytes.substr(5))[0].frames[0];
    check(frame.txn == util::uuid_text(reinterpret_cast<const uint8_t*>(frame.id.data())),
          "the transaction id is the text of the message id");

    // The broker asks for the content size of a frame it stores as it came.
    Reader r(bytes.substr(5));
    r.u32(); r.text16(); r.text16(); r.u32();
    const std::string zstd = r.take(r.u32());
    check(Reader(zstd.substr(0, 4)).u32() == 0xFD2FB528, "a zstd frame");
    // Frame header descriptor: the single-segment bit, or a content-size field
    // of 2 bytes or more, says the frame carries its content size.
    const int descriptor = static_cast<unsigned char>(zstd[4]);
    check((descriptor & 0x20) != 0 || (descriptor >> 6) != 0, "the frame carries its content size");
}

void test_the_zstd_options_say_what_they_need() {
    StubServer server(queued());
    auto build = [&server](bool binary_push, bool push_compression, bool framed_pop) {
        try {
            QueenClient client({server.url()}, config_with(binary_push, push_compression, framed_pop));
            return std::string();
        } catch (const std::runtime_error& e) {
            return std::string(e.what());
        }
    };
    // push_compression alone compresses nothing: it is binary_push that sends frames.
    check(build(false, true, false).empty(), "push_compression without binary_push builds anywhere");
    check(build(true, false, false).empty(), "binary_push needs no zstd");
    if (wire::zstd_available()) {
        check(build(true, true, true).empty(), "every option builds with zstd");
    } else {
        check(build(true, true, false).find("QUEEN_CLIENT_ZSTD") != std::string::npos,
              "push_compression says it needs zstd");
        check(build(false, false, true).find("QUEEN_CLIENT_ZSTD") != std::string::npos,
              "framed_pop says it needs zstd");
    }
}

// ============================================================================
// Building a framed pop answer
// ============================================================================

std::string le(uint64_t value, size_t width) {
    std::string out;
    for (size_t i = 0; i < width; ++i) out.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
    return out;
}

std::string text(const std::string& s) { return le(s.size(), 2) + s; }

std::string from_hex(const std::string& uuid) {
    std::string out;
    std::string digits;
    for (char c : uuid) if (c != '-') digits.push_back(c);
    for (size_t i = 0; i + 1 < digits.size(); i += 2) {
        out.push_back(static_cast<char>(std::stoi(digits.substr(i, 2), nullptr, 16)));
    }
    return out;
}

/// One stored frame: u32 body_len | flags | id | [trace] | txn | [producer] | payload.
std::string stored_frame(int id, const std::string& txn, const std::string& payload,
                         const std::string& producer = "", const std::string& trace = "") {
    std::string id_bytes(16, '\0');
    id_bytes[0] = static_cast<char>(id);
    id_bytes[6] = 0x70;
    id_bytes[8] = static_cast<char>(0x80);
    std::string body;
    body.push_back(static_cast<char>((trace.empty() ? 0 : 1) | (producer.empty() ? 0 : 2)));
    body += id_bytes;
    if (!trace.empty()) body += from_hex(trace);
    body += text(txn);
    if (!producer.empty()) body += text(producer);
    body += payload;
    return le(body.size(), 4) + body;
}

/// A framed pop answer of one claim on partition 7 ("p7") of "orders", lease
/// "lease-1": one record at base offset 10 holding `frames`, created at
/// 2023-11-14T22:13:20Z, of which the claim covers start..=end.
std::string framed_answer(const std::string& own, uint64_t start, uint64_t end, bool compress,
                          const std::vector<std::string>& frames) {
    std::string raw;
    for (const auto& frame : frames) raw += frame;
    if (compress) raw = wire::zstd_compress(raw);
    std::string b("QPF1\x01", 5);
    b += le(own.size(), 4) + own;
    b += text("orders") + text("lease-1") + text("__QUEUE_MODE__");
    b += le(1, 4);                                    // claims
    b += le(7, 8) + text("p7") + le(start, 8) + le(end, 8);
    b += le(3, 4);                                    // delivery attempt
    b += le(1, 4);                                    // records
    b += le(10, 8) + le(1700000000000000ULL, 8) + le(frames.size(), 4);
    b.push_back(static_cast<char>(compress ? 1 : 0));
    b += le(raw.size(), 4) + raw;
    return b;
}

std::vector<std::string> two_frames() {
    return {stored_frame(1, "t-10", R"({"n":10})"),
            stored_frame(2, "t-11", R"({"n":11,"s":"x"})", "svc")};
}

/// The codecs a decoder case runs on: raw always, zstd where the build has it.
std::vector<bool> codecs() {
    return wire::zstd_available() ? std::vector<bool>{false, true} : std::vector<bool>{false};
}

/// What decode_pop_frames throws for `bytes`, or "" when it decodes.
std::string decode_error(const std::string& bytes) {
    try {
        wire::decode_pop_frames(bytes);
        return "";
    } catch (const std::runtime_error& e) {
        return e.what();
    }
}

// ============================================================================
// The decoder
// ============================================================================

void test_decode_unpacks_a_record_into_the_json_answer() {
    for (bool compress : codecs()) {
        json got = wire::decode_pop_frames(framed_answer(R"({"frames":true})", 10, 11, compress, two_frames()));
        check(got == json::parse(TWO_MESSAGES_JSON),
              std::string(compress ? "zstd" : "raw") + " record:\n      got:  " + got.dump() +
                  "\n      want: " + TWO_MESSAGES_JSON);
    }
}

void test_decode_keeps_only_the_messages_inside_the_claim() {
    for (bool compress : codecs()) {
        json got = wire::decode_pop_frames(framed_answer(R"({"frames":true})", 11, 11, compress, two_frames()));
        check(got["messages"].size() == 1, "one message inside the claim");
        const json& m = got["messages"][0];
        check(m["offset"] == 11 && m["transactionId"] == "t-11" && m["producerSub"] == "svc", "the second message");
        check(m["id"] == "02000000-0000-7000-8000-000000000000", "its id as text");
    }
}

void test_decode_reads_a_trace_id_an_empty_payload_and_an_answer_without_own_keys() {
    const std::string bytes = framed_answer("", 10, 11, false,
        {stored_frame(1, "t-10", "", "", TRACE), stored_frame(2, "t-11", R"([1,"due"])")});
    json got = wire::decode_pop_frames(bytes);

    check(got["messages"][0]["traceId"] == TRACE, "the trace id as text");
    check(got["messages"][0]["data"].is_null(), "an empty payload is null");
    check(got["messages"][1]["data"] == json::array({1, "due"}), "a payload that is an array");

    // Version 0 has no JSON object in front of the names.
    check(wire::decode_pop_frames(std::string("QPF1\x00", 5) + bytes.substr(9)) == got, "version 0 decodes the same");
}

void test_decode_adds_the_answers_own_keys_without_frames() {
    const std::string own = R"({"frames":true,"conflation":true,"autopilot":{"partitions":4,"batch":50}})";
    json got = wire::decode_pop_frames(framed_answer(own, 10, 11, false, two_frames()));

    check(!got.contains("frames"), "`frames` is not a key of the JSON answer");
    check(got["conflation"] == true, "the conflation echo");
    check(got["autopilot"] == json({{"partitions", 4}, {"batch", 50}}), "the autopilot's choice");
}

void test_decode_rejects_damage() {
    const auto frames = two_frames();
    const std::string bytes = framed_answer(R"({"frames":true})", 10, 11, false, frames);
    const size_t tail = frames[0].size() + frames[1].size();

    check(wire::is_pop_frames(bytes) && !wire::is_pop_frames(TWO_MESSAGES_JSON), "the magic tells the two answers apart");
    check(decode_error(bytes.substr(0, bytes.size() - 3)).find("framed pop answer") != std::string::npos, "a truncated answer");
    check(decode_error(TWO_MESSAGES_JSON).find("not a framed pop answer") != std::string::npos, "a JSON answer");

    std::string future = bytes;
    future[4] = 9;
    check(decode_error(future).find("unknown version 9") != std::string::npos, "an unknown version");

    // A record that declares more frames than it holds.
    std::string short_record = bytes;
    short_record.replace(bytes.size() - tail - 9, 4, le(3, 4));
    check(decode_error(short_record).find("a record is shorter than its 3 frames") != std::string::npos,
          "a record shorter than its count: " + decode_error(short_record));

    std::string bad_codec = bytes;
    bad_codec[bytes.size() - tail - 5] = 7;
    check(decode_error(bad_codec).find("unknown codec 7") != std::string::npos, "an unknown codec");
}

// ============================================================================
// Pop
// ============================================================================

void test_a_pop_sends_no_frames_parameter_by_default() {
    StubServer server(answer(TWO_MESSAGES_JSON));
    QueenClient client({server.url()}, config_with(false, false, false));

    json messages = client.queue("orders").batch(2).wait(false).pop();

    check(messages.size() == 2, "two messages");
    check(!server.pops()[0].asked_frames, "the default pop carries no frames parameter");
}

void test_a_framed_pop_gives_the_messages_of_the_json_answer() {
    StubServer json_server(answer(TWO_MESSAGES_JSON));
    QueenClient plain({json_server.url()}, config_with(false, false, false));
    const json want = plain.queue("orders").batch(2).wait(false).pop();

    for (bool compress : {false, true}) {
        StubServer server(framed(framed_answer(R"({"frames":true})", 10, 11, compress, two_frames())));
        QueenClient client({server.url()}, config_with(false, false, true));

        const json got = client.queue("orders").batch(2).wait(false).pop();

        check(server.pops()[0].asked_frames, "the framed pop asks for frames");
        check(got.size() == 2, "two messages");
        check(got == want, std::string(compress ? "zstd" : "raw") + " record:\n      got:  " + got.dump() +
                               "\n      want: " + want.dump());
    }
}

void test_a_framed_pop_asks_for_frames_on_the_partition_and_namespace_routes_too() {
    StubServer server(nothing());
    QueenClient client({server.url()}, config_with(false, false, true));

    client.queue("orders").partition("p7").wait(false).pop();
    client.queue().namespace_name("ns").wait(false).pop();

    const auto pops = server.pops();
    check(pops.size() == 2, "two pops");
    check(pops[0].target.rfind("/api/v1/pop/queue/orders/partition/p7?", 0) == 0 && pops[0].asked_frames,
          "the partition route: " + pops[0].target);
    check(pops[1].target.rfind("/api/v1/pop?", 0) == 0 && pops[1].asked_frames,
          "the namespace route: " + pops[1].target);
}

void test_a_framed_pop_reads_a_json_answer_as_always() {
    // A broker that does not know the parameter answers JSON.
    StubServer server(answer(TWO_MESSAGES_JSON));
    QueenClient client({server.url()}, config_with(false, false, true));

    const json got = client.queue("orders").batch(2).wait(false).pop();

    check(got.size() == 2 && got[1]["transactionId"] == "t-11" && got[1]["data"]["s"] == "x",
          "the JSON answer's messages: " + got.dump());
}

void test_a_framed_pop_of_nothing() {
    StubServer server(nothing());
    QueenClient client({server.url()}, config_with(false, false, true));

    check(client.queue("orders").wait(false).pop().empty(), "an empty pop is no messages");
}

void test_a_framed_pop_carries_the_autopilots_choice() {
    const std::string own = R"({"frames":true,"autopilot":{"partitions":4,"batch":50}})";
    StubServer server(framed(framed_answer(own, 10, 11, false, two_frames())));
    QueenClient client({server.url()}, config_with(false, false, true));

    const PopResult result = client.queue("orders").wait(false).pop_result();

    check(result.messages.size() == 2, "two messages");
    check(result.autopilot.present && result.autopilot.partitions == 4 && result.autopilot.batch == 50,
          "the autopilot's choice from the answer's own keys");
}

void test_a_framed_pop_carries_the_conflation_echo_and_still_throws_without_it() {
    {
        StubServer server(framed(framed_answer(R"({"frames":true,"conflation":true})", 10, 11, false, two_frames())));
        QueenClient client({server.url()}, config_with(false, false, true));
        check(client.queue("orders").group("g").conflation(true).wait(false).pop().size() == 2,
              "a conflating pop whose answer echoes it");
    }
    StubServer server(framed(framed_answer(R"({"frames":true})", 10, 11, false, two_frames())));
    QueenClient client({server.url()}, config_with(false, false, true));
    bool raised = false;
    try {
        client.queue("orders").group("g2").conflation(true).wait(false).pop();
    } catch (const ConflationUnsupportedError&) {
        raised = true;
    }
    check(raised, "no echo, no conflation: the pop raises");
}

void test_consume_with_framed_pops_hands_the_handler_the_claim_in_order() {
    StubServer server(framed(framed_answer(R"({"frames":true})", 10, 11, true, two_frames())));
    QueenClient client({server.url()}, config_with(false, false, true));
    StopAfter budget(5000);
    std::mutex mutex;
    std::vector<std::string> seen;

    client.queue("orders").batch(2).wait(false).auto_ack(false).each().limit(2)
        .consume([&](const json& message) {
            std::lock_guard<std::mutex> lock(mutex);
            seen.push_back(message["transactionId"].get<std::string>() + "@" +
                           message["partitionId"].get<std::string>() + "/" +
                           message["leaseId"].get<std::string>());
        }, budget.signal());

    check(seen == std::vector<std::string>({"t-10@7/lease-1", "t-11@7/lease-1"}),
          "the handler got the claim in order (" + std::to_string(seen.size()) + " messages)");
    const auto pops = server.pops();
    check(!pops.empty(), "the consumer popped");
    for (const auto& pop : pops) check(pop.asked_frames, "a pop of the consumer asks for frames");
}

// ============================================================================
// Transaction
// ============================================================================

static const json POPPED = {{"transactionId", "in-1"}, {"partitionId", "7"}, {"leaseId", "lease-1"}};

inline Responder committed(const json& results) {
    return answer(json{{"transactionId", "tx"}, {"success", true}, {"results", results}}.dump());
}

struct TransactionBody {
    int flags = 0;
    json head;                      // null for an empty head
    std::vector<FramedGroup> groups;
};

/// The flags, the head and the groups of a binary transaction body.
TransactionBody transaction_body(const std::string& raw) {
    check(raw.substr(0, 4) == "QTB1", "a binary transaction body");
    TransactionBody body;
    body.flags = static_cast<unsigned char>(raw[4]);
    const size_t head_length = Reader(raw.substr(5, 4)).u32();
    const std::string head = raw.substr(9, head_length);
    body.head = head.empty() ? json(nullptr) : json::parse(head);
    body.groups = framed_groups(raw.substr(9 + head_length));
    return body;
}

void test_a_transaction_is_the_json_bundle_by_default() {
    const json results = json::array({{{"index", 0}, {"type", "ack"}, {"success", true}},
                                      {{"index", 1}, {"type", "push"}, {"success", true}}});
    for (const auto& config : {config_with(false, false, false), config_with(true, false, false),
                               config_with(false, true, false)}) {
        StubServer server(committed(results));
        QueenClient client({server.url()}, config);

        client.transaction().ack(POPPED).queue("out").push({{{"data", {{"n", 1}}}}}).commit();

        const auto sent = server.transactions();
        check(sent.size() == 1, "one transaction request");
        check(sent[0].content_type == "application/json", "content type " + sent[0].content_type);
        const json body = json::parse(sent[0].body);
        check(body["operations"][0]["type"] == "ack" && body["operations"][1]["type"] == "push",
              "the JSON bundle, its operations as the caller added them");
    }
}

void test_the_pushes_travel_as_frames_and_the_rest_as_the_json_head() {
    StubServer server(committed(json::array({
        {{"index", 0}, {"type", "ack"}, {"success", true}, {"transactionId", "in-1"}},
        {{"index", 1}, {"type", "push"}, {"success", true}, {"transactionId", "a"}},
        {{"index", 2}, {"type", "push"}, {"success", true}, {"transactionId", "tx-b"}},
        {{"index", 3}, {"type", "push"}, {"success", true}, {"transactionId", "c"}}})));
    QueenClient client({server.url()}, config_with(true, true, false));

    // This client's transaction pushes go to a queue's default partition.
    json result = client.transaction()
        .ack(POPPED)
        .queue("out").push({{{"data", {{"n", 1}}}}})
        .queue("out").push({{{"data", {{"n", 2}}}, {"transactionId", "tx-b"}}})
        .queue("other").push({{{"data", "text"}}})
        .commit();

    check(result["success"] == true && result["results"].size() == 4, "the answer: " + result.dump());
    const auto sent = server.transactions();
    check(sent.size() == 1, "one transaction request");
    check(sent[0].content_type == "application/x-queen-push", "content type " + sent[0].content_type);
    const auto body = transaction_body(sent[0].body);
    check(body.flags == 0, "no flags");
    // The head: the ack and the lease, and no push.
    check(body.head == json({
              {"operations", json::array({{{"type", "ack"}, {"transactionId", "in-1"}, {"partitionId", "7"},
                                           {"status", "completed"}, {"leaseId", "lease-1"}}})},
              {"requiredLeases", json::array({"lease-1"})}}),
          "the head: " + body.head.dump());
    // The pushes: consecutive items of one partition share a group.
    check(body.groups.size() == 2, "two groups");
    if (body.groups.size() != 2) return;
    const auto& first = body.groups[0];
    check(first.queue == "out" && first.partition == "Default" && first.count == 2 && first.frames.size() == 2,
          "the first group: out/Default with two messages");
    check(first.frames[0].payload == R"({"n":1})" && first.frames[0].txn.size() == 36, "its first message");
    check(first.frames[1].payload == R"({"n":2})" && first.frames[1].txn == "tx-b", "its second message");
    const auto& second = body.groups[1];
    check(second.queue == "other" && second.count == 1 && second.frames[0].payload == R"("text")",
          "the second group: other with one message");
}

void test_the_results_come_back_in_the_order_of_the_callers_operations() {
    // The caller pushes, acks, pushes. The broker answers the ack first (it is
    // in the head), then the two pushes, then the rider.
    StubServer server(committed(json::array({
        {{"index", 0}, {"type", "ack"}, {"success", true}, {"transactionId", "in-1"}},
        {{"index", 1}, {"type", "push"}, {"success", true}, {"transactionId", "first"}},
        {{"index", 2}, {"type", "push"}, {"success", true}, {"transactionId", "second"}},
        {{"index", 3}, {"type", "kv"}, {"opIndex", 0}, {"applied", true}}})));
    QueenClient client({server.url()}, config_with(true, true, false));

    json result = client.transaction()
        .queue("out").push({{{"data", 1}, {"transactionId", "first"}}})
        .ack(POPPED)
        .queue("out").push({{{"data", 2}, {"transactionId", "second"}}})
        .kv("saga").put("k1", {{"step", 1}}, KvTtl::seconds(86400))
        .commit();

    const json& results = result["results"];
    check(results.size() == 4, "four results");
    if (results.size() != 4) return;
    check(results[0]["index"] == 0 && results[0]["type"] == "push" && results[0]["transactionId"] == "first",
          "results[0] is the first push: " + results[0].dump());
    check(results[1]["index"] == 1 && results[1]["type"] == "ack" && results[1]["transactionId"] == "in-1",
          "results[1] is the ack: " + results[1].dump());
    check(results[2]["index"] == 2 && results[2]["type"] == "push" && results[2]["transactionId"] == "second",
          "results[2] is the second push: " + results[2].dump());
    check(results[3]["index"] == 3 && results[3]["type"] == "kv", "the rider stays where it was: " + results[3].dump());
    // On the wire the two pushes are one group, in the caller's order, and the
    // rider travels in the head.
    const auto body = transaction_body(server.transactions()[0].body);
    check(body.head["operations"].size() == 1 && body.head["operations"][0]["type"] == "ack", "the head has the ack only");
    check(body.head["kv"].size() == 1, "the rider travels in the head");
    check(body.groups.size() == 1 && body.groups[0].frames.size() == 2 &&
              body.groups[0].frames[0].txn == "first" && body.groups[0].frames[1].txn == "second",
          "one group, the pushes in the caller's order");
}

void test_the_results_stay_as_they_are_when_the_acks_come_first_anyway() {
    const json results = json::array({
        {{"index", 0}, {"type", "ack"}, {"success", true}, {"transactionId", "in-1"}},
        {{"index", 1}, {"type", "push"}, {"success", true}, {"transactionId", "a"}}});
    StubServer server(committed(results));
    QueenClient client({server.url()}, config_with(true, true, false));

    json result = client.transaction().ack(POPPED)
        .queue("out").push({{{"data", 1}, {"transactionId", "a"}}}).commit();

    check(result["results"] == results, "the results as the broker sent them: " + result["results"].dump());
}

void test_a_rolled_back_bundle_comes_back_as_the_broker_answered_it() {
    const json lost = {{"transactionId", "tx"}, {"success", false}, {"reason", "kv_precondition"},
                       {"results", json::array()}, {"failedIndex", 2}, {"kvReason", "exists"}};
    StubServer server(answer(lost.dump()));
    QueenClient client({server.url()}, config_with(true, true, false));
    KvWriteOptions required;
    required.required = true;

    json result = client.transaction()
        .queue("out").push({{{"data", 1}}})
        .ack(POPPED)
        .kv("idem").put_if_absent("order-1", true, KvTtl::seconds(86400), required)
        .commit();

    check(result == lost, "the verdict as the broker sent it: " + result.dump());
    check(server.transactions()[0].body.substr(0, 4) == "QTB1", "the bundle went out as the binary body");
}

void test_a_bundle_whose_push_carries_a_trace_id_stays_json() {
    StubServer server(committed(json::array({{{"index", 0}, {"type", "push"}, {"success", true}}})));
    QueenClient client({server.url()}, config_with(true, true, false));

    client.transaction().queue("out").push({{{"data", 1}, {"traceId", TRACE}}}).commit();

    const auto sent = server.transactions();
    check(sent[0].content_type == "application/json", "a push with a trace id travels as JSON");
    check(json::parse(sent[0].body)["operations"][0]["items"][0]["traceId"] == TRACE, "with its trace id");
}

void test_a_bundle_without_pushes_stays_json() {
    // There is nothing to frame.
    StubServer server(committed(json::array({{{"index", 0}, {"type", "ack"}, {"success", true}}})));
    QueenClient client({server.url()}, config_with(true, true, false));

    client.transaction().ack(POPPED).commit();

    const auto sent = server.transactions();
    check(sent[0].content_type == "application/json" && sent[0].body[0] == '{', "a bundle of acks travels as JSON");
}

void test_the_transaction_counts_flag_and_an_empty_head() {
    const std::string body = wire::encode_transaction_frames("", {{"q", "p", {"1"}, std::nullopt}}, true);

    check(body.substr(0, 9) == std::string("QTB1\x02\x00\x00\x00\x00", 9), "flag 2 and an empty head");
    const auto decoded = transaction_body(body);
    check(decoded.flags == 2 && decoded.head.is_null(), "the counts flag, no head");
    check(decoded.groups.size() == 1 && decoded.groups[0].frames[0].payload == "1", "the group");
}

void test_transaction_frames_needs_a_push_it_can_carry() {
    const json ack = {{"type", "ack"}, {"transactionId", "in-1"}, {"partitionId", "7"}, {"status", "completed"}};
    check(!wire::transaction_frames({{"operations", json::array({ack})}}), "a bundle of acks");
    check(!wire::transaction_frames({{"operations", json::array()}}), "no operations");
    check(!wire::transaction_frames({{"kv", json::array()}}), "no operations key");
    check(!wire::transaction_frames({{"operations", json::array({{{"type", "push"}, {"items", json::array({
              {{"queue", "q"}, {"payload", 1}, {"traceId", TRACE}}})}}})}}),
          "a push with a trace id");
    // The order the broker answers in, when it is not the caller's.
    json answer = {{"results", json::array({{{"index", 0}, {"type", "ack"}}, {{"index", 1}, {"type", "push"}}})}};
    wire::restore_result_order(answer, std::vector<size_t>{1, 0});
    check(answer["results"][0] == json({{"index", 0}, {"type", "push"}}) &&
              answer["results"][1] == json({{"index", 1}, {"type", "ack"}}),
          "the results in the caller's order: " + answer["results"].dump());
}

// ============================================================================
// main
// ============================================================================

int main() {
    std::cout << BLUE << "Queen C++ client - binary push and framed pop wire tests"
              << (wire::zstd_available() ? " (with zstd)" : " (without zstd)") << RESET
              << std::endl
              << std::endl;

    run_test("a push is JSON by default", test_a_push_is_json_by_default);
    run_test("binary_push sends the binary body, each item with its transaction id",
             test_binary_push_sends_the_binary_body_with_each_transaction_id);
    run_test("consecutive items of one partition share a group",
             test_consecutive_items_of_one_partition_share_a_group);
    run_test("binary_push carries payloads that are not objects, and non-ASCII text",
             test_binary_push_carries_payloads_that_are_not_objects_and_non_ascii_text);
    run_test("an item with a trace id stays JSON", test_an_item_with_a_trace_id_stays_json);
    run_test("an item the binary body does not carry leaves the push to JSON",
             test_an_item_the_binary_body_does_not_carry_leaves_the_push_to_json);
    run_test("a buffered push is binary too", test_a_buffered_push_is_binary_too);
    run_test("the counts flag, and mismatched transaction ids",
             test_the_counts_flag_and_mismatched_transaction_ids);
    run_zstd_test("the zstd flag compresses the whole body", test_the_zstd_flag_compresses_the_whole_body);
    run_zstd_test("push_compression sends each partition as one zstd frame of stored frames",
                  test_push_compression_sends_each_partition_as_one_zstd_frame_of_stored_frames);
    run_zstd_test("a frame without a transaction id gets the text of its id",
                  test_a_frame_without_a_transaction_id_gets_the_text_of_its_id);
    run_test("the zstd options say what they need", test_the_zstd_options_say_what_they_need);

    run_test("decode unpacks a record into the JSON answer", test_decode_unpacks_a_record_into_the_json_answer);
    run_test("decode keeps only the messages inside the claim",
             test_decode_keeps_only_the_messages_inside_the_claim);
    run_test("decode reads a trace id, an empty payload, and an answer without own keys",
             test_decode_reads_a_trace_id_an_empty_payload_and_an_answer_without_own_keys);
    run_test("decode adds the answer's own keys, without `frames`",
             test_decode_adds_the_answers_own_keys_without_frames);
    run_test("decode rejects damage", test_decode_rejects_damage);

    run_test("a pop sends no frames parameter by default", test_a_pop_sends_no_frames_parameter_by_default);
    run_zstd_test("a framed pop gives the messages of the JSON answer",
                  test_a_framed_pop_gives_the_messages_of_the_json_answer);
    run_zstd_test("a framed pop asks for frames on the partition and namespace routes too",
                  test_a_framed_pop_asks_for_frames_on_the_partition_and_namespace_routes_too);
    run_zstd_test("a framed pop reads a JSON answer as always", test_a_framed_pop_reads_a_json_answer_as_always);
    run_zstd_test("a framed pop of nothing", test_a_framed_pop_of_nothing);
    run_zstd_test("a framed pop carries the autopilot's choice", test_a_framed_pop_carries_the_autopilots_choice);
    run_zstd_test("a framed pop carries the conflation echo, and still throws without it",
                  test_a_framed_pop_carries_the_conflation_echo_and_still_throws_without_it);
    run_zstd_test("consume with framed pops hands the handler the claim in order",
                  test_consume_with_framed_pops_hands_the_handler_the_claim_in_order);

    run_test("a transaction is the JSON bundle by default", test_a_transaction_is_the_json_bundle_by_default);
    run_zstd_test("a transaction's pushes travel as frames, the rest as the JSON head",
                  test_the_pushes_travel_as_frames_and_the_rest_as_the_json_head);
    run_zstd_test("a transaction's results come back in the order of the caller's operations",
                  test_the_results_come_back_in_the_order_of_the_callers_operations);
    run_zstd_test("a transaction's results stay as they are when the acks come first anyway",
                  test_the_results_stay_as_they_are_when_the_acks_come_first_anyway);
    run_zstd_test("a rolled-back bundle comes back as the broker answered it",
                  test_a_rolled_back_bundle_comes_back_as_the_broker_answered_it);
    run_zstd_test("a bundle whose push carries a trace id stays JSON",
                  test_a_bundle_whose_push_carries_a_trace_id_stays_json);
    run_zstd_test("a bundle without pushes stays JSON", test_a_bundle_without_pushes_stays_json);
    run_zstd_test("the transaction counts flag, and an empty head",
                  test_the_transaction_counts_flag_and_an_empty_head);
    run_test("transaction_frames needs a push it can carry", test_transaction_frames_needs_a_push_it_can_carry);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All binary push and framed pop wire tests passed" << RESET;
        if (skipped > 0) std::cout << YELLOW << " (" << skipped << " skipped: built without zstd)" << RESET;
        std::cout << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
