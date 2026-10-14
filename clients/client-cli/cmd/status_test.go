package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// `queenctl status` against the shapes a 2.x broker sends.
//
// The command used to read its queue rows from GET /api/v1/status, whose rows
// carry a name, the labels and a partition count: every count column printed
// `-` and every JSON count was null. The three bodies below were captured from
// a broker (2.2.0), trimmed of the fields the command does not read.

// GET /api/v1/resources/queues: newest queue first, counts under `messages`,
// an unset label as "".
const statusQueuesBody = `{"queues":[
  {"id":"01a134d9-4b7e-7001-8056-83ebdd41597a","name":"sc.queuemode","queue":"sc.queuemode","namespace":"","task":"","storage":"segments","partitions":1,
   "messages":{"total":7,"pending":5,"processing":0,"completed":2,"deadLetter":0},"retainedBytes":614,"createdAt":"2026-10-13T10:40:18.302428Z"},
  {"id":"01a134d9-4b6f-7001-bc57-43d03babda7c","name":"sc.empty","queue":"sc.empty","namespace":"billing","task":"idle","storage":"segments","partitions":0,
   "messages":{"total":0,"pending":0,"processing":0,"completed":0,"deadLetter":0},"retainedBytes":0,"createdAt":"2026-10-13T10:40:18.287852Z"},
  {"id":"01a134d9-4b5c-7001-96ea-13656ea2c1a8","name":"sc.emails","queue":"sc.emails","namespace":"notify","task":"send","storage":"segments","partitions":1,
   "messages":{"total":5,"pending":4,"processing":0,"completed":0,"deadLetter":1},"retainedBytes":450,"createdAt":"2026-10-13T10:40:18.268621Z"},
  {"id":"01a134d9-4b44-7001-9e02-12ded6893f6d","name":"sc.orders","queue":"sc.orders","namespace":"billing","task":"charge","storage":"segments","partitions":4,
   "messages":{"total":40,"pending":31,"processing":3,"completed":6,"deadLetter":0},"retainedBytes":4110,"createdAt":"2026-10-13T10:40:18.244752Z"}
],"kvRows":0,"kvBytes":0,"timerRows":0,"timerBytes":0}`

// GET /api/v1/consumer-groups: a bare array, one row per (group, queue) that
// holds a cursor. sc.orders has two groups, sc.empty has none.
const statusGroupsBody = `[
  {"name":"__QUEUE_MODE__","queueName":"sc.queuemode","topics":["sc.queuemode"],"members":1,"partitionsWithLag":1,"totalLag":5,"maxTimeLag":90,"state":"Stable","conflation":false},
  {"name":"audit","queueName":"sc.orders","topics":["sc.orders"],"members":4,"partitionsWithLag":1,"totalLag":2,"maxTimeLag":12,"state":"Stable","conflation":false},
  {"name":"billing","queueName":"sc.orders","topics":["sc.orders"],"members":3,"partitionsWithLag":3,"totalLag":24,"maxTimeLag":406,"state":"Lagging","conflation":false},
  {"name":"smtp","queueName":"sc.emails","topics":["sc.emails"],"members":1,"partitionsWithLag":1,"totalLag":4,"maxTimeLag":406,"state":"Lagging","conflation":false}
]`

// GET /api/v1/status/queues/sc.orders, at the same moment as the listing
// above: its pending (34) counts the 3 leased messages the listing leaves out.
const statusDetailBody = `{
  "queue":{"id":"01a134d9-4b44-7001-9e02-12ded6893f6d","name":"sc.orders","namespace":"billing","task":"charge","priority":0,"createdAt":"2026-10-13T10:40:18.244752Z","config":{"leaseTime":300}},
  "partitions":[
    {"name":"c0","messages":{"total":10,"pending":4,"processing":0,"completed":6,"failed":null,"deadLetter":0}},
    {"name":"c1","messages":{"total":10,"pending":10,"processing":0,"completed":0,"failed":null,"deadLetter":0}},
    {"name":"c2","messages":{"total":10,"pending":10,"processing":0,"completed":0,"failed":null,"deadLetter":0}},
    {"name":"c3","messages":{"total":10,"pending":10,"processing":3,"completed":0,"failed":null,"deadLetter":0}}
  ],
  "totals":{"messages":{"total":40,"pending":34,"processing":3,"completed":6,"failed":null,"deadLetter":0}}
}`

// runStatus runs `queenctl status` and returns what it printed. The command's
// flags are package variables cobra writes only when a flag is typed, so a
// --namespace from one run would filter the next: they are put back around
// every run.
func runStatus(t *testing.T, server string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	reset := func() { statusNamespace, statusTask, statusPartitions = "", "", false }
	reset()
	t.Cleanup(reset)
	stop := captureStdio(t)
	err = runCLI(t, server, append([]string{"status"}, args...)...)
	stdout, stderr = stop()
	return stdout, stderr, err
}

func statusRows(t *testing.T, stdout string) []map[string]any {
	t.Helper()
	var rows []map[string]any
	if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
		t.Fatalf("status -o json is not an array of rows: %v\n%s", err, stdout)
	}
	return rows
}

func statusBroker(t *testing.T) *fakeBroker {
	t.Helper()
	fb := newFakeBroker(t)
	fb.route("/api/v1/resources/queues", statusQueuesBody)
	fb.route("/api/v1/consumer-groups", statusGroupsBody)
	fb.route("/api/v1/status/queues/sc.orders", statusDetailBody)
	return fb
}

func TestStatusFillsEveryQueueRow(t *testing.T) {
	fb := statusBroker(t)
	stdout, _, err := runStatus(t, fb.url(), "-o", "json")
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	// The whole row, so a key that comes back null or a `failed` that comes
	// back at all fails here. By name, not in the listing's newest-first order.
	// lagSeconds is the slowest group's: 406 for sc.orders, not audit's 12.
	want := []map[string]any{
		{"queue": "sc.emails", "namespace": "notify", "task": "send", "partitions": 1.0,
			"pending": 4.0, "processing": 0.0, "completed": 0.0, "dlq": 1.0, "lagSeconds": 406.0},
		{"queue": "sc.empty", "namespace": "billing", "task": "idle", "partitions": 0.0,
			"pending": 0.0, "processing": 0.0, "completed": 0.0, "dlq": 0.0, "lagSeconds": nil},
		{"queue": "sc.orders", "namespace": "billing", "task": "charge", "partitions": 4.0,
			"pending": 31.0, "processing": 3.0, "completed": 6.0, "dlq": 0.0, "lagSeconds": 406.0},
		{"queue": "sc.queuemode", "namespace": nil, "task": nil, "partitions": 1.0,
			"pending": 5.0, "processing": 0.0, "completed": 2.0, "dlq": 0.0, "lagSeconds": 90.0},
	}
	if got := statusRows(t, stdout); !reflect.DeepEqual(got, want) {
		t.Errorf("status rows:\n got %v\nwant %v", got, want)
	}

	// GET /api/v1/status has no per-queue counts, and a proxy answers it 404
	// to a tenant credential.
	if reqs := fb.requestsTo("/api/v1/status"); len(reqs) != 0 {
		t.Errorf("status still calls GET /api/v1/status (%d requests)", len(reqs))
	}
}

func TestStatusTableShowsTheCounts(t *testing.T) {
	fb := statusBroker(t)
	stdout, _, err := runStatus(t, fb.url(), "-o", "table")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	want := [][]string{
		{"QUEUE", "NAMESPACE", "PENDING", "PROCESSING", "COMPLETED", "DLQ", "LAG"},
		{"sc.emails", "notify", "4", "0", "0", "1", "6m46s"},
		// No group has read it: an empty lag, not a zero one.
		{"sc.empty", "billing", "0", "0", "0", "0", "-"},
		{"sc.orders", "billing", "31", "3", "6", "0", "6m46s"},
		{"sc.queuemode", "-", "5", "0", "2", "0", "1m30s"},
	}
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) != len(want) {
		t.Fatalf("want %d lines, got %d:\n%s", len(want), len(lines), stdout)
	}
	for i, line := range lines {
		if got := strings.Fields(line); !reflect.DeepEqual(got, want[i]) {
			t.Errorf("line %d = %v, want %v", i, got, want[i])
		}
	}
}

func TestStatusFiltersByNamespaceAndTask(t *testing.T) {
	// The listing answers every queue whatever its query says, so the flags
	// are applied by the command.
	cases := []struct {
		args []string
		want []string
	}{
		{[]string{"--namespace", "billing"}, []string{"sc.empty", "sc.orders"}},
		{[]string{"--task", "send"}, []string{"sc.emails"}},
		{[]string{"--namespace", "billing", "--task", "charge"}, []string{"sc.orders"}},
		{[]string{"--namespace", "nobody"}, []string{}},
	}
	for _, tc := range cases {
		fb := statusBroker(t)
		stdout, _, err := runStatus(t, fb.url(), append([]string{"-o", "json"}, tc.args...)...)
		if err != nil {
			t.Fatalf("status %v: %v", tc.args, err)
		}
		got := []string{}
		for _, row := range statusRows(t, stdout) {
			got = append(got, row["queue"].(string))
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("status %v listed %v, want %v", tc.args, got, tc.want)
		}
	}
}

func TestStatusOfOneQueueReadsTheNestedTotals(t *testing.T) {
	fb := statusBroker(t)
	stdout, _, err := runStatus(t, fb.url(), "sc.orders", "-o", "json")
	if err != nil {
		t.Fatalf("status sc.orders: %v", err)
	}
	rows := statusRows(t, stdout)
	if len(rows) != 1 {
		t.Fatalf("want one row, got %d:\n%s", len(rows), stdout)
	}
	row := rows[0]
	// The partitions stay the broker's array in JSON; the table prints how many.
	if parts, _ := row["partitions"].([]any); len(parts) != 4 {
		t.Errorf("partitions = %v, want the 4 partitions the broker sent", row["partitions"])
	}
	delete(row, "partitions")
	// pending is 31, as the listing counts it: the route's 34 less the 3 leased.
	want := map[string]any{"queue": "sc.orders", "namespace": "billing", "task": "charge",
		"pending": 31.0, "processing": 3.0, "completed": 6.0, "dlq": 0.0, "lagSeconds": 406.0}
	if !reflect.DeepEqual(row, want) {
		t.Errorf("status sc.orders:\n got %v\nwant %v", row, want)
	}

	stdout, _, err = runStatus(t, fb.url(), "sc.orders", "-o", "table")
	if err != nil {
		t.Fatalf("status sc.orders -o table: %v", err)
	}
	wantTable := [][]string{
		{"QUEUE", "PENDING", "PROCESSING", "COMPLETED", "DLQ", "PARTITIONS", "LAG"},
		{"sc.orders", "31", "3", "6", "0", "4", "6m46s"},
	}
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) != len(wantTable) {
		t.Fatalf("want %d lines, got %d:\n%s", len(wantTable), len(lines), stdout)
	}
	for i, line := range lines {
		if got := strings.Fields(line); !reflect.DeepEqual(got, wantTable[i]) {
			t.Errorf("line %d = %v, want %v", i, got, wantTable[i])
		}
	}
}

func TestStatusKeepsTheCountsWhenTheGroupListingFails(t *testing.T) {
	// The group listing walks every cursor of the tenant and can fail where
	// the queue listing answers. A 4xx, because the SDK retries a 5xx.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/consumer-groups" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"no_such_route","error":"not found"}`))
			return
		}
		_, _ = w.Write([]byte(statusQueuesBody))
	}))
	t.Cleanup(srv.Close)

	stdout, stderr, err := runStatus(t, srv.URL, "-o", "json")
	if err != nil {
		t.Fatalf("status must still print the queues: %v", err)
	}
	rows := statusRows(t, stdout)
	if len(rows) != 4 {
		t.Fatalf("want the 4 queues, got %d:\n%s", len(rows), stdout)
	}
	for _, row := range rows {
		if row["lagSeconds"] != nil {
			t.Errorf("%v: lagSeconds = %v, want null without the group listing", row["queue"], row["lagSeconds"])
		}
	}
	if got := rows[2]["pending"]; got != 31.0 {
		t.Errorf("sc.orders pending = %v, want 31", got)
	}
	if !strings.Contains(stderr, "consumer group lag unavailable") {
		t.Errorf("stderr does not say why LAG is empty: %q", stderr)
	}
}
