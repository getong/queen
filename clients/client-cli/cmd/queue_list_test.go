package cmd

import (
	"encoding/json"
	"reflect"
	"testing"
)

// `queenctl queue list` against the shape a 2.x broker sends.
//
// Its table read `partitionCount`, `priority`, `leaseTime`, `retryLimit`,
// `maxSize` and `encryptionEnabled` at the top level of a row, where the
// listing has `partitions` there and the configuration under `options`: six
// columns printed `-`. Its four flags went out as a query the route does not
// read, so they kept nothing out.

// GET /api/v1/resources/queues, captured from a broker (2.2.0) and trimmed of
// the options the command does not print: newest queue first. sc.queuemode was
// created by its first push, which labels a queue after its name.
const queueListBody = `{"queues":[
  {"id":"01a134f2-f540-7001-94bb-5821cd70a45a","name":"sc.vault","queue":"sc.vault","namespace":"billing","task":"vault","storage":"segments","partitions":0,
   "messages":{"total":0,"pending":0,"processing":0,"completed":0,"deadLetter":0},"retainedBytes":0,
   "options":{"priority":0,"leaseTime":300,"retryLimit":3,"maxSize":2500000,"encryptionEnabled":true},"createdAt":"2026-10-13T11:08:20.160675Z"},
  {"id":"01a134ec-14ec-7008-a842-f6b159607a9a","name":"sc.queuemode","queue":"sc.queuemode","namespace":"sc","task":"queuemode","storage":"segments","partitions":1,
   "messages":{"total":7,"pending":5,"processing":0,"completed":2,"deadLetter":0},"retainedBytes":628,
   "options":{"priority":0,"leaseTime":60,"retryLimit":3,"maxSize":0,"encryptionEnabled":false},"createdAt":"2026-10-13T11:00:49.516462Z"},
  {"id":"01a134ec-1487-7000-95d2-ab4c15bd01a0","name":"sc.empty","queue":"sc.empty","namespace":"billing","task":"idle","storage":"segments","partitions":0,
   "messages":{"total":0,"pending":0,"processing":0,"completed":0,"deadLetter":0},"retainedBytes":0,
   "options":{"priority":0,"leaseTime":300,"retryLimit":3,"maxSize":0,"encryptionEnabled":false},"createdAt":"2026-10-13T11:00:49.415010Z"},
  {"id":"01a134ec-1470-7000-a801-ccfc0d376bad","name":"sc.emails","queue":"sc.emails","namespace":"notify","task":"send","storage":"segments","partitions":1,
   "messages":{"total":5,"pending":0,"processing":5,"completed":0,"deadLetter":0},"retainedBytes":465,
   "options":{"priority":0,"leaseTime":300,"retryLimit":3,"maxSize":0,"encryptionEnabled":false},"createdAt":"2026-10-13T11:00:49.392049Z"},
  {"id":"01a134ec-1458-7000-b587-1d52604d431c","name":"sc.orders","queue":"sc.orders","namespace":"billing","task":"charge","storage":"segments","partitions":4,
   "messages":{"total":41,"pending":33,"processing":0,"completed":8,"deadLetter":0},"retainedBytes":3934,
   "options":{"priority":5,"leaseTime":60,"retryLimit":2,"maxSize":1000,"encryptionEnabled":false},"createdAt":"2026-10-13T11:00:49.367983Z"}
],"kvRows":0,"kvBytes":0,"timerRows":0,"timerBytes":0}`

const queuesPath = "/api/v1/resources/queues"

func queueListBroker(t *testing.T) *fakeBroker {
	t.Helper()
	fb := newFakeBroker(t)
	fb.route(queuesPath, queueListBody)
	return fb
}

// runQueueList runs `queenctl queue list` and returns what it printed. Its
// flags are package variables cobra writes only when a flag is typed, so a
// --limit from one run would cut the next: they are put back around every run.
func runQueueList(t *testing.T, server string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	reset := func() {
		queueListNamespace, queueListTask = "", ""
		queueListLimit, queueListOffset = 0, 0
	}
	reset()
	t.Cleanup(reset)
	stop := captureStdio(t)
	err = runCLI(t, server, append([]string{"queue", "list"}, args...)...)
	stdout, stderr = stop()
	return stdout, stderr, err
}

func TestQueueListTableShowsPartitionsAndOptions(t *testing.T) {
	fb := queueListBroker(t)
	stdout, _, err := runQueueList(t, fb.url(), "-o", "table")
	if err != nil {
		t.Fatalf("queue list: %v", err)
	}
	// LEASE is in seconds, as `queue configure --lease-time` takes it.
	wantTable(t, stdout, [][]string{
		{"NAME", "NAMESPACE", "TASK", "PARTITIONS", "PRIORITY", "LEASE"},
		{"sc.vault", "billing", "vault", "0", "0", "300"},
		{"sc.queuemode", "sc", "queuemode", "1", "0", "60"},
		{"sc.empty", "billing", "idle", "0", "0", "300"},
		{"sc.emails", "notify", "send", "1", "0", "300"},
		{"sc.orders", "billing", "charge", "4", "5", "60"},
	})

	stdout, _, err = runQueueList(t, fb.url(), "-o", "wide")
	if err != nil {
		t.Fatalf("queue list -o wide: %v", err)
	}
	wantTable(t, stdout, [][]string{
		{"NAME", "NAMESPACE", "TASK", "PARTITIONS", "PRIORITY", "LEASE", "RETRY", "MAXSIZE", "ENCRYPTED"},
		{"sc.vault", "billing", "vault", "0", "0", "300", "3", "2,500,000", "yes"},
		{"sc.queuemode", "sc", "queuemode", "1", "0", "60", "3", "0", "-"},
		{"sc.empty", "billing", "idle", "0", "0", "300", "3", "0", "-"},
		{"sc.emails", "notify", "send", "1", "0", "300", "3", "0", "-"},
		{"sc.orders", "billing", "charge", "4", "5", "60", "2", "1,000", "-"},
	})
}

func TestQueueListJSONKeepsTheBrokersRows(t *testing.T) {
	// The rows always carried real values under the broker's keys: scripts
	// read them, and the table is what changed.
	fb := queueListBroker(t)
	stdout, _, err := runQueueList(t, fb.url(), "-o", "json")
	if err != nil {
		t.Fatalf("queue list -o json: %v", err)
	}
	var want struct {
		Queues json.RawMessage `json:"queues"`
	}
	if err := json.Unmarshal([]byte(queueListBody), &want); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	wantBrokerRows(t, stdout, string(want.Queues))
}

func TestQueueListAppliesItsFlags(t *testing.T) {
	// The listing answers every queue whatever its query says, so the flags
	// are applied by the command: the labels first, then the page.
	cases := []struct {
		args []string
		want []string
	}{
		{[]string{"--namespace", "billing"}, []string{"sc.vault", "sc.empty", "sc.orders"}},
		{[]string{"--task", "send"}, []string{"sc.emails"}},
		{[]string{"--namespace", "billing", "--task", "charge"}, []string{"sc.orders"}},
		{[]string{"--namespace", "nobody"}, []string{}},
		{[]string{"--limit", "2"}, []string{"sc.vault", "sc.queuemode"}},
		{[]string{"--offset", "3"}, []string{"sc.emails", "sc.orders"}},
		{[]string{"--limit", "2", "--offset", "1"}, []string{"sc.queuemode", "sc.empty"}},
		{[]string{"--limit", "9"}, []string{"sc.vault", "sc.queuemode", "sc.empty", "sc.emails", "sc.orders"}},
		{[]string{"--offset", "9"}, []string{}},
		{[]string{"--namespace", "billing", "--limit", "1", "--offset", "1"}, []string{"sc.empty"}},
	}
	for _, tc := range cases {
		fb := queueListBroker(t)
		stdout, _, err := runQueueList(t, fb.url(), append([]string{"-o", "json"}, tc.args...)...)
		if err != nil {
			t.Fatalf("queue list %v: %v", tc.args, err)
		}
		var rows []map[string]any
		if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
			t.Fatalf("queue list %v -o json is not an array of rows: %v\n%s", tc.args, err, stdout)
		}
		got := []string{}
		for _, row := range rows {
			got = append(got, row["name"].(string))
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("queue list %v listed %v, want %v", tc.args, got, tc.want)
		}

		// A broker that came to read the query would apply --offset a second
		// time: nothing is sent.
		reqs := fb.requestsTo(queuesPath)
		if len(reqs) != 1 {
			t.Fatalf("queue list %v: want one request to the listing, got %d", tc.args, len(reqs))
		}
		if q := reqs[0].Query.Encode(); q != "" {
			t.Errorf("queue list %v sent the query %q, want none", tc.args, q)
		}
	}
}
