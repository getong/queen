package cmd

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// `queenctl lag`, `cg list` and `cg lag` against the shapes a 2.x broker sends.
//
// Their table read `queue`, `lagMessages` and `lagSeconds`, keys no broker has
// sent: QUEUE, MESSAGES and LAG printed `-` on every row. With --min-seconds
// the same table was put on another route, whose rows are partitions under
// snake_case keys, and every cell printed `-`, the group's name included. The
// two bodies below were captured from a broker (2.2.0) at the same moment.

// GET /api/v1/consumer-groups: a bare array, one row per (group, queue) that
// holds a cursor. digest conflates, done has consumed everything, audit is 12
// seconds behind where the others are 8 minutes, and the first three groups
// registered no subscription.
const lagGroupsBody = `[
  {"conflation":false,"kind":"queen","maxTimeLag":482,"members":1,"name":"__QUEUE_MODE__","partitionCursors":1,"partitionsWithLag":1,"queueName":"sc.queuemode","state":"Lagging","storage":"segments","subscriptionCreatedAt":null,"subscriptionMode":null,"subscriptionTimestamp":null,"topics":["sc.queuemode"],"totalLag":5},
  {"conflation":false,"kind":"queen","maxTimeLag":12,"members":2,"name":"audit","partitionCursors":2,"partitionsWithLag":1,"queueName":"sc.orders","state":"Stable","storage":"segments","subscriptionCreatedAt":null,"subscriptionMode":null,"subscriptionTimestamp":null,"topics":["sc.orders"],"totalLag":1},
  {"conflation":false,"kind":"queen","maxTimeLag":482,"members":4,"name":"billing","partitionCursors":4,"partitionsWithLag":4,"queueName":"sc.orders","state":"Lagging","storage":"segments","subscriptionCreatedAt":null,"subscriptionMode":null,"subscriptionTimestamp":null,"topics":["sc.orders"],"totalLag":33},
  {"conflation":true,"kind":"queen","maxTimeLag":482,"members":1,"name":"digest","partitionCursors":1,"partitionsWithLag":1,"queueName":"sc.emails","state":"Lagging","storage":"segments","subscriptionCreatedAt":"2026-10-13T11:07:50.510855Z","subscriptionMode":"all","subscriptionTimestamp":"1970-01-01T00:00:00.000000Z","topics":["sc.emails"],"totalLag":5},
  {"conflation":false,"kind":"queen","maxTimeLag":0,"members":1,"name":"done","partitionCursors":1,"partitionsWithLag":0,"queueName":"sc.emails","state":"Stable","storage":"segments","subscriptionCreatedAt":"2026-10-13T11:07:40.765780Z","subscriptionMode":"all","subscriptionTimestamp":"1970-01-01T00:00:00.000000Z","topics":["sc.emails"],"totalLag":0},
  {"conflation":false,"kind":"queen","maxTimeLag":482,"members":1,"name":"smtp","partitionCursors":1,"partitionsWithLag":1,"queueName":"sc.emails","state":"Lagging","storage":"segments","subscriptionCreatedAt":"2026-10-13T11:00:49.635040Z","subscriptionMode":"all","subscriptionTimestamp":"1970-01-01T00:00:00.000000Z","topics":["sc.emails"],"totalLag":4}
]`

// GET /api/v1/consumer-groups/lagging?minLagSeconds=5: a bare array, one row
// per partition cursor more than 5 seconds behind, oldest first. Only the two
// cursors that took a lease name a worker, and done is not here.
const lagPartitionsBody = `[
  {"consumer_group":"digest","kind":"queen","lag_hours":0.13,"last_consumed_at":null,"offset_lag":5,"oldest_unconsumed_at":"2026-10-13T11:00:49.501Z","partition_id":"01a134ec-14de-7000-9df2-7f1d9830de19","partition_name":"Default","queue_name":"sc.emails","time_lag_seconds":482,"worker_id":"01a134f2-816e-7001-98e2-5e604c2453c2"},
  {"consumer_group":"smtp","kind":"queen","lag_hours":0.13,"last_consumed_at":null,"offset_lag":4,"oldest_unconsumed_at":"2026-10-13T11:00:49.501Z","partition_id":"01a134ec-14de-7000-9df2-7f1d9830de19","partition_name":"Default","queue_name":"sc.emails","time_lag_seconds":482,"worker_id":null},
  {"consumer_group":"billing","kind":"queen","lag_hours":0.13,"last_consumed_at":null,"offset_lag":4,"oldest_unconsumed_at":"2026-10-13T11:00:49.438Z","partition_id":"01a134ec-149e-7000-9450-c2c55a123b4b","partition_name":"c0","queue_name":"sc.orders","time_lag_seconds":482,"worker_id":null},
  {"consumer_group":"billing","kind":"queen","lag_hours":0.13,"last_consumed_at":null,"offset_lag":10,"oldest_unconsumed_at":"2026-10-13T11:00:49.454Z","partition_id":"01a134ec-14af-7000-a1ed-0dd5def0b8e0","partition_name":"c1","queue_name":"sc.orders","time_lag_seconds":482,"worker_id":null},
  {"consumer_group":"billing","kind":"queen","lag_hours":0.13,"last_consumed_at":null,"offset_lag":9,"oldest_unconsumed_at":"2026-10-13T11:00:49.470Z","partition_id":"01a134ec-14be-700c-9b5d-f1b4f7271edd","partition_name":"c2","queue_name":"sc.orders","time_lag_seconds":482,"worker_id":null},
  {"consumer_group":"billing","kind":"queen","lag_hours":0.13,"last_consumed_at":null,"offset_lag":10,"oldest_unconsumed_at":"2026-10-13T11:00:49.486Z","partition_id":"01a134ec-14ce-700b-86ef-7f4b8e733e21","partition_name":"c3","queue_name":"sc.orders","time_lag_seconds":482,"worker_id":"01a134f1-f04a-7001-b78f-8786acd4a54e"},
  {"consumer_group":"__QUEUE_MODE__","kind":"queen","lag_hours":0.13,"last_consumed_at":null,"offset_lag":5,"oldest_unconsumed_at":"2026-10-13T11:00:49.516Z","partition_id":"01a134ec-14ec-7009-b365-5e53692885e3","partition_name":"Default","queue_name":"sc.queuemode","time_lag_seconds":482,"worker_id":null},
  {"consumer_group":"audit","kind":"queen","lag_hours":0.0,"last_consumed_at":null,"offset_lag":1,"oldest_unconsumed_at":"2026-10-13T11:08:39.697Z","partition_id":"01a134ec-14af-7000-a1ed-0dd5def0b8e0","partition_name":"c1","queue_name":"sc.orders","time_lag_seconds":12,"worker_id":null}
]`

const (
	groupsPath  = "/api/v1/consumer-groups"
	laggingPath = "/api/v1/consumer-groups/lagging"
)

func lagBroker(t *testing.T) *fakeBroker {
	t.Helper()
	fb := newFakeBroker(t)
	fb.route(groupsPath, lagGroupsBody)
	fb.route(laggingPath, lagPartitionsBody)
	return fb
}

// runLag runs `queenctl <args>` and returns what it printed. --min-seconds
// lands in package variables cobra writes only when the flag is typed, and
// `cg list` runs lag's RunE, so a threshold from one run would send the next
// to the other route: they are put back around every run.
func runLag(t *testing.T, server string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	reset := func() { lagMinSeconds, cgLagMinSec = 0, 0 }
	reset()
	t.Cleanup(reset)
	stop := captureStdio(t)
	err = runCLI(t, server, args...)
	stdout, stderr = stop()
	return stdout, stderr, err
}

// wantTable compares a printed table with its cells, row by row. A cell that
// prints several words, an age above all, is that many cells here.
func wantTable(t *testing.T, stdout string, want [][]string) {
	t.Helper()
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

// wantBrokerRows checks that -o json printed the rows of body as they came.
func wantBrokerRows(t *testing.T, stdout, body string) {
	t.Helper()
	var got, want []map[string]any
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("-o json is not an array of rows: %v\n%s", err, stdout)
	}
	if err := json.Unmarshal([]byte(body), &want); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("-o json rows:\n got %v\nwant %v", got, want)
	}
}

func TestLagTableShowsQueueMessagesAndLag(t *testing.T) {
	want := [][]string{
		{"CG", "QUEUE", "MESSAGES", "LAG", "CONFLATION"},
		{"__QUEUE_MODE__", "sc.queuemode", "5", "8m2s", "-"},
		{"audit", "sc.orders", "1", "12s", "-"},
		{"billing", "sc.orders", "33", "8m2s", "-"},
		{"digest", "sc.emails", "5", "8m2s", "yes"},
		// Caught up: a zero, where a group the broker says nothing about
		// would print `-`.
		{"done", "sc.emails", "0", "0s", "-"},
		{"smtp", "sc.emails", "4", "8m2s", "-"},
	}
	// `cg list` is the same listing, and so is `cg lag` without a threshold.
	for _, args := range [][]string{{"lag"}, {"cg", "list"}, {"cg", "lag"}} {
		fb := lagBroker(t)
		stdout, _, err := runLag(t, fb.url(), append(args, "-o", "table")...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		wantTable(t, stdout, want)
		if reqs := fb.requestsTo(laggingPath); len(reqs) != 0 {
			t.Errorf("%v called the lagging route without --min-seconds", args)
		}
	}
}

func TestLagWideShowsCursorsAndRegistration(t *testing.T) {
	fb := lagBroker(t)
	stdout, _, err := runLag(t, fb.url(), "lag", "-o", "wide")
	if err != nil {
		t.Fatalf("lag -o wide: %v", err)
	}
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) != 7 {
		t.Fatalf("want a header and 6 groups, got %d lines:\n%s", len(lines), stdout)
	}
	if got, want := strings.Fields(lines[0]), []string{"CG", "QUEUE", "MESSAGES", "LAG", "CONFLATION", "PARTITIONS", "SUBSCRIBED"}; !reflect.DeepEqual(got, want) {
		t.Errorf("header = %v, want %v", got, want)
	}
	// PARTITIONS is the group's cursors on the queue. SUBSCRIBED is an age, so
	// only the groups that registered nothing have a fixed cell.
	for i, want := range [][]string{
		{"__QUEUE_MODE__", "sc.queuemode", "5", "8m2s", "-", "1", "-"},
		{"audit", "sc.orders", "1", "12s", "-", "2", "-"},
		{"billing", "sc.orders", "33", "8m2s", "-", "4", "-"},
	} {
		if got := strings.Fields(lines[i+1]); !reflect.DeepEqual(got, want) {
			t.Errorf("line %d = %v, want %v", i+1, got, want)
		}
	}
	// smtp reads from the first message, so its subscriptionTimestamp is the
	// epoch: the column is when it registered, an age and not "a long while
	// ago".
	smtp := strings.Fields(lines[6])
	if len(smtp) < 7 || !reflect.DeepEqual(smtp[:6], []string{"smtp", "sc.emails", "4", "8m2s", "-", "1"}) {
		t.Fatalf("smtp line = %v", smtp)
	}
	if age := strings.Join(smtp[6:], " "); !strings.HasSuffix(age, " ago") || strings.Contains(age, "long while") {
		t.Errorf("smtp SUBSCRIBED = %q, want the age of its registration", age)
	}
}

func TestLagJSONKeepsTheBrokersRows(t *testing.T) {
	// The rows always carried real values under the broker's keys: scripts
	// read them, and the table is what changed.
	fb := lagBroker(t)
	stdout, _, err := runLag(t, fb.url(), "lag", "-o", "json")
	if err != nil {
		t.Fatalf("lag -o json: %v", err)
	}
	wantBrokerRows(t, stdout, lagGroupsBody)
}

func TestLagMinSecondsListsPartitions(t *testing.T) {
	want := [][]string{
		{"CG", "QUEUE", "PARTITION", "MESSAGES", "LAG"},
		{"digest", "sc.emails", "Default", "5", "8m2s"},
		{"smtp", "sc.emails", "Default", "4", "8m2s"},
		{"billing", "sc.orders", "c0", "4", "8m2s"},
		{"billing", "sc.orders", "c1", "10", "8m2s"},
		{"billing", "sc.orders", "c2", "9", "8m2s"},
		{"billing", "sc.orders", "c3", "10", "8m2s"},
		{"__QUEUE_MODE__", "sc.queuemode", "Default", "5", "8m2s"},
		{"audit", "sc.orders", "c1", "1", "12s"},
	}
	for _, args := range [][]string{{"lag"}, {"cg", "lag"}} {
		fb := lagBroker(t)
		stdout, _, err := runLag(t, fb.url(), append(args, "--min-seconds", "5", "-o", "table")...)
		if err != nil {
			t.Fatalf("%v --min-seconds 5: %v", args, err)
		}
		wantTable(t, stdout, want)

		reqs := fb.requestsTo(laggingPath)
		if len(reqs) != 1 {
			t.Fatalf("%v: want one request to the lagging route, got %d", args, len(reqs))
		}
		if got := reqs[0].Query.Get("minLagSeconds"); got != "5" {
			t.Errorf("%v: minLagSeconds = %q, want 5", args, got)
		}
		if reqs := fb.requestsTo(groupsPath); len(reqs) != 0 {
			t.Errorf("%v --min-seconds also read the group listing", args)
		}
	}
}

func TestLagMinSecondsWideShowsWorkerAndOldest(t *testing.T) {
	fb := lagBroker(t)
	stdout, _, err := runLag(t, fb.url(), "lag", "--min-seconds", "5", "-o", "wide")
	if err != nil {
		t.Fatalf("lag --min-seconds 5 -o wide: %v", err)
	}
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) != 9 {
		t.Fatalf("want a header and 8 partitions, got %d lines:\n%s", len(lines), stdout)
	}
	for i, want := range map[int][]string{
		0: {"CG", "QUEUE", "PARTITION", "MESSAGES", "LAG", "WORKER", "OLDEST"},
		// OLDEST is the broker's timestamp, not an age that repeats LAG.
		2: {"smtp", "sc.emails", "Default", "4", "8m2s", "-", "2026-10-13T11:00:49.501Z"},
		6: {"billing", "sc.orders", "c3", "10", "8m2s", "01a134f1-f04a-7001-b78f-8786acd4a54e", "2026-10-13T11:00:49.486Z"},
	} {
		if got := strings.Fields(lines[i]); !reflect.DeepEqual(got, want) {
			t.Errorf("line %d = %v, want %v", i, got, want)
		}
	}
}

func TestLagMinSecondsJSONKeepsTheBrokersRows(t *testing.T) {
	fb := lagBroker(t)
	stdout, _, err := runLag(t, fb.url(), "lag", "--min-seconds", "5", "-o", "json")
	if err != nil {
		t.Fatalf("lag --min-seconds 5 -o json: %v", err)
	}
	wantBrokerRows(t, stdout, lagPartitionsBody)
}

func TestLagMinSecondsWithNothingBehind(t *testing.T) {
	fb := newFakeBroker(t)
	fb.route(laggingPath, `[]`)
	stdout, _, err := runLag(t, fb.url(), "lag", "--min-seconds", "3600", "-o", "table")
	if err != nil {
		t.Fatalf("lag --min-seconds 3600: %v", err)
	}
	wantTable(t, stdout, [][]string{{"CG", "QUEUE", "PARTITION", "MESSAGES", "LAG"}})

	stdout, _, err = runLag(t, fb.url(), "lag", "--min-seconds", "3600", "-o", "json")
	if err != nil {
		t.Fatalf("lag --min-seconds 3600 -o json: %v", err)
	}
	if got := strings.TrimSpace(stdout); got != "[]" {
		t.Errorf("-o json = %q, want an empty array", got)
	}
}
