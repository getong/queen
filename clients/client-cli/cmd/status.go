package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"

	clierr "github.com/smartpricing/queen/clients/client-cli/v2/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/v2/internal/output"
	"github.com/smartpricing/queen/clients/client-cli/v2/internal/sdk"
	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/spf13/cobra"
)

var (
	statusNamespace  string
	statusTask       string
	statusPartitions bool
)

var statusCmd = &cobra.Command{
	Use:   "status [queue]",
	Short: "Show cluster overview or per-queue depth and lag",
	Long: `With no argument, prints every queue with its depth (pending,
processing, completed, dead-lettered) and its lag. With a queue name, prints
that one queue and its partition count; -o json carries the partitions
themselves.

PENDING is what waits for a consumer and PROCESSING what is leased. LAG is the
largest time lag among the consumer groups reading the queue, queue mode
included, and stays empty for a queue no group has read: 'queenctl lag' lists
the groups one by one.`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()
		if len(args) == 1 {
			data, err := c.A.GetQueueDetail(ctx, args[0], statusPartitions)
			if err != nil {
				return err
			}
			lags := queueLags(ctx, c)
			view := output.View{
				Columns: []output.Column{
					{Header: "QUEUE"},
					{Header: "PENDING"},
					{Header: "PROCESSING"},
					{Header: "COMPLETED"},
					{Header: "DLQ"},
					{Header: "PARTITIONS"},
					{Header: "LAG", Path: "lagSeconds", Format: output.HumanDuration},
				},
				RowsFrom: func(d any) []any {
					m := output.AsMap(d)
					if m == nil {
						return nil
					}
					return []any{queueDetailRow(m, lags)}
				},
			}
			r, err := rendererFor(view, stdout())
			if err != nil {
				return err
			}
			return r.Render(data)
		}
		// The queue listing, not GET /api/v1/status: that route's queue rows
		// carry a name, its labels and a partition count, and its message
		// counts are cluster totals. The listing is what the dashboard's
		// queue table reads, and a proxy serves it to a tenant credential.
		data, err := c.A.ListQueues(ctx, queen.ListQueuesParams{})
		if err != nil {
			return clierr.Server(err)
		}
		lags := queueLags(ctx, c)
		view := output.View{
			Columns: []output.Column{
				{Header: "QUEUE"},
				{Header: "NAMESPACE"},
				{Header: "TASK", Wide: true},
				{Header: "PENDING"},
				{Header: "PROCESSING"},
				{Header: "COMPLETED"},
				{Header: "DLQ"},
				{Header: "LAG", Path: "lagSeconds", Format: output.HumanDuration},
			},
			RowsFrom: func(d any) []any {
				return queueListRows(d, statusNamespace, statusTask, lags)
			},
		}
		r, err := rendererFor(view, stdout())
		if err != nil {
			return err
		}
		return r.Render(data)
	},
}

// queueLags is each queue's lag in seconds: the largest maxTimeLag among its
// rows of GET /api/v1/consumer-groups, the listing 'queenctl lag' prints. The
// broker has no per-queue lag of its own, and a group shows up there once it
// holds a cursor, so a queue nobody has read has no entry.
//
// That listing walks every cursor of the tenant. When it fails the depth
// columns are still worth printing: the failure goes to stderr and LAG stays
// empty.
func queueLags(ctx context.Context, c *sdk.Client) map[string]float64 {
	data, err := c.A.ListConsumerGroups(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "queenctl: consumer group lag unavailable, LAG left empty: %v\n", err)
		return nil
	}
	lags := map[string]float64{}
	for _, r := range consumerGroupsView().RowsFrom(data) {
		m := output.AsMap(r)
		if m == nil {
			continue
		}
		name, _ := m["queueName"].(string)
		lag, ok := m["maxTimeLag"].(float64)
		if name == "" || !ok {
			continue
		}
		if cur, seen := lags[name]; !seen || lag > cur {
			lags[name] = lag
		}
	}
	return lags
}

// queueListRows turns GET /api/v1/resources/queues into the status rows, by
// queue name. The route answers every queue of the tenant whatever the query
// says, so --namespace and --task are applied here.
func queueListRows(d any, namespace, task string, lags map[string]float64) []any {
	rows := output.AsArray(d, "queues")
	out := make([]any, 0, len(rows))
	for _, r := range rows {
		m := output.AsMap(r)
		if m == nil {
			continue
		}
		if ns, _ := m["namespace"].(string); namespace != "" && ns != namespace {
			continue
		}
		if t, _ := m["task"].(string); task != "" && t != task {
			continue
		}
		row := queueRow(m, output.AsMap(m["messages"]), lags)
		row["partitions"] = m["partitions"]
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := out[i].(map[string]any)["queue"].(string)
		b, _ := out[j].(map[string]any)["queue"].(string)
		return a < b
	})
	return out
}

// queueDetailRow flattens GET /api/v1/status/queues/:queue, which nests the
// queue's identity under `queue` and its counts under `totals.messages`.
//
// Its pending counts the leased messages too, where the listing's does not:
// they are taken out here, so that 'status' and 'status <queue>' print the same
// PENDING for the same queue.
func queueDetailRow(m map[string]any, lags map[string]float64) map[string]any {
	msgs := output.AsMap(output.AsMap(m["totals"])["messages"])
	row := queueRow(output.AsMap(m["queue"]), msgs, lags)
	pending, hasPending := msgs["pending"].(float64)
	processing, hasProcessing := msgs["processing"].(float64)
	if hasPending && hasProcessing {
		row["pending"] = max(pending-processing, 0)
	}
	row["partitions"] = m["partitions"]
	return row
}

// queueRow is the part of a status row both routes fill the same way: q is the
// queue's identity and msgs its message counts. There is no `failed`: a failed
// ack sends its message back to pending or to the DLQ, and the broker keeps no
// count of them per queue.
func queueRow(q, msgs map[string]any, lags map[string]float64) map[string]any {
	name, _ := q["name"].(string)
	row := map[string]any{
		"queue":      name,
		"namespace":  output.FirstNonNil(q, "namespace"),
		"task":       output.FirstNonNil(q, "task"),
		"pending":    msgs["pending"],
		"processing": msgs["processing"],
		"completed":  msgs["completed"],
		"dlq":        msgs["deadLetter"],
		"lagSeconds": nil,
	}
	if lag, ok := lags[name]; ok {
		row["lagSeconds"] = lag
	}
	return row
}

func init() {
	statusCmd.Flags().StringVar(&statusNamespace, "namespace", "", "filter by namespace")
	statusCmd.Flags().StringVar(&statusTask, "task", "", "filter by task")
	statusCmd.Flags().BoolVar(&statusPartitions, "partitions", false, "include per-partition detail")
	rootCmd.AddCommand(statusCmd)
}
