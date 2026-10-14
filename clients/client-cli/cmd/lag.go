package cmd

import (
	"context"

	"github.com/smartpricing/queen/clients/client-cli/v2/internal/output"
	"github.com/spf13/cobra"
)

var lagMinSeconds int

var lagCmd = &cobra.Command{
	Use:   "lag",
	Short: "List consumer groups with their lag",
	Long: `Shows every consumer group, one row per queue it reads: MESSAGES is what
the group has not consumed yet and LAG the age of the oldest of those messages.

With --min-seconds N it lists partitions instead: one row per partition a
group is more than N seconds behind on, oldest first, which is the listing an
alerting pipeline wants. Those rows say nothing about conflation: the listing
without the flag does.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()
		var data map[string]any
		view := consumerGroupsView()
		if lagMinSeconds > 0 {
			data, err = c.A.GetLaggingConsumers(ctx, lagMinSeconds)
			view = laggingPartitionsView()
		} else {
			data, err = c.A.ListConsumerGroups(ctx)
		}
		if err != nil {
			return err
		}
		r, err := rendererFor(view, stdout())
		if err != nil {
			return err
		}
		return r.Render(data)
	},
}

// consumerGroupsView is the table shape shared by `queenctl lag` and
// `queenctl cg list` (which reuses this RunE): the rows of GET
// /api/v1/consumer-groups, one per (group, queue) that holds a cursor, under
// the broker's own keys. JSON prints those rows as they came.
//
// CONFLATION is not decoration: it changes how the two columns to its left are
// read. A conflating group's MESSAGES is log depth — positions still to retire —
// while the work left is one handler run per non-empty partition
// (PLAN_CONFLATION §5.3). A group sitting at four million messages is healthy
// when that column says "yes" and an incident when it says "-", and until
// §2.6 taught the consumer-groups route to report conflation there was no way
// to tell the two apart from here.
//
// SUBSCRIBED is when the group registered on the queue. subscriptionTimestamp
// is where it started reading, which is the epoch for a group that reads from
// the first message.
func consumerGroupsView() output.View {
	return output.View{
		Columns: []output.Column{
			{Header: "CG", Path: "name"},
			{Header: "QUEUE", Path: "queueName"},
			{Header: "MESSAGES", Path: "totalLag", Format: output.HumanInt},
			{Header: "LAG", Path: "maxTimeLag", Format: output.HumanDuration},
			{Header: "CONFLATION", Path: "conflation", Format: output.Flag},
			{Header: "PARTITIONS", Path: "partitionCursors", Format: output.HumanInt, Wide: true},
			{Header: "SUBSCRIBED", Path: "subscriptionCreatedAt", Wide: true},
		},
		RowsFrom: consumerGroupRows,
	}
}

// laggingPartitionsView is the table of `queenctl lag --min-seconds N`. GET
// /api/v1/consumer-groups/lagging is not the group listing with a filter: it
// answers one row per partition cursor whose oldest unconsumed message is more
// than N seconds old, oldest first, under snake_case keys of its own. So every
// row names its partition, and the columns are the ones the dashboard's
// lagging-partitions table has.
//
// No CONFLATION column: the route carries no policy field, and a column that
// printed "-" on every row would say a conflating group is not one.
func laggingPartitionsView() output.View {
	return output.View{
		Columns: []output.Column{
			{Header: "CG", Path: "consumer_group"},
			{Header: "QUEUE", Path: "queue_name"},
			{Header: "PARTITION", Path: "partition_name"},
			{Header: "MESSAGES", Path: "offset_lag", Format: output.HumanInt},
			{Header: "LAG", Path: "time_lag_seconds", Format: output.HumanDuration},
			{Header: "WORKER", Path: "worker_id", Wide: true},
			// The timestamp as the broker wrote it: rendered as an age it
			// would repeat LAG.
			{Header: "OLDEST", Path: "oldest_unconsumed_at", Format: verbatim, Wide: true},
		},
		RowsFrom: consumerGroupRows,
	}
}

// consumerGroupRows finds the rows of either listing. Both routes answer a
// bare array, which the Go SDK hands over as {"data": [...]}.
func consumerGroupRows(d any) []any {
	if rows := output.AsArray(d, "consumerGroups"); rows != nil {
		return rows
	}
	if rows := output.AsArray(d, "groups"); rows != nil {
		return rows
	}
	if rows := output.AsArray(d, "data"); rows != nil {
		return rows
	}
	if arr, ok := d.([]any); ok {
		return arr
	}
	return nil
}

// verbatim prints a string cell as it is, where the default would turn a
// timestamp into "3 minutes ago".
func verbatim(v any) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return "-"
}

func init() {
	lagCmd.Flags().IntVar(&lagMinSeconds, "min-seconds", 0, "list the partitions a group is more than N seconds behind on, one row each")
	rootCmd.AddCommand(lagCmd)
}
