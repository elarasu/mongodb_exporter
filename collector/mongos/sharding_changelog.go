package collector_mongos

import (
	"time"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	shardingChangelogInfo = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:	Namespace,
		Subsystem:	"sharding",
		Name:		"changelog_10min_total",
		Help:		"Total # of Cluster Balancer log events over the last 10 minutes",
	}, []string{"event"})
)

type ShardingChangelogSummaryId struct {
	Event	string	`bson:"event"`
	Note	string	`bson:"note"`
}

type ShardingChangelogSummary struct {
	Id	*ShardingChangelogSummaryId	`bson:"_id"`
	Count 	float64				`bson:"count"`
}

type ShardingChangelogStats struct {
	Items	*[]ShardingChangelogSummary
}

func (status *ShardingChangelogStats) Export(ch chan<- prometheus.Metric) {
	// set all expected event types to zero first, so they show in results if there was no events in the current time period
	shardingChangelogInfo.WithLabelValues("moveChunk.start").Add(0)
	shardingChangelogInfo.WithLabelValues("moveChunk.to").Add(0)
	shardingChangelogInfo.WithLabelValues("moveChunk.to_failed").Add(0)
	shardingChangelogInfo.WithLabelValues("moveChunk.from").Add(0)
	shardingChangelogInfo.WithLabelValues("moveChunk.from_failed").Add(0)
	shardingChangelogInfo.WithLabelValues("moveChunk.commit").Add(0)
	shardingChangelogInfo.WithLabelValues("addShard").Add(0)
	shardingChangelogInfo.WithLabelValues("removeShard.start").Add(0)
	shardingChangelogInfo.WithLabelValues("shardCollection").Add(0)
	shardingChangelogInfo.WithLabelValues("shardCollection.start").Add(0)
	shardingChangelogInfo.WithLabelValues("split").Add(0)
	shardingChangelogInfo.WithLabelValues("multi-split").Add(0)

	// set counts for events found in our query
	for _, item := range *status.Items {
		event := item.Id.Event
		note  := item.Id.Note
		count := item.Count
		switch event {
			case "moveChunk.to":
				if note == "success" || note == "" {
					shardingChangelogInfo.WithLabelValues(event).Set(count)
				} else {
					shardingChangelogInfo.WithLabelValues(event + "_failed").Set(count)
				}
			case "moveChunk.from":
				if note == "success" || note == "" {
					shardingChangelogInfo.WithLabelValues(event).Set(count)
				} else {
					shardingChangelogInfo.WithLabelValues(event + "_failed").Set(count)
				}
			default:
				shardingChangelogInfo.WithLabelValues(event).Set(count)
		}
	}
	shardingChangelogInfo.Collect(ch)
}

func (status *ShardingChangelogStats) Describe(ch chan<- *prometheus.Desc) {
	shardingChangelogInfo.Describe(ch)
}

func GetShardingChangelogStatus(session *mgo.Session) *ShardingChangelogStats {
	var qresults []ShardingChangelogSummary
	coll  := session.DB("config").C("changelog")
	match := bson.M{ "time" : bson.M{ "$gt" : time.Now().Add(-10 * time.Minute) } }
	group := bson.M{ "_id" : bson.M{ "event" : "$what", "note" : "$details.note" }, "count" : bson.M{ "$sum" : 1 } }

	err := coll.Pipe([]bson.M{ { "$match" : match }, { "$group" : group } }).All(&qresults)
	if err != nil {
		glog.Error("Failed to execute find query on 'config.changelog'!")
	}

	results := &ShardingChangelogStats{}
	results.Items = &qresults
	return results
}
