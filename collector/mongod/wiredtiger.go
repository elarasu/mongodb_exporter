package collector_mongod

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	wtBlockManagerBlocksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_blockmanager",
		Name:      "blocks_total",
		Help:      "The total number of blocks read by the WiredTiger BlockManager",
	}, []string{"type"})
	wtBlockManagerBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_blockmanager",
		Name:      "bytes_total",
		Help:      "The total number of bytes read by the WiredTiger BlockManager",
	}, []string{"type"})
)

var (
	wtCachePages = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_cache",
		Name:      "pages",
		Help:      "The current number of pages in the WiredTiger Cache",
	}, []string{"type"})
	wtCachePagesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_cache",
		Name:      "pages_total",
		Help:      "The total number of pages read into/from the WiredTiger Cache",
	}, []string{"type"})
	wtCacheBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_cache",
		Name:      "bytes",
		Help:      "The current size of data in the WiredTiger Cache in bytes",
	}, []string{"type"})
	wtCacheMaxBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_cache",
		Name:      "max_bytes",
		Help:      "The maximum size of data in the WiredTiger Cache in bytes",
	})
	wtCacheBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_cache",
		Name:      "bytes_total",
		Help:      "The total number of bytes read into/from the WiredTiger Cache",
	}, []string{"type"})
	wtCacheEvictedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_cache",
		Name:      "evicted_total",
		Help:      "The total number of pages evicted from the WiredTiger Cache",
	}, []string{"type"})
	wtCachePercentOverhead = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_cache",
		Name:      "overhead_percent",
		Help:      "The percentage overhead of the WiredTiger Cache",
	})
)

var (
	wtTransactionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_transactions",
		Name:      "total",
		Help:      "The total number of transactions WiredTiger has handled",
	}, []string{"type"})
	wtTransactionsTotalCheckpointMs = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_transactions",
		Name:      "checkpoint_milliseconds_total",
		Help:      "The total time in milliseconds transactions have checkpointed in WiredTiger",
	})
	wtTransactionsCheckpointMs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_transactions",
		Name:      "checkpoint_milliseconds",
		Help:      "The time in milliseconds transactions have checkpointed in WiredTiger",
	}, []string{"type"})
	wtTransactionsCheckpointsRunning = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_transactions",
		Name:      "running_checkpoints",
		Help:      "The number of currently running checkpoints in WiredTiger",
	})
)

var (
	wtLogRecordsScannedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_log",
		Name:      "records_scanned_total",
		Help:      "The total number of records scanned by log scan in the WiredTiger log",
	})
	wtLogRecordsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_log",
		Name:      "records_total",
		Help:      "The total number of compressed/uncompressed records written to the WiredTiger log",
	}, []string{"type"})
	wtLogBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_log",
		Name:      "bytes_total",
		Help:      "The total number of bytes written to the WiredTiger log",
	}, []string{"type"})
	wtLogOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_log",
		Name:      "operations_total",
		Help:      "The total number of WiredTiger log operations",
	}, []string{"type"})
)

var (
	wtOpenCursors = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_session",
		Name:      "open_cursors_total",
		Help:      "The total number of cursors opened in WiredTiger",
	})
	wtOpenSessions = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_session",
		Name:      "open_sessions_total",
		Help:      "The total number of sessions opened in WiredTiger",
	})
)

var (
	wtConcurrentTransactionsOut = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_concurrent_transactions",
		Name:      "out_tickets",
		Help:      "The number of tickets that are currently in use (out) in WiredTiger",
	}, []string{"type"})
	wtConcurrentTransactionsAvailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_concurrent_transactions",
		Name:      "available_tickets",
		Help:      "The number of tickets that are available in WiredTiger",
	}, []string{"type"})
	wtConcurrentTransactionsTotalTickets = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "wiredtiger_concurrent_transactions",
		Name:      "total_tickets",
		Help:      "The total number of tickets that are available in WiredTiger",
	}, []string{"type"})
)

// blockmanager stats
type WTBlockManagerStats struct {
	MappedBytesRead  float64 `bson:"mapped bytes read"`
	BytesRead        float64 `bson:"bytes read"`
	BytesWritten     float64 `bson:"bytes written"`
	MappedBlocksRead float64 `bson:"mapped blocks read"`
	BlocksPreLoaded  float64 `bson:"blocks pre-loaded"`
	BlocksRead       float64 `bson:"blocks read"`
	BlocksWritten    float64 `bson:"blocks written"`
}

func (stats *WTBlockManagerStats) Export(ch chan<- prometheus.Metric) {
	wtBlockManagerBlocksTotal.WithLabelValues("read").Add(stats.BlocksRead)
	wtBlockManagerBlocksTotal.WithLabelValues("read_mapped").Add(stats.MappedBlocksRead)
	wtBlockManagerBlocksTotal.WithLabelValues("pre_loaded").Add(stats.BlocksPreLoaded)
	wtBlockManagerBlocksTotal.WithLabelValues("written").Add(stats.BlocksWritten)
	wtBlockManagerBytesTotal.WithLabelValues("read").Add(stats.BytesRead)
	wtBlockManagerBytesTotal.WithLabelValues("read_mapped").Add(stats.MappedBytesRead)
	wtBlockManagerBytesTotal.WithLabelValues("written").Add(stats.BytesWritten)
}

func (stats *WTBlockManagerStats) Describe(ch chan<- *prometheus.Desc) {
	wtBlockManagerBlocksTotal.Describe(ch)
	wtBlockManagerBytesTotal.Describe(ch)
}

// cache stats
type WTCacheStats struct {
	BytesTotal         float64 `bson:"bytes currently in the cache"`
	BytesDirty         float64 `bson:"tracked dirty bytes in the cache"`
	BytesInternalPages float64 `bson:"tracked bytes belonging to internal pages in the cache"`
	BytesLeafPages     float64 `bson:"tracked bytes belonging to leaf pages in the cache"`
	MaxBytes           float64 `bson:"maximum bytes configured"`
	BytesReadInto      float64 `bson:"bytes read into cache"`
	BytesWrittenFrom   float64 `bson:"bytes written from cache"`
	EvictedUnmodified  float64 `bson:"unmodified pages evicted"`
	EvictedModified    float64 `bson:"modified pages evicted"`
	PercentOverhead    float64 `bson:"percentage overhead"`
	PagesTotal         float64 `bson:"pages currently held in the cache"`
	PagesReadInto      float64 `bson:"pages read into cache"`
	PagesWrittenFrom   float64 `bson:"pages written from cache"`
	PagesDirty         float64 `bson:"tracked dirty pages in the cache"`
}

func (stats *WTCacheStats) Export(ch chan<- prometheus.Metric) {
	wtCachePagesTotal.WithLabelValues("read").Add(stats.PagesReadInto)
	wtCachePagesTotal.WithLabelValues("written").Add(stats.PagesWrittenFrom)
	wtCacheBytesTotal.WithLabelValues("read").Add(stats.BytesReadInto)
	wtCacheBytesTotal.WithLabelValues("written").Add(stats.BytesWrittenFrom)
	wtCacheEvictedTotal.WithLabelValues("modified").Add(stats.EvictedModified)
	wtCacheEvictedTotal.WithLabelValues("unmodified").Add(stats.EvictedUnmodified)
	wtCachePages.WithLabelValues("total").Set(stats.PagesTotal)
	wtCachePages.WithLabelValues("dirty").Set(stats.PagesDirty)
	wtCacheBytes.WithLabelValues("total").Set(stats.BytesTotal)
	wtCacheBytes.WithLabelValues("dirty").Set(stats.BytesDirty)
	wtCacheBytes.WithLabelValues("internal_pages").Set(stats.BytesInternalPages)
	wtCacheBytes.WithLabelValues("leaf_pages").Set(stats.BytesLeafPages)
	wtCacheMaxBytes.Set(stats.MaxBytes)
	wtCachePercentOverhead.Set(stats.PercentOverhead)
}

func (stats *WTCacheStats) Describe(ch chan<- *prometheus.Desc) {
	wtCachePagesTotal.Describe(ch)
	wtCacheEvictedTotal.Describe(ch)
	wtCachePages.Describe(ch)
	wtCacheBytes.Describe(ch)
	wtCacheMaxBytes.Describe(ch)
	wtCachePercentOverhead.Describe(ch)
}

// log stats
type WTLogStats struct {
	TotalBufferSize         float64 `bson:"total log buffer size"`
	TotalSizeCompressed     float64 `bson:"total size of compressed records"`
	BytesPayloadData        float64 `bson:"log bytes of payload data"`
	BytesWritten            float64 `bson:"log bytes written"`
	RecordsUncompressed     float64 `bson:"log records not compressed"`
	RecordsCompressed       float64 `bson:"log records compressed"`
	RecordsProcessedLogScan float64 `bson:"records processed by log scan"`
	MaxLogSize              float64 `bson:"maximum log file size"`
	LogFlushes              float64 `bson:"log flush operations"`
	LogReads                float64 `bson:"log read operations"`
	LogScansDouble          float64 `bson:"log scan records requiring two reads"`
	LogScans                float64 `bson:"log scan operations"`
	LogSyncs                float64 `bson:"log sync operations"`
	LogSyncDirs             float64 `bson:"log sync_dir operations"`
	LogWrites               float64 `bson:"log write operations"`
}

func (stats *WTLogStats) Export(ch chan<- prometheus.Metric) {
	wtLogRecordsTotal.WithLabelValues("compressed").Add(stats.RecordsCompressed)
	wtLogRecordsTotal.WithLabelValues("uncompressed").Add(stats.RecordsUncompressed)
	wtLogBytesTotal.WithLabelValues("payload").Add(stats.BytesPayloadData)
	wtLogBytesTotal.WithLabelValues("written").Add(stats.BytesWritten)
	wtLogOperationsTotal.WithLabelValues("read").Add(stats.LogReads)
	wtLogOperationsTotal.WithLabelValues("write").Add(stats.LogWrites)
	wtLogOperationsTotal.WithLabelValues("scan").Add(stats.LogScans)
	wtLogOperationsTotal.WithLabelValues("scan_double").Add(stats.LogScansDouble)
	wtLogOperationsTotal.WithLabelValues("sync").Add(stats.LogSyncs)
	wtLogOperationsTotal.WithLabelValues("sync_dir").Add(stats.LogSyncDirs)
	wtLogOperationsTotal.WithLabelValues("flush").Add(stats.LogFlushes)
	wtLogRecordsScannedTotal.Add(stats.RecordsProcessedLogScan)
}

func (stats *WTLogStats) Describe(ch chan<- *prometheus.Desc) {
	wtLogRecordsTotal.Describe(ch)
	wtLogBytesTotal.Describe(ch)
	wtLogOperationsTotal.Describe(ch)
	wtLogRecordsScannedTotal.Describe(ch)
}

// session stats
type WTSessionStats struct {
	Cursors  float64 `bson:"open cursor count"`
	Sessions float64 `bson:"open session count"`
}

func (stats *WTSessionStats) Export(ch chan<- prometheus.Metric) {
	wtOpenCursors.Set(stats.Cursors)
	wtOpenSessions.Set(stats.Sessions)
}

func (stats *WTSessionStats) Describe(ch chan<- *prometheus.Desc) {
	wtOpenCursors.Describe(ch)
	wtOpenSessions.Describe(ch)
}

// transaction stats
type WTTransactionStats struct {
	Begins               float64 `bson:"transaction begins"`
	Checkpoints          float64 `bson:"transaction checkpoints"`
	CheckpointsRunning   float64 `bson:"transaction checkpoint currently running"`
	CheckpointMaxMs      float64 `bson:"transaction checkpoint max time (msecs)"`
	CheckpointMinMs      float64 `bson:"transaction checkpoint min time (msecs)"`
	CheckpointLastMs     float64 `bson:"transaction checkpoint most recent time (msecs)"`
	CheckpointTotalMs    float64 `bson:"transaction checkpoint total time (msecs)"`
	Committed            float64 `bson:"transactions committed"`
	CacheOverflowFailure float64 `bson:"transaction failures due to cache overflow"`
	RolledBack           float64 `bson:"transactions rolled back"`
}

func (stats *WTTransactionStats) Export(ch chan<- prometheus.Metric) {
	wtTransactionsTotal.WithLabelValues("begins").Add(stats.Begins)
	wtTransactionsTotal.WithLabelValues("checkpoints").Add(stats.Checkpoints)
	wtTransactionsTotal.WithLabelValues("committed").Add(stats.Committed)
	wtTransactionsTotal.WithLabelValues("rolledback").Add(stats.RolledBack)
	wtTransactionsCheckpointMs.WithLabelValues("min").Set(stats.CheckpointMinMs)
	wtTransactionsCheckpointMs.WithLabelValues("max").Set(stats.CheckpointMaxMs)
	wtTransactionsTotalCheckpointMs.Add(stats.CheckpointTotalMs)
	wtTransactionsCheckpointsRunning.Set(stats.CheckpointsRunning)
}

func (stats *WTTransactionStats) Describe(ch chan<- *prometheus.Desc) {
	wtTransactionsTotal.Describe(ch)
	wtTransactionsTotalCheckpointMs.Describe(ch)
	wtTransactionsCheckpointMs.Describe(ch)
	wtTransactionsCheckpointsRunning.Describe(ch)
}

// concurrenttransaction stats
type WTConcurrentTransactionsTypeStats struct {
	Out          float64 `bson:"out"`
	Available    float64 `bson:"available"`
	TotalTickets float64 `bson:"totalTickets"`
}

type WTConcurrentTransactionsStats struct {
	Write *WTConcurrentTransactionsTypeStats `bson:"read"`
	Read  *WTConcurrentTransactionsTypeStats `bson:"write"`
}

func (stats *WTConcurrentTransactionsStats) Export(ch chan<- prometheus.Metric) {
	wtConcurrentTransactionsOut.WithLabelValues("read").Set(stats.Read.Out)
	wtConcurrentTransactionsOut.WithLabelValues("write").Set(stats.Write.Out)
	wtConcurrentTransactionsAvailable.WithLabelValues("read").Set(stats.Read.Available)
	wtConcurrentTransactionsAvailable.WithLabelValues("write").Set(stats.Write.Available)
	wtConcurrentTransactionsTotalTickets.WithLabelValues("read").Set(stats.Read.TotalTickets)
	wtConcurrentTransactionsTotalTickets.WithLabelValues("write").Set(stats.Write.TotalTickets)
}

func (stats *WTConcurrentTransactionsStats) Describe(ch chan<- *prometheus.Desc) {
	wtConcurrentTransactionsOut.Describe(ch)
	wtConcurrentTransactionsAvailable.Describe(ch)
	wtConcurrentTransactionsTotalTickets.Describe(ch)
}

// WiredTiger stats
type WiredTigerStats struct {
	BlockManager           *WTBlockManagerStats           `bson:"block-manager"`
	Cache                  *WTCacheStats                  `bson:"cache"`
	Log                    *WTLogStats                    `bson:"log"`
	Session                *WTSessionStats                `bson:"session"`
	Transaction            *WTTransactionStats            `bson:"transaction"`
	ConcurrentTransactions *WTConcurrentTransactionsStats `bson:"concurrentTransactions"`
}

func (stats *WiredTigerStats) Describe(ch chan<- *prometheus.Desc) {
	if stats.BlockManager != nil {
		stats.BlockManager.Describe(ch)
	}
	if stats.Cache != nil {
		stats.Cache.Describe(ch)
	}
	if stats.Transaction != nil {
		stats.Transaction.Describe(ch)
	}
	if stats.Log != nil {
		stats.Log.Describe(ch)
	}
	if stats.Session != nil {
		stats.Session.Describe(ch)
	}
	if stats.ConcurrentTransactions != nil {
		stats.ConcurrentTransactions.Describe(ch)
	}
}

func (stats *WiredTigerStats) Export(ch chan<- prometheus.Metric) {
	if stats.BlockManager != nil {
		stats.BlockManager.Export(ch)
	}
	if stats.Cache != nil {
		stats.Cache.Export(ch)
	}
	if stats.Transaction != nil {
		stats.Transaction.Export(ch)
	}
	if stats.Log != nil {
		stats.Log.Export(ch)
	}
	if stats.Session != nil {
		stats.Session.Export(ch)
	}
	if stats.ConcurrentTransactions != nil {
		stats.ConcurrentTransactions.Export(ch)
	}

	wtBlockManagerBlocksTotal.Collect(ch)
	wtBlockManagerBytesTotal.Collect(ch)

	wtCachePagesTotal.Collect(ch)
	wtCacheBytesTotal.Collect(ch)
	wtCacheEvictedTotal.Collect(ch)
	wtCachePages.Collect(ch)
	wtCacheBytes.Collect(ch)
	wtCacheMaxBytes.Collect(ch)
	wtCachePercentOverhead.Collect(ch)

	wtTransactionsTotal.Collect(ch)
	wtTransactionsTotalCheckpointMs.Collect(ch)
	wtTransactionsCheckpointMs.Collect(ch)
	wtTransactionsCheckpointsRunning.Collect(ch)

	wtLogRecordsTotal.Collect(ch)
	wtLogBytesTotal.Collect(ch)
	wtLogOperationsTotal.Collect(ch)
	wtLogRecordsScannedTotal.Collect(ch)

	wtOpenCursors.Collect(ch)
	wtOpenSessions.Collect(ch)

	wtConcurrentTransactionsOut.Collect(ch)
	wtConcurrentTransactionsAvailable.Collect(ch)
	wtConcurrentTransactionsTotalTickets.Collect(ch)
}
