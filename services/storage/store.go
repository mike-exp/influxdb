package storage

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type Store struct {
	TSDBStore  *tsdb.Store
	MetaClient StorageMetaClient
	Logger     *zap.Logger
}

func NewStore() *Store {
	return &Store{Logger: zap.NewNop()}
}

// WithLogger sets the logger for the service.
func (s *Store) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "store"))
}

func (s *Store) findShardIDs(database, rp string, desc bool, start, end int64) ([]uint64, error) {
	groups, err := s.MetaClient.ShardGroupsByTimeRange(database, rp, time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	}

	if len(groups) == 0 {
		return nil, nil
	}

	if desc {
		sort.Sort(sort.Reverse(meta.ShardGroupInfos(groups)))
	} else {
		sort.Sort(meta.ShardGroupInfos(groups))
	}

	shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
	for _, g := range groups {
		for _, si := range g.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}
	return shardIDs, nil
}

func (s *Store) validateArgs(database string, start, end int64) (string, string, int64, int64, error) {
	rp := ""
	if p := strings.IndexByte(database, '/'); p > -1 {
		database, rp = database[:p], database[p+1:]
	}

	s.Logger.Info("metaclient check", logger.Database(database))

	di := s.MetaClient.Database(database)
	if di == nil {
		return "", "", 0, 0, errors.New("no database")
	}

	if rp == "" {
		rp = di.DefaultRetentionPolicy
	}

	rpi := di.RetentionPolicy(rp)
	if rpi == nil {
		return "", "", 0, 0, errors.New("invalid retention policy")
	}

	if start <= 0 {
		start = models.MinNanoTime
	}
	if end <= 0 {
		end = models.MaxNanoTime
	}
	return database, rp, start, end, nil
}

func (s *Store) Read(ctx context.Context, req *ReadRequest) (*ResultSet, error) {
	database, rp, start, end, err := s.validateArgs(req.Database, req.TimestampRange.Start, req.TimestampRange.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, req.Descending, start, end)
	if err != nil {
		return nil, err
	}

	var cur seriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil {
		return nil, nil
	} else {
		cur = ic
	}

	if len(req.Grouping) > 0 {
		cur = newGroupSeriesCursor(ctx, cur, req.Grouping)
	}

	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		cur = newLimitSeriesCursor(ctx, cur, req.SeriesLimit, req.SeriesOffset)
	}

	return &ResultSet{
		req: readRequest{
			ctx:       ctx,
			start:     start,
			end:       end,
			asc:       !req.Descending,
			limit:     req.PointsLimit,
			aggregate: req.Aggregate,
		},
		cur: cur,
	}, nil
}

type tagKeysSlice []tsdb.TagKeys

func (a tagKeysSlice) Len() int           { return len(a) }
func (a tagKeysSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a tagKeysSlice) Less(i, j int) bool { return a[i].Measurement < a[j].Measurement }

func (s *Store) ReadTagKeys(ctx context.Context, req *ReadTagKeysRequest) ([]string, error) {
	database, rp, start, end, err := s.validateArgs(req.Database, req.TimestampRange.Start, req.TimestampRange.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, false, start, end)
	if err != nil {
		return nil, err
	}

	keys, err := s.TSDBStore.TagKeys(query.OpenAuthorizer, shardIDs, nil)
	if err != nil {
		return nil, err
	}

	return MergeTagKeys(keys), nil
}

func (s *Store) ReadTagKeyValues(ctx context.Context, req *ReadTagKeyValuesRequest) (interface{}, error) {
	return nil, nil
}

func MergeTagKeys(keys []tsdb.TagKeys) []string {
	if len(keys) == 1 {
		return keys[0].Keys
	} else if len(keys) == 0 {
		return nil
	}

	var s []string
	for i := range keys {
		s = append(s, keys[i].Keys...)
	}

	sort.Strings(s)

	// dedupe
	i := 1
	for j := 1; j < len(s); j++ {
		if s[i-1] != s[j] {
			s[i] = s[j]
			i++
		}
	}
	return s[:i]
}
