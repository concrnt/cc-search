package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/meilisearch/meilisearch-go"
	"github.com/redis/go-redis/v9"
	"github.com/totegamma/concurrent/cdid"
	"github.com/totegamma/concurrent/core"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	db_dsn          = ""
	meilisearch_url = ""
	meilisearch_key = ""
	meilisearch_idx = ""
	redis_url       = ""
	port            = 8000
)

var (
	version      = "unknown"
	buildMachine = "unknown"
	buildTime    = "unknown"
	goVersion    = "unknown"
)

var indexing int32 = 0

type searchResult struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
}

type messageRecord struct {
	ID        string   `json:"id"`
	Type      string   `json:"type"`
	Body      any      `json:"body"`
	Schema    string   `json:"schema"`
	SignedAt  int64    `json:"signedAt"`
	Signer    string   `json:"signer"`
	Timelines []string `json:"timelines"`
}

func indexLogs(ctx context.Context, db *gorm.DB, rdb *redis.Client, index meilisearch.IndexManager) {

	if atomic.CompareAndSwapInt32(&indexing, 0, 1) {
		defer atomic.StoreInt32(&indexing, 0)
	} else {
		return
	}

	lastKeyStr, err := rdb.Get(ctx, "ccsearch:readitr").Result()
	if err != nil {
		log.Println("lastKey not found")
		lastKeyStr = "0"
	}

	lastKey64, err := strconv.ParseUint(lastKeyStr, 10, 64)
	if err != nil {
		log.Println("lastKey is not integer")
		lastKey64 = 0
	}

	lastKey := uint(lastKey64)

	pageSize := 512

	for {
		var commits []core.CommitLog
		db.Where("id > ?", lastKey).Limit(pageSize).Find(&commits)

		documents := []messageRecord{}

		for _, commit := range commits {

			document := commit.Document

			var doc core.DocumentBase[any]
			err := json.Unmarshal([]byte(document), &doc)
			if err != nil {
				continue
			}

			hash := core.GetHash([]byte(document))
			hash10 := [10]byte{}
			copy(hash10[:], hash[:10])
			signedAt := doc.SignedAt
			cdidBase := cdid.New(hash10, signedAt).String()

			switch doc.Type {
			case "message":
				{
					id := "m" + cdidBase
					var message core.MessageDocument[any]
					err := json.Unmarshal([]byte(document), &message)
					if err != nil {
						log.Println(err)
						continue
					}
					documents = append(documents, messageRecord{
						ID:        id,
						Type:      "message",
						Body:      message.Body,
						Schema:    message.Schema,
						SignedAt:  message.SignedAt.UnixMilli(),
						Signer:    message.Signer,
						Timelines: message.Timelines,
					})
				}
			}

			lastKey = commit.ID
		}

		if len(documents) == 0 {
			break
		}

		_, err := index.AddDocuments(documents)
		if err != nil {
			log.Println(err)
			break
		}

		rdb.Set(ctx, "ccsearch:readitr", lastKey, 0)
		log.Println("indexed until -> ", lastKey)

		if len(commits) < pageSize { // no more commits
			break
		}

		time.Sleep(1 * time.Second)
	}
}

func main() {

	db_dsn = os.Getenv("DB_DSN")
	redis_url = os.Getenv("REDIS_URL")
	meilisearch_url = os.Getenv("MEILISEARCH_URL")
	meilisearch_key = os.Getenv("MEILISEARCH_KEY")
	meilisearch_idx = os.Getenv("MEILISEARCH_IDX")
	port_env := os.Getenv("PORT")
	if port_env != "" {
		port, _ = strconv.Atoi(port_env)
	}

	e := echo.New()

	db, err := gorm.Open(postgres.Open(db_dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redis_url,
		Password: "",
		DB:       0,
	})

	client := meilisearch.New(meilisearch_url, meilisearch.WithAPIKey(meilisearch_key))
	_, err = client.GetIndex(meilisearch_idx)
	if err != nil {
		_, err = client.CreateIndex(&meilisearch.IndexConfig{
			Uid: meilisearch_idx,
		})
		if err != nil {
			panic(err)
		}
	}

	index := client.Index(meilisearch_idx)
	filterables, err := index.GetFilterableAttributes()
	if err != nil {
		panic(err)
	}
	filters := []string{"signer", "timelines"}

	ok := false
	if len(*filterables) == len(filters) {
		for _, filter := range filters {
			if !slices.Contains(*filterables, filter) {
				ok = false
				break
			}
		}
		ok = true
	}
	if !ok {
		_, err := index.UpdateFilterableAttributes(&filters)
		if err != nil {
			panic(err)
		}
		log.Println("filterables updated")
	}

	sorts := []string{"signedAt"}
	sortables, err := index.GetSortableAttributes()
	if err != nil {
		panic(err)
	}

	ok = false
	if len(*sortables) == len(sorts) {
		for _, sort := range sorts {
			if !slices.Contains(*sortables, sort) {
				ok = false
				break
			}
		}
		ok = true
	}
	if !ok {
		_, err := index.UpdateSortableAttributes(&sorts)
		if err != nil {
			panic(err)
		}
		log.Println("sortables updated")
	}

	ctx := context.Background()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				go indexLogs(ctx, db, rdb, index)
			}
		}
	}()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	e.GET("/cc-info", func(c echo.Context) error {
		return c.JSON(http.StatusOK, core.CCInfo{
			Name:    "github.com/concrnt/cc-search",
			Version: version,
		})
	})

	e.GET("/timeline/:id", func(c echo.Context) error {
		query := c.QueryParam("q")
		if query == "" {
			return c.JSON(http.StatusBadRequest, echo.Map{
				"error": "query is empty",
			})
		}

		offsetStr := c.QueryParam("offset")
		offset := 0
		if offsetStr != "" {
			offset, _ = strconv.Atoi(offsetStr)
		}

		timeline := c.Param("id")
		if timeline == "" {
			return c.JSON(http.StatusBadRequest, echo.Map{
				"error": "timeline is empty",
			})
		}

		search, err := index.Search(query,
			&meilisearch.SearchRequest{
				Limit:  10,
				Offset: int64(offset),
				Filter: fmt.Sprintf("timelines = \"%s\"", timeline),
				Sort:   []string{"signedAt:desc"},
			},
		)

		if err != nil {
			return c.JSON(http.StatusInternalServerError, echo.Map{
				"error": err.Error(),
			})
		}

		hits := search.Hits
		if hits == nil {
			return c.JSON(http.StatusOK, echo.Map{"status": "ok", "content": []searchResult{}})
		}

		var results []searchResult
		for _, hit := range hits {
			hitDoc := hit.(map[string]any)
			results = append(results, searchResult{
				ID:    hitDoc["id"].(string),
				Owner: hitDoc["signer"].(string),
			})
		}

		return c.JSON(http.StatusOK,
			echo.Map{
				"status":  "ok",
				"content": results,
				"limit":   search.Limit,
				"offset":  search.Offset,
			},
		)
	})

	log.Fatal(e.Start(fmt.Sprintf(":%d", port)))
}
