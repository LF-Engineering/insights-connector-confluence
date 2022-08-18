package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/LF-Engineering/insights-connector-confluence/build"
	"github.com/LF-Engineering/insights-datasource-shared/aws"
	"github.com/LF-Engineering/insights-datasource-shared/cache"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	neturl "net/url"

	elastic "github.com/LF-Engineering/insights-datasource-shared/elastic"
	logger "github.com/LF-Engineering/insights-datasource-shared/ingestjob"
	"github.com/LF-Engineering/lfx-event-schema/service"
	"github.com/LF-Engineering/lfx-event-schema/service/insights"
	"github.com/LF-Engineering/lfx-event-schema/service/user"
	"github.com/LF-Engineering/lfx-event-schema/utils/datalake"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	insightsConf "github.com/LF-Engineering/lfx-event-schema/service/insights/confluence"

	shared "github.com/LF-Engineering/insights-datasource-shared"
	"github.com/LF-Engineering/insights-datasource-shared/cryptography"
	jsoniter "github.com/json-iterator/go"
)

const (
	// ConfluenceBackendVersion - backend version
	ConfluenceBackendVersion = "0.1.0"
	// ConfluenceDefaultMaxContents - max contents to fetch at a time
	ConfluenceDefaultMaxContents = 1000
	// ConfluenceDefaultSearchField - default search field
	ConfluenceDefaultSearchField = "item_id"
	// ConfluenceConnector - connector name
	ConfluenceConnector = "confluence-connector"
	// ConfluenceDataSource - confluence datasource
	ConfluenceDataSource = "confluence"
	// ConfluenceDefaultStream - Stream To Publish confluence
	ConfluenceDefaultStream = "PUT-S3-confluence"
	// ConfluenceAddHistoryCreatedByRole - should we add contributor for history->createdBy page version edit?
	ConfluenceAddHistoryCreatedByRole = false
	// ConfluenceAddHistoryLastUpdatedByRole - should we add contributor for history->lastUpdatedBy page version edit?
	ConfluenceAddHistoryLastUpdatedByRole = false
)

var (
	gMaxUpdatedAt    time.Time
	gMaxUpdatedAtMtx = &sync.Mutex{}
	// ConfluenceDataSource - constant
	//ConfluenceDataSource = &models.DataSource{Name: "Confluence", Slug: "confluence", Model: "documentation"}
	//gConfluenceMetaData = &models.MetaData{BackendName: "confluence", BackendVersion: ConfluenceBackendVersion}
)

// Publisher - publish data to S3
type Publisher interface {
	PushEvents(action, source, eventType, subEventType, env string, data []interface{}) error
}

// DSConfluence - DS implementation for confluence - does nothing at all, just presents a skeleton code
type DSConfluence struct {
	URL             string // Confluence instance URL, for example https://wiki.lfnetworking.org
	MaxContents     int    // Defaults to ConfluenceDefaultMaxContents
	User            string // If user is provided then we assume that we don't have base64 encoded user:token yet
	Token           string // If user is not specified we assume that token already contains "<username>:<your-api-token>"
	SkipBody        bool   // Do not retrieve page body from API and do not store it (schema allows null for body)
	FlagURL         *string
	FlagMaxContents *int
	FlagUser        *string
	FlagToken       *string
	FlagSkipBody    *bool
	FlagStream      *string
	// Publisher & stream
	Publisher
	Stream        string // stream to publish the data
	Logger        logger.Logger
	log           *logrus.Entry
	cacheProvider cache.Manager
	endpoint      string
}

// AddPublisher - sets Kinesis publisher
func (j *DSConfluence) AddPublisher(publisher Publisher) {
	j.Publisher = publisher
}

// PublisherPushEvents - this is a fake function to test publisher locally
// FIXME: don't use when done implementing
func (j *DSConfluence) PublisherPushEvents(ev, ori, src, cat, env string, v []interface{}) error {
	data, err := jsoniter.Marshal(v)
	j.log.WithFields(logrus.Fields{"operation": "AddLogger"}).Infof("publish[ev=%s ori=%s src=%s cat=%s env=%s]: %d items: %+v -> %v", ev, ori, src, cat, env, len(v), string(data), err)
	return nil
}

// AddLogger - adds logger
func (j *DSConfluence) AddLogger(ctx *shared.Ctx) {
	client, err := elastic.NewClientProvider(&elastic.Params{
		URL:      os.Getenv("ELASTIC_LOG_URL"),
		Password: os.Getenv("ELASTIC_LOG_PASSWORD"),
		Username: os.Getenv("ELASTIC_LOG_USER"),
	})
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "AddLogger"}).Errorf("elastic NewClientProvider error: %+v", err)
		return
	}
	logProvider, err := logger.NewLogger(client, os.Getenv("STAGE"))
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "AddLogger"}).Errorf("logger NewLogger error: %+v", err)
		return
	}
	j.Logger = *logProvider
}

// WriteLog - writes to log
func (j *DSConfluence) WriteLog(ctx *shared.Ctx, status, message string) error {
	return nil
	arn, err := aws.GetContainerARN()
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "WriteLog"}).Errorf("getContainerMetadata Error : %+v", err)
		return err
	}
	err = j.Logger.Write(&logger.Log{
		Connector: ConfluenceDataSource,
		TaskARN:   arn,
		Configuration: []map[string]string{
			{
				"CONFLUENCE_URL": j.URL,
				"ProjectSlug":    ctx.Project,
			}},
		Status:    status,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Message:   message,
	})
	return err
}

// AddFlags - add confluence specific flags
func (j *DSConfluence) AddFlags() {
	j.FlagURL = flag.String("confluence-url", "", "Confluence instance URL, for example https://wiki.lfnetworking.org")
	j.FlagMaxContents = flag.Int("confluence-max-contents", ConfluenceDefaultMaxContents, fmt.Sprintf("Max Contents - defaults to ConfluenceDefaultMaxContents (%d)", ConfluenceDefaultMaxContents))
	j.FlagUser = flag.String("confluence-user", "", "User: if user is provided then we assume that we don't have base64 encoded user:token yet")
	j.FlagToken = flag.String("confluence-token", "", "Token: if user is not specified we assume that token already contains \"<username>:<your-api-token>\"")
	j.FlagSkipBody = flag.Bool("confluence-skip-body", false, "Do not retrieve page body from API and do not store it (schema allows null for body)")
	j.FlagStream = flag.String("confluence-stream", ConfluenceDefaultStream, "confluence kinesis stream name, for example PUT-S3-confluence")
}

// ParseArgs - parse confluence specific environment variables
func (j *DSConfluence) ParseArgs(ctx *shared.Ctx) (err error) {
	// Cryptography
	encrypt, err := cryptography.NewEncryptionClient()
	if err != nil {
		return err
	}

	// Confluence URL
	if shared.FlagPassed(ctx, "url") && *j.FlagURL != "" {
		j.URL = *j.FlagURL
	}
	if ctx.EnvSet("URL") {
		j.URL = ctx.Env("URL")
	}

	// Max contents
	passed := shared.FlagPassed(ctx, "max-contents")
	if passed {
		j.MaxContents = *j.FlagMaxContents
	}
	if ctx.EnvSet("MAX_CONTENTS") {
		maxContents, err := strconv.Atoi(ctx.Env("MAX_CONTENTS"))
		shared.FatalOnError(err)
		if maxContents > 0 {
			j.MaxContents = maxContents
		}
	} else if !passed {
		j.MaxContents = ConfluenceDefaultMaxContents
	}

	// SkipBody
	if shared.FlagPassed(ctx, "skip-body") {
		j.SkipBody = *j.FlagSkipBody
	}
	skipBody, present := ctx.BoolEnvSet("SKIP_BODY")
	if present {
		j.SkipBody = skipBody
	}

	// SSO User
	if shared.FlagPassed(ctx, "user") && *j.FlagUser != "" {
		j.User = *j.FlagUser
	}
	if ctx.EnvSet("USER") {
		j.User = ctx.Env("USER")
	}
	if j.User != "" {
		j.User, err = encrypt.Decrypt(j.User)
		if err != nil {
			return err
		}
		shared.AddRedacted(j.User, false)
	}

	// SSO Token
	if shared.FlagPassed(ctx, "token") && *j.FlagToken != "" {
		j.Token = *j.FlagToken
	}
	if ctx.EnvSet("TOKEN") {
		j.Token = ctx.Env("TOKEN")
	}
	if j.Token != "" {
		j.Token, err = encrypt.Decrypt(j.Token)
		if err != nil {
			return err
		}
		shared.AddRedacted(j.Token, false)
	}

	// confluence Kinesis stream
	j.Stream = ConfluenceDefaultStream
	if shared.FlagPassed(ctx, "stream") {
		j.Stream = *j.FlagStream
	}
	if ctx.EnvSet("STREAM") {
		j.Stream = ctx.Env("STREAM")
	}

	// SSO: Handle either user,token pair or just a token
	if j.User != "" {
		// If user is specified, then we must calculate base64(user:token) to get a real token
		j.Token = base64.StdEncoding.EncodeToString([]byte(j.User + ":" + j.Token))
		shared.AddRedacted(j.Token, false)
	}
	// NOTE: don't forget this
	// gConfluenceMetaData.Project = ctx.Project
	// gConfluenceMetaData.Tags = ctx.Tags
	return
}

// Validate - is current DS configuration OK?
func (j *DSConfluence) Validate() (err error) {
	j.URL = strings.TrimSpace(j.URL)
	if strings.HasSuffix(j.URL, "/") {
		j.URL = j.URL[:len(j.URL)-1]
	}
	if j.URL == "" {
		err = fmt.Errorf("URL must be set")
	}
	return
}

// Init - initialize confluence data source
func (j *DSConfluence) Init(ctx *shared.Ctx) (err error) {
	shared.NoSSLVerify()
	ctx.InitEnv("Confluence")
	j.AddFlags()
	ctx.Init()
	err = j.ParseArgs(ctx)
	if err != nil {
		return
	}
	err = j.Validate()
	if err != nil {
		return
	}
	if ctx.Debug > 1 {
		j.log.WithFields(logrus.Fields{"operation": "Init"}).Debugf("confluence: %+v\nshared context: %s", j, ctx.Info())
	}

	if j.Stream != "" {
		sess, err := session.NewSession()
		if err != nil {
			return err
		}
		s3Client := s3.New(sess)
		objectStore := datalake.NewS3ObjectStore(s3Client)
		datalakeClient := datalake.NewStoreClient(&objectStore)
		j.AddPublisher(&datalakeClient)
	}
	j.AddLogger(ctx)
	return
}

// GetHistoricalContents - get historical contents from teh current content
func (j *DSConfluence) GetHistoricalContents(ctx *shared.Ctx, content map[string]interface{}, dateFrom, dateTo time.Time) (contents []map[string]interface{}, err error) {
	iContentURL, _ := shared.Dig(content, []string{"_links", "webui"}, true, false)
	ancestors, ok := shared.Dig(content, []string{"ancestors"}, false, true)
	if !ok {
		ancestors = []interface{}{}
	}
	content["ancestors"] = ancestors
	contentURL, _ := iContentURL.(string)
	contentURL = j.URL + contentURL
	content["content_url"] = contentURL
	iVersionNumber, _ := shared.Dig(content, []string{"version", "number"}, true, false)
	lastVersion := int(iVersionNumber.(float64))
	if lastVersion == 1 {
		contents = append(contents, content)
		return
	}
	iID, ok := content["id"]
	if !ok {
		err = fmt.Errorf("missing id property in content: %+v", content)
		return
	}
	id, ok := iID.(string)
	if !ok {
		err = fmt.Errorf("id property is not a string: %+v", content)
		return
	}
	method := "GET"
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Authorization": "Basic " + j.Token}
	}
	cacheDur := time.Duration(24) * time.Hour
	version := 1
	var (
		res    interface{}
		status int
	)
	for {
		var url string
		if j.SkipBody {
			url = j.URL + "/rest/api/content/" + id + "?version=" + strconv.Itoa(version) + "&status=historical&expand=" + neturl.QueryEscape("history,history.lastUpdated,version,space")
		} else {
			url = j.URL + "/rest/api/content/" + id + "?version=" + strconv.Itoa(version) + "&status=historical&expand=" + neturl.QueryEscape("body.storage,history,history.lastUpdated,version,space")
		}
		if ctx.Debug > 1 {
			j.log.WithFields(logrus.Fields{"operation": "GetHistoricalContents"}).Debugf("historical content url: %s", url)
		}
		res, status, _, _, err = shared.Request(
			ctx,
			url,
			method,
			headers,
			nil,
			nil,
			map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses: 200
			nil,                                 // Error statuses
			map[[2]int]struct{}{{200, 200}: {}, {500, 500}: {}, {404, 404}: {}}, // OK statuses: 200
			map[[2]int]struct{}{{200, 200}: {}},                                 // Cache statuses: 200
			false,                                                               // retry
			&cacheDur,                                                           // cache duration
			false,                                                               // skip in dry-run mode
		)
		if status == 404 || status == 500 {
			if ctx.Debug > 1 {
				j.log.WithFields(logrus.Fields{"operation": "GetHistoricalContents"}).Debugf("%s: v%d status %d: %s", id, version, status, url)
			}
			break
		}
		if err != nil {
			return
		}
		result, ok := res.(map[string]interface{})
		if !ok {
			err = fmt.Errorf("cannot parse JSON from (status: %d):\n%s", status, string(res.([]byte)))
			return
		}
		iLatest, _ := shared.Dig(result, []string{"history", "latest"}, true, false)
		latest, ok := iLatest.(bool)
		if !ok {
			err = fmt.Errorf("cannot read latest property: %+v", result)
			return
		}
		iWhen, ok := shared.Dig(result, []string{"version", "when"}, false, true)
		if !ok {
			if ctx.Debug > 0 {
				j.log.WithFields(logrus.Fields{"operation": "GetHistoricalContents"}).Debugf("missing 'when' attribute for content %s version %d, skipping", id, version)
			}
			if latest {
				break
			}
			version++
			continue
		}
		var when time.Time
		when, err = shared.TimeParseInterfaceString(iWhen)
		if err != nil {
			return
		}
		if !when.Before(dateFrom) && !when.After(dateTo) {
			result["content_url"] = contentURL
			result["ancestors"] = ancestors
			contents = append(contents, result)
		}
		if ctx.Debug > 2 {
			j.log.WithFields(logrus.Fields{"operation": "GetHistoricalContents"}).Debugf("%s: v%d %+v,%v (%s)", id, version, when, latest, url)
		}
		if latest {
			break
		}
		version++
		if version == lastVersion {
			break
		}
	}
	contents = append(contents, content)
	if ctx.Debug > 1 {
		j.log.WithFields(logrus.Fields{"operation": "GetHistoricalContents"}).Debugf("final %s %d (%d historical contents)", id, version, len(contents))
	}
	return
}

// GetConfluenceContents - get confluence historical contents
func (j *DSConfluence) GetConfluenceContents(ctx *shared.Ctx, fromDate, toDate, next string) (contents []map[string]interface{}, newNext string, err error) {
	/*
		shared.Printf("GetConfluenceContents: in\n")
		defer func() {
			shared.Printf("GetConfluenceContents: out %d\n", len(contents))
		}()
	*/
	if next == "" {
		return
	}
	method := "GET"
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Authorization": "Basic " + j.Token}
	}
	cacheDur := time.Duration(24) * time.Hour
	var url string
	// Init state
	if next == "i" {
		////url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("lastModified>='"+fromDate+"' order by lastModified") + fmt.Sprintf("&limit=%d&expand=ancestors", j.MaxContents)
		if j.SkipBody {
			url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("(lastModified>='"+fromDate+"' and lastModified<='"+toDate+"') order by lastModified") + fmt.Sprintf("&limit=%d", j.MaxContents) + "&expand=" + neturl.QueryEscape("ancestors,version,space,history,history.lastUpdated")
		} else {
			url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("(lastModified>='"+fromDate+"' and lastModified<='"+toDate+"') order by lastModified") + fmt.Sprintf("&limit=%d", j.MaxContents) + "&expand=" + neturl.QueryEscape("body.storage,ancestors,version,space,history,history.lastUpdated")
		}
	} else {
		url = j.URL + next
	}
	if ctx.Debug > 1 {
		j.log.WithFields(logrus.Fields{"operation": "GetConfluenceContents"}).Debugf("content url: %s", url)
	}
	res, status, _, _, err := shared.Request(
		ctx,
		url,
		method,
		headers,
		nil,
		nil,
		map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses: 200
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
		false,                               // retry
		&cacheDur,                           // cache duration
		false,                               // skip in dry-run mode
	)
	// shared.Printf("res=%v\n", res.(map[string]interface{}))
	// shared.Printf("status=%d, err=%v\n", status, err)
	if err != nil {
		return
	}
	result, ok := res.(map[string]interface{})
	if !ok {
		err = fmt.Errorf("cannot parse JSON from (status: %d):\n%s", status, string(res.([]byte)))
		return
	}
	iNext, ok := shared.Dig(result, []string{"_links", "next"}, false, true)
	if ok {
		newNext, _ = iNext.(string)
	}
	iResults, ok := result["results"]
	if ok {
		results, ok := iResults.([]interface{})
		if ok {
			for _, iResult := range results {
				content, ok := iResult.(map[string]interface{})
				if ok {
					contents = append(contents, content)
				}
			}
		}
	}
	return
}

// ItemID - return unique identifier for an item
func (j *DSConfluence) ItemID(item interface{}) string {
	id, _ := shared.Dig(item, []string{"id"}, true, false)
	//versionNumber, _ := shared.Dig(item, []string{"version", "number"}, true, false)
	//return id.(string) + "#v" + fmt.Sprintf("%.0f", versionNumber.(float64))
	return id.(string)
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSConfluence) ItemUpdatedOn(item interface{}) time.Time {
	iWhen, _ := shared.Dig(item, []string{"version", "when"}, false, true)
	when, err := shared.TimeParseInterfaceString(iWhen)
	shared.FatalOnError(err)
	return when
}

// AddMetadata - add metadata to the item
func (j *DSConfluence) AddMetadata(ctx *shared.Ctx, item interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := j.URL
	tags := ctx.Tags
	if len(tags) == 0 {
		tags = []string{origin}
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := shared.UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = ctx.DS
	mItem["backend_version"] = ConfluenceBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem["uuid"] = uuid
	mItem["origin"] = origin
	mItem["tags"] = tags
	mItem["offset"] = float64(updatedOn.Unix())
	mItem["category"] = "historical content"
	mItem["search_fields"] = make(map[string]interface{})
	id, _ := shared.Dig(item, []string{"id"}, true, false)
	versionNumber, _ := shared.Dig(item, []string{"version", "number"}, true, false)
	var ancestorIDs []interface{}
	iAncestors, ok := shared.Dig(item, []string{"ancestors"}, false, true)
	if ok {
		ancestors, ok := iAncestors.([]interface{})
		if ok {
			for _, iAncestor := range ancestors {
				ancestor, ok := iAncestor.(map[string]interface{})
				if !ok {
					continue
				}
				ancestorID, ok := ancestor["id"]
				if ok {
					ancestorIDs = append(ancestorIDs, ancestorID)
				}
			}
		}
	}
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", ConfluenceDefaultSearchField}, itemID, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "content_id"}, id, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "ancestor_ids"}, ancestorIDs, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "version_number"}, versionNumber, false))
	// shared.Printf("%+v\n", mItem["search_fields"])
	mItem["metadata__updated_on"] = shared.ToESDate(updatedOn)
	mItem["metadata__timestamp"] = shared.ToESDate(timestamp)
	// mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// Sync - sync confluence data source
func (j *DSConfluence) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom != nil {
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("%s fetching from %v", j.URL, ctx.DateFrom)
	}
	if ctx.DateFrom == nil {
		cachedLastSync, er := j.cacheProvider.GetLastSync(j.endpoint)
		if er != nil {
			err = er
			return
		}
		ctx.DateFrom = &cachedLastSync
		if ctx.DateFrom != nil {
			j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("%s resuming from %v", j.URL, ctx.DateFrom)
		}
	}
	if ctx.DateTo != nil {
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("%s fetching till %v", j.URL, ctx.DateTo)
	}
	// NOTE: Non-generic starts here
	var (
		sDateFrom string
		dateFrom  time.Time
		sDateTo   string
		dateTo    time.Time
	)
	if ctx.DateFrom != nil {
		dateFrom = *ctx.DateFrom
		sDateFrom = shared.ToYMDHMDate(dateFrom)
	} else {
		dateFrom = shared.DefaultDateFrom
		sDateFrom = "1970-01-01 00:00"
	}
	if ctx.DateTo != nil {
		dateTo = *ctx.DateTo
		sDateTo = shared.ToYMDHMDate(dateTo)
	} else {
		dateTo = shared.DefaultDateTo
		sDateTo = "2100-01-01 00:00"
	}
	next := "i"
	var (
		ch             chan error
		allDocs        []interface{}
		allContents    []interface{}
		allContentsMtx *sync.Mutex
		escha          []chan error
		eschaMtx       *sync.Mutex
	)
	if thrN > 1 {
		ch = make(chan error)
		allContentsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processContent := func(c chan error, content map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// shared.Printf("processContent: in\n")
		var contents []map[string]interface{}
		contents, e = j.GetHistoricalContents(ctx, content, dateFrom, dateTo)
		if e != nil {
			return
		}
		var esItems []interface{}
		for _, content := range contents {
			esItem := j.AddMetadata(ctx, content)
			if ctx.Project != "" {
				content["project"] = ctx.Project
			}
			esItem["data"] = content
			esItems = append(esItems, esItem)
		}
		// shared.Printf("processContent: out %d\n", len(contents))
		if allContentsMtx != nil {
			allContentsMtx.Lock()
		}
		allContents = append(allContents, esItems...)
		nContents := len(allContents)
		if nContents >= ctx.PackSize {
			sendToQueue := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = j.ConfluenceEnrichItems(ctx, thrN, allContents, &allDocs, false)
				if ee != nil {
					j.log.WithFields(logrus.Fields{"operation": "Sync"}).Errorf("error %v sending %d historical contents to queue", ee, len(allContents))
				}
				allContents = []interface{}{}
				if allContentsMtx != nil {
					allContentsMtx.Unlock()
				}
				return
			}
			if thrN > 1 {
				wch = make(chan error)
				go func() {
					_ = sendToQueue(wch)
				}()
			} else {
				e = sendToQueue(nil)
				if e != nil {
					return
				}
			}
		} else {
			if allContentsMtx != nil {
				allContentsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for {
			var contents []map[string]interface{}
			contents, next, err = j.GetConfluenceContents(ctx, sDateFrom, sDateTo, next)
			if err != nil {
				return
			}
			for _, cont := range contents {
				go func(content map[string]interface{}) {
					var (
						e    error
						esch chan error
					)
					esch, e = processContent(ch, content)
					if e != nil {
						j.log.WithFields(logrus.Fields{"operation": "Sync"}).Errorf("process error: %v", e)
						return
					}
					if esch != nil {
						if eschaMtx != nil {
							eschaMtx.Lock()
						}
						escha = append(escha, esch)
						if eschaMtx != nil {
							eschaMtx.Unlock()
						}
					}
				}(cont)
				nThreads++
				if nThreads == thrN {
					err = <-ch
					if err != nil {
						return
					}
					nThreads--
				}
			}
			if next == "" {
				break
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for {
			var contents []map[string]interface{}
			contents, next, err = j.GetConfluenceContents(ctx, sDateFrom, sDateTo, next)
			if err != nil {
				return
			}
			for _, content := range contents {
				_, err = processContent(nil, content)
				if err != nil {
					return
				}
			}
			if next == "" {
				break
			}
		}
	}
	// NOTE: lock needed
	if eschaMtx != nil {
		eschaMtx.Lock()
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			if eschaMtx != nil {
				eschaMtx.Unlock()
			}
			return
		}
	}
	if eschaMtx != nil {
		eschaMtx.Unlock()
	}
	nContents := len(allContents)
	if ctx.Debug > 0 {
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Debugf("%d remaining contents to send to queue", nContents)
	}
	// NOTE: for all items, even if 0 - to flush the queue
	err = j.ConfluenceEnrichItems(ctx, thrN, allContents, &allDocs, true)
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Errorf("Error %v sending %d contents to queue", err, len(allContents))
	}
	// NOTE: Non-generic ends here
	gMaxUpdatedAtMtx.Lock()
	defer gMaxUpdatedAtMtx.Unlock()
	err = j.cacheProvider.SetLastSync(j.endpoint, gMaxUpdatedAt)
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("unable to set last sync date to cache.error: %v", err)
	}
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSConfluence) EnrichItem(ctx *shared.Ctx, item map[string]interface{}) (rich map[string]interface{}, err error) {
	// shared.Printf("raw: %+v\n", item)
	/*
		shared.Printf("raw: %s\n", shared.InterfaceToStringTrunc(item, shared.MaxPayloadPrintfLen, true))
		jsonBytes, err := jsoniter.Marshal(item)
		if err != nil {
			shared.Printf("Error: %+v\n", err)
			return
		}
		shared.Printf("%s\n", string(jsonBytes))
	*/
	rich = make(map[string]interface{})
	for _, field := range shared.RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	page, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", shared.DumpKeys(item))
		return
	}
	// shared.Printf("page = %s\n", shared.PrettyPrint(page))
	for _, field := range []string{"type", "id", "status", "title", "content_url"} {
		rich[field], _ = page[field]
	}
	title := ""
	iTitle, ok := page["title"]
	if ok {
		title, _ = iTitle.(string)
	}
	if len(title) > shared.KeywordMaxlength {
		title = title[:shared.KeywordMaxlength]
	}
	rich["title"] = title
	version, ok := page["version"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing version field in item %+v", shared.DumpKeys(page))
		return
	}
	user, _ := shared.Dig(version, []string{"by"}, true, false)
	rich["by"] = user
	rich["message"], _ = shared.Dig(version, []string{"message"}, false, true)
	iVersion, _ := version["number"]
	rich["version"] = iVersion
	rich["date"], _ = version["when"]
	////base, _ := shared.Dig(page, []string{"_links", "base"}, true, false)
	webUI, _ := shared.Dig(page, []string{"_links", "webui"}, true, false)
	////rich["url"] = base.(string) + webUI.(string)
	rich["url"] = j.URL + webUI.(string)
	// This code works when we are not expanding space
	/*
		  iSpace, ok := shared.Dig(page, []string{"_expandable", "space"}, false, true)
			if ok {
				space, _ := iSpace.(string)
				space = strings.Replace(space, "/rest/api/space/", "", -1)
				rich["space"] = space
			}
	*/
	rich["space_id"], _ = shared.Dig(page, []string{"space", "id"}, false, true)
	rich["space_key"], _ = shared.Dig(page, []string{"space", "key"}, false, true)
	rich["space_name"], _ = shared.Dig(page, []string{"space", "name"}, false, true)
	rich["space_type"], _ = shared.Dig(page, []string{"space", "type"}, false, true)
	var (
		ancestorIDs    []interface{}
		ancestorTitles []interface{}
		ancestorLinks  []interface{}
	)
	iAncestors, ok := shared.Dig(page, []string{"ancestors"}, false, true)
	if ok {
		ancestors, ok := iAncestors.([]interface{})
		if ok {
			for _, iAncestor := range ancestors {
				ancestor, ok := iAncestor.(map[string]interface{})
				if !ok {
					continue
				}
				ancestorID, ok := ancestor["id"]
				ancestorIDs = append(ancestorIDs, ancestorID)
				ancestorTitle, ok := ancestor["title"]
				if ok {
					ancestorTitles = append(ancestorTitles, ancestorTitle)
				} else {
					ancestorTitles = append(ancestorTitles, "NO_TITLE")
				}
				ancestorLink, _ := shared.Dig(ancestor, []string{"_links", "webui"}, true, false)
				sAncestorLink, _ := ancestorLink.(string)
				sAncestorLink = j.URL + sAncestorLink
				ancestorLinks = append(ancestorLinks, sAncestorLink)
			}
		}
	}
	rich["ancestors_ids"] = ancestorIDs
	rich["ancestors_titles"] = ancestorTitles
	rich["ancestors_links"] = ancestorLinks
	iType, _ := shared.Dig(page, []string{"type"}, true, false)
	if iType.(string) == "page" && int(iVersion.(float64)) == 1 {
		rich["type"] = "new_page"
	}
	rich["original_type"] = iType
	rich["is_blogpost"] = 0
	tp, _ := rich["type"].(string)
	rich["is_"+tp] = 1
	// can also be rich["date"]
	updatedOn, _ := shared.Dig(item, []string{"metadata__updated_on"}, true, false)
	rich["updated_on"] = updatedOn
	if !j.SkipBody {
		iBody, ok := shared.Dig(page, []string{"body", "storage", "value"}, false, true)
		if ok {
			body, _ := iBody.(string)
			if len(body) > shared.MaxBodyLength {
				body = body[:shared.MaxBodyLength]
			}
			rich["body"] = body
		}
	}
	iAvatar, ok := shared.Dig(page, []string{"version", "by", "profilePicture", "path"}, false, true)
	if ok {
		avatar, _ := iAvatar.(string)
		rich["avatar"] = j.URL + avatar
	}
	rich["by_name"], rich["by_username"], rich["by_email"] = j.GetRoleIdentity(item)
	if ConfluenceAddHistoryCreatedByRole {
		iAvatar, ok = shared.Dig(page, []string{"history", "createdBy", "profilePicture", "path"}, false, true)
		if ok {
			avatar, _ := iAvatar.(string)
			rich["created_by_avatar"] = j.URL + avatar
		}
		rich["created_by_name"], rich["created_by_username"], rich["created_by_email"] = j.GetCreatedRoleIdentity(ctx, item)
	}
	if ConfluenceAddHistoryLastUpdatedByRole {
		iAvatar, ok = shared.Dig(page, []string{"history", "lastUpdated", "by", "profilePicture", "path"}, false, true)
		if ok {
			avatar, _ := iAvatar.(string)
			rich["updated_by_avatar"] = j.URL + avatar
		}
		rich["updated_by_name"], rich["updated_by_username"], rich["updated_by_email"] = j.GetLastUpdatedRoleIdentity(ctx, item)
	}
	// From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// GetRoleIdentity - return identity data for version->by
func (j *DSConfluence) GetRoleIdentity(item map[string]interface{}) (name, username, email string) {
	iUser, _ := shared.Dig(item, []string{"data", "version", "by"}, true, false)
	user, _ := iUser.(map[string]interface{})
	iUserName, ok := user["username"]
	if ok {
		username, _ = iUserName.(string)
	} else {
		iPublicName, ok := user["publicName"]
		if ok {
			username, _ = iPublicName.(string)
		}
	}
	iDisplayName, ok := user["displayName"]
	if ok {
		name, _ = iDisplayName.(string)
	}
	iEmail, ok := user["email"]
	if ok {
		email, _ = iEmail.(string)
	}
	return
}

// GetCreatedRoleIdentity - return identity data for history->createdBy
func (j *DSConfluence) GetCreatedRoleIdentity(ctx *shared.Ctx, item map[string]interface{}) (name, username, email string) {
	iUser, ok := shared.Dig(item, []string{"data", "history", "createdBy"}, false, true)
	if !ok {
		if ctx.Debug > 0 {
			j.log.WithFields(logrus.Fields{"operation": "GetCreatedRoleIdentity"}).Debugf("GetCreatedRoleIdentity -> no history->createdBy in %s", shared.PrettyPrint(item))
		}
		return
	}
	user, _ := iUser.(map[string]interface{})
	iUserName, ok := user["username"]
	if ok {
		username, _ = iUserName.(string)
	} else {
		iPublicName, ok := user["publicName"]
		if ok {
			username, _ = iPublicName.(string)
		}
	}
	iDisplayName, ok := user["displayName"]
	if ok {
		name, _ = iDisplayName.(string)
	}
	iEmail, ok := user["email"]
	if ok {
		email, _ = iEmail.(string)
	}
	return
}

// GetLastUpdatedRoleIdentity - return identity data for history->lastUpdated->by
func (j *DSConfluence) GetLastUpdatedRoleIdentity(ctx *shared.Ctx, item map[string]interface{}) (name, username, email string) {
	iUser, ok := shared.Dig(item, []string{"data", "history", "lastUpdated", "by"}, false, true)
	if !ok {
		if ctx.Debug > 0 {
			j.log.WithFields(logrus.Fields{"operation": "GetLastUpdatedRoleIdentity"}).Debugf("GetLastUpdatedRoleIdentity -> no history->lastUpdated->by in %s", shared.PrettyPrint(item))
		}
		return
	}
	user, _ := iUser.(map[string]interface{})
	iUserName, ok := user["username"]
	if ok {
		username, _ = iUserName.(string)
	} else {
		iPublicName, ok := user["publicName"]
		if ok {
			username, _ = iPublicName.(string)
		}
	}
	iDisplayName, ok := user["displayName"]
	if ok {
		name, _ = iDisplayName.(string)
	}
	iEmail, ok := user["email"]
	if ok {
		email, _ = iEmail.(string)
	}
	return
}

func (j *DSConfluence) mapActivityType(actType string) insightsConf.ContentType {
	/* types:
	page,240195
	attachment,98324
	new_page,29520
	comment,23960
	blogpost,228
	*/
	switch actType {
	case "confluence_page", "confluence_new_page":
		return insightsConf.PageType
	case "confluence_blogpost":
		return insightsConf.BlogPostType
	case "confluence_attachment":
		return insightsConf.AttachmentType
	case "confluence_comment":
		return insightsConf.CommentType
	default:
		j.log.WithFields(logrus.Fields{"operation": "mapActivityType"}).Warningf("warning: unknown activity type: '%s'", actType)
	}
	return insightsConf.ContentType(actType)
}

// GetModelData - return data in swagger format
func (j *DSConfluence) GetModelData(ctx *shared.Ctx, docs []interface{}) (data map[string][]interface{}, err error) {
	data = make(map[string][]interface{})
	defer func() {
		if err != nil {
			return
		}
		contentBaseEvent := insightsConf.ContentBaseEvent{
			Connector:        insights.ConfluenceConnector,
			ConnectorVersion: ConfluenceBackendVersion,
			Source:           insights.ConfluenceSource,
		}
		for k, v := range data {
			switch k {
			case "created":
				baseEvent := service.BaseEvent{
					Type: service.EventType(insightsConf.ContentCreatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: ConfluenceConnector,
						UpdatedBy: ConfluenceConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}

				ary := []interface{}{}
				for _, content := range v {
					ary = append(ary, insightsConf.ContentCreatedEvent{
						ContentBaseEvent: contentBaseEvent,
						BaseEvent:        baseEvent,
						Payload:          content.(insightsConf.Content),
					})
				}
				data[k] = ary
			case "updated":
				baseEvent := service.BaseEvent{
					Type: service.EventType(insightsConf.ContentUpdatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: ConfluenceConnector,
						UpdatedBy: ConfluenceConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}

				ary := []interface{}{}
				for _, content := range v {
					ary = append(ary, insightsConf.ContentUpdatedEvent{
						ContentBaseEvent: contentBaseEvent,
						BaseEvent:        baseEvent,
						Payload:          content.(insightsConf.Content),
					})
				}
				data[k] = ary
			default:
				err = fmt.Errorf("unknown content '%s' event", k)
				return
			}
		}
	}()
	// generate map of entityID:first_updated_on, entityID:version
	createDates := make(map[string]time.Time)
	// createDatesVersions := make(map[string]float64)
	for _, iDoc := range docs {
		doc, _ := iDoc.(map[string]interface{})
		entityID, _ := doc["id"].(string)
		// version, _ := doc["version"].(float64)
		iUpdatedOn := doc["updated_on"]
		updatedOn, err := shared.TimeParseInterfaceString(iUpdatedOn)
		shared.FatalOnError(err)
		currentMin, ok := createDates[entityID]
		if !ok {
			createDates[entityID] = updatedOn
			// createDatesVersions[entityID] = version
			continue
		}
		if updatedOn.Before(currentMin) {
			createDates[entityID] = updatedOn
			// createDatesVersions[entityID] = version
		}
	}
	// Generate children IDs
	children := make(map[string]map[string]struct{})
	for _, iDoc := range docs {
		doc, _ := iDoc.(map[string]interface{})
		iAIDs, ok := doc["ancestors_ids"]
		if !ok {
			continue
		}
		aIDs, ok := iAIDs.([]interface{})
		if !ok {
			continue
		}
		entityID, _ := doc["id"].(string)
		for _, aID := range aIDs {
			aid, _ := aID.(string)
			_, ok := children[aid]
			if !ok {
				children[aid] = make(map[string]struct{})
			}
			children[aid][entityID] = struct{}{}
		}
	}
	// Process contents versions
	userID, confluenceContentID, confluenceSpaceID := "", "", ""
	var updatedOn time.Time
	source := ConfluenceDataSource
	for _, iDoc := range docs {
		doc, _ := iDoc.(map[string]interface{})
		entityID, _ := doc["id"].(string)
		kids := []string{}
		kidsMap, ok := children[entityID]
		if ok {
			for kid := range kidsMap {
				kids = append(kids, kid)
			}
		}
		version, _ := doc["version"].(float64)
		activityType, _ := doc["type"].(string)
		activityType = "confluence_" + activityType
		contentType := j.mapActivityType(activityType)
		iUpdatedOn := doc["updated_on"]
		updatedOn, err = shared.TimeParseInterfaceString(iUpdatedOn)
		shared.FatalOnError(err)
		sBody := ""
		if !j.SkipBody {
			sBody, _ = doc["body"].(string)
		}
		avatar, _ := doc["avatar"].(string)
		title, _ := doc["title"].(string)
		url, _ := doc["url"].(string)
		spaceID := ""
		iSpaceID, ok := doc["space_id"]
		if ok {
			fSpaceID, _ := iSpaceID.(float64)
			spaceID = fmt.Sprintf("%.0f", fSpaceID)
		}
		nonEmptySpaceID := spaceID
		if nonEmptySpaceID == "" {
			nonEmptySpaceID = "0"
		}
		spaceKey, _ := doc["space_key"].(string)
		spaceName, _ := doc["space_name"].(string)
		spaceType, _ := doc["space_type"].(string)
		// shared.Printf("space = '%s,%s,%s,%s,%s'\n", spaceID, nonEmptySpaceID, spaceKey, spaceName, spaceType)
		name, _ := doc["by_name"].(string)
		username, _ := doc["by_username"].(string)
		email, _ := doc["by_email"].(string)
		// No identity data postprocessing in V2
		// name, username = shared.PostprocessNameUsername(name, username, email)
		userID, err = user.GenerateIdentity(&source, &email, &name, &username)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v", source, email, name, username, err, doc)
			return
		}
		contributors := []insights.Contributor{
			{
				Role:   insights.AuthorRole,
				Weight: 1.0,
				Identity: user.UserIdentityObjectBase{
					ID:         userID,
					Email:      email,
					IsVerified: false,
					Name:       name,
					Username:   username,
					Source:     ConfluenceDataSource,
					Avatar:     avatar,
				},
			},
		}
		if ConfluenceAddHistoryCreatedByRole {
			name, _ = doc["created_by_name"].(string)
			username, _ = doc["created_by_username"].(string)
			email, _ = doc["created_by_email"].(string)
			if name != "" || username != "" || email != "" {
				avatar, _ = doc["created_by_avatar"].(string)
				// No identity data postprocessing in V2
				// name, username = shared.PostprocessNameUsername(name, username, email)
				userID, err = user.GenerateIdentity(&source, &email, &name, &username)
				if err != nil {
					j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v", source, email, name, username, err, doc)
					return
				}
				contributor := insights.Contributor{
					Role:   insights.AuthorRole,
					Weight: 1.0,
					Identity: user.UserIdentityObjectBase{
						ID:         userID,
						Email:      email,
						IsVerified: false,
						Name:       name,
						Username:   username,
						Source:     ConfluenceDataSource,
						Avatar:     avatar,
					},
				}
				contributors = append(contributors, contributor)
			}
		}
		if ConfluenceAddHistoryLastUpdatedByRole {
			name, _ = doc["updated_by_name"].(string)
			username, _ = doc["updated_by_username"].(string)
			email, _ = doc["updated_by_email"].(string)
			if name != "" || username != "" || email != "" {
				avatar, _ = doc["updated_by_avatar"].(string)
				// No identity data postprocessing in V2
				// name, username = shared.PostprocessNameUsername(name, username, email)
				userID, err = user.GenerateIdentity(&source, &email, &name, &username)
				if err != nil {
					j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v", source, email, name, username, err, doc)
					return
				}
				contributor := insights.Contributor{
					Role:   insights.AuthorRole,
					Weight: 1.0,
					Identity: user.UserIdentityObjectBase{
						ID:         userID,
						Email:      email,
						IsVerified: false,
						Name:       name,
						Username:   username,
						Source:     ConfluenceDataSource,
						Avatar:     avatar,
					},
				}
				contributors = append(contributors, contributor)
			}
		}
		sVersion := fmt.Sprintf("%.0f", version)
		entityIDWithVersion := entityID + "-" + sVersion
		confluenceContentID, err = insightsConf.GenerateConfluenceContentID(j.URL, nonEmptySpaceID, string(contentType), entityIDWithVersion)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateConfluenceContentID(%s,%s,%s,%s): %+v for %+v", j.URL, nonEmptySpaceID, contentType, entityIDWithVersion, err, doc)
			return
		}
		confluenceSpaceID, err = insightsConf.GenerateConfluenceSpaceID(j.URL, nonEmptySpaceID)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateConfluenceSpaceID(%s,%s,%s,%s): %+v for %+v", j.URL, nonEmptySpaceID, contentType, entityIDWithVersion, err, doc)
			return
		}
		confSpace := insightsConf.Space{
			ID:        confluenceSpaceID,
			URL:       j.URL,
			SpaceID:   spaceID,
			SpaceKey:  spaceKey,
			SpaceName: spaceName,
			SpaceType: spaceType,
		}
		content := insightsConf.Content{
			ID:              confluenceContentID,
			EndpointID:      confluenceSpaceID,
			Space:           confSpace,
			ServerURL:       j.URL,
			ContentID:       entityID,
			ContentURL:      url,
			Version:         sVersion,
			Type:            contentType,
			Title:           title,
			Body:            sBody,
			Contributors:    shared.DedupContributors(contributors),
			SyncTimestamp:   time.Now(),
			SourceTimestamp: updatedOn,
			Children:        kids,
		}
		cacheID := fmt.Sprintf("content-%s", confluenceContentID)
		isCreated, err := j.cacheProvider.IsKeyCreated(j.endpoint, cacheID)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "GetModelDataPullRequest"}).Errorf("error getting cache for endpoint %s. error: %+v", j.endpoint, err)
			return data, err
		}
		key := "updated"
		if !isCreated {
			key = "created"
		}
		ary, ok := data[key]
		if !ok {
			ary = []interface{}{content}
		} else {
			ary = append(ary, content)
		}
		data[key] = ary
		gMaxUpdatedAtMtx.Lock()
		if updatedOn.After(gMaxUpdatedAt) {
			gMaxUpdatedAt = updatedOn
		}
		gMaxUpdatedAtMtx.Unlock()
	}
	return
}

// ConfluenceEnrichItems - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSConfluence) ConfluenceEnrichItems(ctx *shared.Ctx, thrN int, items []interface{}, docs *[]interface{}, final bool) (err error) {
	j.log.WithFields(logrus.Fields{"operation": "ConfluenceEnrichItems"}).Infof("input processing(%d/%d/%v)", len(items), len(*docs), final)
	outputDocs := func() {
		if len(*docs) > 0 {
			var (
				data      map[string][]interface{}
				jsonBytes []byte
				err       error
			)
			// actual output
			j.log.WithFields(logrus.Fields{"operation": "ConfluenceEnrichItems"}).Infof("output processing(%d/%d/%v)", len(items), len(*docs), final)
			data, err = j.GetModelData(ctx, *docs)
			if err == nil {
				if j.Publisher != nil {
					insightsStr := "insights"
					contentsStr := "contents"
					envStr := os.Getenv("STAGE")
					// Push the event
					d := make([]map[string]interface{}, 0)
					for k, v := range data {
						switch k {
						case "created":
							ev, _ := v[0].(insightsConf.ContentCreatedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, ConfluenceDataSource, contentsStr, envStr, v)
							for _, val := range v {
								id := fmt.Sprintf("%s-%s", "content", val.(insightsConf.ContentCreatedEvent).Payload.ID)
								d = append(d, map[string]interface{}{
									"id":   id,
									"data": "",
								})
							}
						case "updated":
							ev, _ := v[0].(insightsConf.ContentUpdatedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, ConfluenceDataSource, contentsStr, envStr, v)
						default:
							err = fmt.Errorf("unknown confluence event type '%s'", k)
						}
						if err != nil {
							break
						}
					}
					err = j.cacheProvider.Create(j.endpoint, d)
					if err != nil {
						j.log.WithFields(logrus.Fields{"operation": "ConfluenceEnrichItems"}).Errorf("error creating cache for endpoint %s. Error: %+v", j.endpoint, err)
					}
				} else {
					jsonBytes, err = jsoniter.Marshal(data)
				}
			}
			if err != nil {
				j.log.WithFields(logrus.Fields{"operation": "ConfluenceEnrichItems"}).Errorf("Error: %+v", err)
				return
			}
			if j.Publisher == nil {
				j.log.WithFields(logrus.Fields{"operation": "ConfluenceEnrichItems"}).Infof("%s", string(jsonBytes))
			}
			*docs = []interface{}{}
			gMaxUpdatedAtMtx.Lock()
			defer gMaxUpdatedAtMtx.Unlock()
			err = j.cacheProvider.SetLastSync(j.endpoint, gMaxUpdatedAt)
			if err != nil {
				j.log.WithFields(logrus.Fields{"operation": "ConfluenceEnrichItems"}).Infof("unable to set last sync date to cache.error: %v", err)
			}
		}
	}
	if final {
		defer func() {
			outputDocs()
		}()
	}
	// NOTE: non-generic code starts
	if ctx.Debug > 0 {
		j.log.WithFields(logrus.Fields{"operation": "ConfluenceEnrichItems"}).Debugf("confluence enrich items %d", len(items))
	}
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	nThreads := 0
	procItem := func(c chan error, idx int) (e error) {
		if thrN > 1 {
			mtx.RLock()
		}
		src := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// NOTE: never refer to _source - we no longer use ES
		doc, ok := src.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		var rich map[string]interface{}
		rich, e = j.EnrichItem(ctx, doc)
		if e != nil {
			return
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, rich)
		// NOTE: flush here
		if len(*docs) >= ctx.PackSize {
			outputDocs()
		}
		if thrN > 1 {
			mtx.Unlock()
		}
		return
	}
	if thrN > 1 {
		for i := range items {
			go func(i int) {
				_ = procItem(ch, i)
			}(i)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for i := range items {
		err = procItem(nil, i)
		if err != nil {
			return
		}
	}
	return
}

func main() {
	var (
		ctx        shared.Ctx
		confluence DSConfluence
	)
	confluence.createStructuredLogger()
	err := confluence.Init(&ctx)
	if err != nil {
		confluence.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("error init confluence: %+v", err)
		return
	}
	// To enable debug mode: start
	// ctx.Debug = 2
	// shared.SetSyncMode(true, true)
	// shared.SetLogLoggerError(true)
	// To enable debug mode: end
	content := "content"
	shared.SetSyncMode(true, false)
	shared.SetLogLoggerError(false)
	shared.AddLogger(&confluence.Logger, ConfluenceDataSource, logger.Internal, []map[string]string{{"CONFLUENCE_URL": confluence.URL, "ProjectSlug": ctx.Project}})
	confluence.AddCacheProvider()
	err = confluence.WriteLog(&ctx, logger.InProgress, content)
	if err != nil {
		confluence.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("WriteLog Error : %+v", err)
	}
	shared.FatalOnError(err)

	err = confluence.Sync(&ctx)
	if err != nil {
		confluence.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("error sync confluence: %+v", err)
		er := confluence.WriteLog(&ctx, logger.Failed, content+": "+err.Error())
		if er != nil {
			err = er
		}
	}
	shared.FatalOnError(err)
	err = confluence.WriteLog(&ctx, logger.Done, content)
	if err != nil {
		confluence.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("WriteLog Error : %+v", err)
	}
	shared.FatalOnError(err)
}

// createStructuredLogger...
func (j *DSConfluence) createStructuredLogger() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	log := logrus.WithFields(
		logrus.Fields{
			"environment": os.Getenv("STAGE"),
			"commit":      build.GitCommit,
			"version":     build.Version,
			"service":     build.AppName,
			"endpoint":    j.URL,
		})
	j.log = log
}

// AddCacheProvider - adds cache provider
func (j *DSConfluence) AddCacheProvider() {
	cacheProvider := cache.NewManager(ConfluenceDataSource, os.Getenv("STAGE"))
	j.cacheProvider = *cacheProvider
	j.endpoint = strings.ReplaceAll(strings.TrimPrefix(strings.TrimPrefix(j.URL, "https://"), "http://"), "/", "-")
}
