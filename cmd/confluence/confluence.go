package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/LF-Engineering/insights-datasource-confluence/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	jsoniter "github.com/json-iterator/go"
)

const (
	// ConfluenceBackendVersion - backend version
	ConfluenceBackendVersion = "0.1.0"
)

var (
	// ConfluenceDefaultMaxContents - max contents to fetch at a time
	ConfluenceDefaultMaxContents = 1000
)

// DSConfluence - DS implementation for confluence - does nothing at all, just presents a skeleton code
type DSConfluence struct {
	URL             string // Confluence instance URL, for example https://wiki.lfnetworking.org
	MaxContents     int    // Defaults to ConfluenceDefaultMaxContents (1000)
	User            string // If user is provided then we assume that we don't have base64 encoded user:token yet
	Token           string // If user is not specified we assume that token already contains "<username>:<your-api-token>"
	FlagURL         *string
	FlagMaxContents *int
	FlagUser        *string
	FlagToken       *string
}

// AddFlags - add confluence specific flags
func (j *DSConfluence) AddFlags() {
	j.FlagURL = flag.String("confluence-url", "", "Confluence instance URL, for example https://wiki.lfnetworking.org")
	j.FlagMaxContents = flag.Int("confluence-max-contents", ConfluenceDefaultMaxContents, "Max Contents - defaults to ConfluenceDefaultMaxContents (1000)")
	j.FlagUser = flag.String("confluence-user", "", "User: if user is provided then we assume that we don't have base64 encoded user:token yet")
	j.FlagToken = flag.String("confluence-token", "", "Token: if user is not specified we assume that token already contains \"<username>:<your-api-token>\"")
}

// ParseArgs - parse confluence specific environment variables
func (j *DSConfluence) ParseArgs(ctx *shared.Ctx) (err error) {
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

	// SSO User
	if shared.FlagPassed(ctx, "user") && *j.FlagUser != "" {
		j.User = *j.FlagUser
	}
	if ctx.EnvSet("USER") {
		j.User = ctx.Env("USER")
	}
	if j.User != "" {
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
		shared.AddRedacted(j.Token, false)
	}

	// SSO: Handle either user,token pair or just a token
	if j.User != "" {
		// If user is specified, then we must calculate base64(user:token) to get a real token
		j.Token = base64.StdEncoding.EncodeToString([]byte(j.User + ":" + j.Token))
		shared.AddRedacted(j.Token, false)
	}
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
	ctx.InitEnv("confluence")
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
		shared.Printf("confluence: %+v\nshared context: %s\n", j, ctx.Info())
	}
	return
}

// Sync - sync confluence data source
func (j *DSConfluence) Sync(ctx *shared.Ctx) (data *models.Data, err error) {
	_ = shared.GetThreadsNum(ctx)
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.URL)
	}
	/*
		if !ctx.NoRaw {
			err = lib.FetchRaw(ctx, ds)
			if err != nil {
				lib.Printf("%s: FetchRaw(%s) error: %v\n", ds.Info(), ctx.Info(), err)
				return
			}
		}
		if ctx.Enrich {
			err = lib.Enrich(ctx, ds)
			if err != nil {
				lib.Printf("%s: Enrich(%s) error: %v\n", ds.Info(), ctx.Info(), err)
				return
			}
		}
	*/
	data = &models.Data{}
	return
}

func main() {
	var (
		ctx        shared.Ctx
		confluence DSConfluence
	)
	err := confluence.Init(&ctx)
	if err != nil {
		fmt.Printf("Error: %+v\n", err)
		return
	}
	data, err := confluence.Sync(&ctx)
	if err != nil {
		fmt.Printf("Error: %+v\n", err)
		return
	}
	jsonBytes, err := jsoniter.Marshal(data)
	if err != nil {
		fmt.Printf("Error: %+v\n", err)
		return
	}
	fmt.Printf("%s\n", string(jsonBytes))
}
