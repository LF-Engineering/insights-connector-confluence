package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Ctx - environment context packed in structure
type Ctx struct {
	DSPrefix      string
	Debug         int        // From CONFLUENCE_DEBUG Debug level: 0-no, 1-info, 2-verbose
	Retry         int        // From CONFLUENCE_RETRY: how many times retry failed operatins, default 5
	ST            bool       // From CONFLUENCE_ST true: use single threaded version, false: use multi threaded version, default false
	NCPUs         int        // From CONFLUENCE_NCPUS, set to override number of CPUs to run, this overwrites CONFLUENCE_ST, default 0 (which means do not use it, use all CPU reported by go library)
	NCPUsScale    float64    // From CONFLUENCE_NCPUS_SCALE, scale number of CPUs, for example 2.0 will report number of cpus 2.0 the number of actually available CPUs
	Tags          []string   // From CONFLUENCE_TAGS - tag
	DryRun        bool       // From CONFLUENCE_DRY_RUN - do only requests that read data, no write to anything (excluding cache - this one can be written in dry-run mode - still can be disabled with NoCache)
	Project       string     // From CONFLUENCE_PROJECT - set project can be for example "ONAP"
	ProjectFilter bool       // From CONFLUENCE_PROJECT_FILTER - set project filter (if this is set for example via 'p2o: true' fixture flag, this says that DS should internally filter by project, otherwise it means that no internal project filtering is needed and this is only used to set project on ES documents)
	DateFrom      *time.Time // From CONFLUENCE_DATE_FROM
	DateTo        *time.Time // From CONFLUENCE_DATE_TO
}

// Env - get env value using current DS prefix
func (ctx *Ctx) Env(v string) string {
	return os.Getenv(ctx.DSPrefix + v)
}

// BoolEnv - parses env variable as bool
// returns false for anything that was parsed as false, zero, empty etc:
// f, F, false, False, fALSe, 0, "", 0.00
// else returns true
func (ctx *Ctx) BoolEnv(k string) bool {
	v := os.Getenv(ctx.DSPrefix + k)
	return StringToBool(v)
}

// Init - get context from environment variables
func (ctx *Ctx) Init() {
	ctx.DSPrefix = "CONFLUENCE_"

	// Debug
	if !ctx.BoolEnv("DEBUG") {
		ctx.Debug = 0
	} else {
		debugLevel, err := strconv.Atoi(ctx.Env("DEBUG"))
		FatalOnError(err)
		if debugLevel != 0 {
			ctx.Debug = debugLevel
		}
	}

	// Retry
	if !ctx.BoolEnv("RETRY") {
		ctx.Retry = 5
	} else {
		retry, err := strconv.Atoi(ctx.Env("RETRY"))
		FatalOnError(err)
		if retry != 0 {
			ctx.Retry = retry
		}
	}

	// Threading
	ctx.ST = ctx.BoolEnv("ST")
	// NCPUs
	if !ctx.BoolEnv("NCPUS") {
		ctx.NCPUs = 0
	} else {
		nCPUs, err := strconv.Atoi(ctx.Env("NCPUS"))
		FatalOnError(err)
		if nCPUs > 0 {
			ctx.NCPUs = nCPUs
			if ctx.NCPUs == 1 {
				ctx.ST = true
			}
		}
	}
	if !ctx.BoolEnv("NCPUS_SCALE") {
		ctx.NCPUsScale = 1.0
	} else {
		nCPUsScale, err := strconv.ParseFloat(ctx.Env("NCPUS_SCALE"), 64)
		FatalOnError(err)
		if nCPUsScale > 0 {
			ctx.NCPUsScale = nCPUsScale
		}
	}

	// Tags
	tags := ctx.Env("TAGS")
	ctx.Tags = strings.Split(tags, ",")

	// Dry run
	ctx.DryRun = ctx.BoolEnv("DRY_RUN")

	// Project, Project slug, Category, Groups
	ctx.Project = ctx.Env("PROJECT")
	ctx.ProjectFilter = ctx.BoolEnv("PROJECT_FILTER")

	// Date from/to (optional)
	if ctx.Env("DATE_FROM") != "" {
		t, err := TimeParseAny(ctx.Env("DATE_FROM"))
		FatalOnError(err)
		ctx.DateFrom = &t
	}
	if ctx.Env("DATE_TO") != "" {
		t, err := TimeParseAny(ctx.Env("DATE_TO"))
		FatalOnError(err)
		ctx.DateTo = &t
	}
}

// Print context contents
func (ctx *Ctx) Print() {
	fmt.Printf("Environment Context Dump\n%+v\n", ctx)
}

// Info - return context in human readable form
func (ctx Ctx) Info() string {
	return fmt.Sprintf("%+v", ctx)
}
