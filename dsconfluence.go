package main

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/LF-Engineering/ds-confluence/gen/models"
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
	URL         string // From DA_CONFLUENCE_URL - Group name like GROUP-topic
	MaxContents int    // From DA_CONFLUENCE_MAX_CONTENTS, defaults to ConfluenceDefaultMaxContents (200)
	User        string // From DA_CONFLUENCE_USER - if user is provided then we assume that we don't have base64 encoded user:token yet
	Token       string // From DA_CONFLUENCE_TOKEN - if user is not specified we assume that token already contains "<username>:<your-api-token>"
}

func noSSLVerify() {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
}

// ParseArgs - parse confluence specific environment variables
func (j *DSConfluence) ParseArgs(ctx *Ctx) (err error) {
	prefix := "CONFLUENCE"
	j.URL = os.Getenv(prefix + "URL")
	if ctx.Env("MAX_CONTENTS") != "" {
		maxContents, err := strconv.Atoi(ctx.Env("MAX_CONTENTS"))
		FatalOnError(err)
		if maxContents > 0 {
			j.MaxContents = maxContents
		}
	} else {
		j.MaxContents = ConfluenceDefaultMaxContents
	}
	j.Token = os.Getenv(prefix + "TOKEN")
	j.User = os.Getenv(prefix + "USER")
	if j.User != "" {
		// If user is specified, then we must calculate base64(user:token) to get a real token
		j.Token = base64.StdEncoding.EncodeToString([]byte(j.User + ":" + j.Token))
	}
	return
}

// Validate - is current DS configuration OK?
func (j *DSConfluence) Validate(ctx *Ctx) (err error) {
	j.URL = strings.TrimSpace(j.URL)
	if strings.HasSuffix(j.URL, "/") {
		j.URL = j.URL[:len(j.URL)-1]
	}
	if j.URL == "" {
		err = fmt.Errorf("URL must be set")
	}
	return
}

func getConfluenceData() (data *models.Data, err error) {
	data = &models.Data{}
	return
}

func main() {
	noSSLVerify()
	data, err := getConfluenceData()
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
