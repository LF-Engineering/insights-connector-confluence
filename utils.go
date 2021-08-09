package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// StringToBool - convert string value to boolean value
// returns false for anything that was parsed as false, zero, empty etc:
// f, F, false, False, fALSe, 0, "", 0.00
// else returns true
func StringToBool(v string) bool {
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err == nil {
		return b
	}
	f, err := strconv.ParseFloat(v, 64)
	if err == nil {
		return f != 0.0
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err == nil {
		return i != 0
	}
	if v == "no" || v == "n" {
		return false
	}
	return true
}

// TimeParseAny - attempts to parse time from string YYYY-MM-DD HH:MI:SS
// Skipping parts from right until only YYYY id left
func TimeParseAny(dtStr string) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05.000000",
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02",
		"2006-01",
		"2006",
	}
	for _, format := range formats {
		t, e := time.Parse(format, dtStr)
		if e == nil {
			return t, e
		}
	}
	e := fmt.Errorf("Error:\nCannot parse date: '%v'", dtStr)
	return time.Now(), e
}
