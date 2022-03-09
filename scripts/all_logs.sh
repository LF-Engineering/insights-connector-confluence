#!/bin/bash
if [ -z "$ES_LOG" ]
then
  ES_LOG="`cat ./secrets/ES_LOG_URL_FULL.secret`"
fi
if [ -z "$ES_LOG" ]
then
  echo "$0: you need to specify ES_LOG=..."
  exit 1
fi
if [ -z "$1" ]
then
  echo "$0: you need to specify date from as a 1st arg, format: 2022-03-07T00:00:00Z"
  exit 2
fi
if [ -z "$2" ]
then
  DTTO="2099-01-01T00:00:00Z"
else
  DTTO="${2}"
fi
curl -s -XPOST -H 'Content-Type: application/json' "${ES_LOG}/_sql?format=csv" -d"{\"query\":\"select created_at, status, configuration.CONFLUENCE_URL, message from \\\"insights-connector-confluence-log-dev\\\" where created_at > '${1}' and created_at <= '${DTTO}' order by created_at asc\",\"fetch_size\":10000}"
