#!/bin/bash
if [ -z "$ES_LOG" ]
then
  ES_LOG="`cat ./secrets/ES_LOG_URL.secret`"
fi
if [ -z "$ES_LOG" ]
then
  echo "$0: you need to specify ES_LOG=..."
  exit 1
fi
if [ -z "$1" ]
then
  echo "$0: you need to specify confluence URL as a 1st arg"
  exit 2
fi
if [ -z "$2" ]
then
  echo "$0: you need to specify date from as a 2nd arg, format: 2022-03-07T00:00:00Z"
  exit 3
fi
if [ -z "$3" ]
then
  DTTO="2099-01-01T00:00:00Z"
else
  DTTO="${3}"
fi
curl -s -XPOST -H 'Content-Type: application/json' "${ES_LOG}/_sql?format=csv" -d"{\"query\":\"select created_at, status, message from \\\"insights-connector-confluence-log-dev\\\" where configuration.CONFLUENCE_URL = '${1}' and created_at > '${2}' and created_at <= '${DTTO}' order by created_at asc\",\"fetch_size\":10000}"
