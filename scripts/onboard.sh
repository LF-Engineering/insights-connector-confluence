#!/bin/bash
if [ -z "${1}" ]
then
  echo "$0: you need to specify confluence URL as a 1st argument"
  exit 1
fi
if [ -z "$TOKEN" ]
then
  TOKEN="`./scripts/get_token.sh`"
fi
if [ -z "$TOKEN" ]
then
  echo "$0: no TOKEN specified, existing"
  exit 2
fi
curl -s -XPOST -H 'Content-Type: application/json' -H "Authorization: Bearer ${TOKEN}" 'https://api-gw.dev.platform.linuxfoundation.org/insights-service/v2/connectors/confluence' -d"{\"confluence_url\":\"${1}\"}" | jq -rS '.'
