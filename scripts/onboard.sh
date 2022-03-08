#!/bin/bash
# [TOKEN="`cat ./secrets/token.secret`"] [DBG=2] ./scripts/onboard.sh 'https://wiki.o-ran-sc.org'
if [ -z "${1}" ]
then
  echo "$0: you need to specify confluence URL as a 1st argument"
  exit 1
fi
if [ -z "$TOKEN" ]
then
  TOKEN="`./scripts/get_token.sh`"
  echo "Got token: ${TOKEN}"
  echo -n "${TOKEN}" > ./secrets/token.secret
fi
if [ -z "$TOKEN" ]
then
  echo "$0: no TOKEN specified, existing"
  exit 2
else
  echo "Using provided token: ${TOKEN}"
fi
if [ ! -z "${DBG}" ]
then
  echo "curl -s -XPOST -H Content-Type: application/json -H Authorization: Bearer ${TOKEN} https://api-gw.dev.platform.linuxfoundation.org/insights-service/v2/connectors/confluence -d{\"confluence_url\":\"${1}\",\"confluence_debug\":\"${DBG}\"} | jq -rS ."
fi
if [ -z "${DBG}" ]
then
  DBG="0"
fi
curl -s -XPOST -H 'Content-Type: application/json' -H "Authorization: Bearer ${TOKEN}" 'https://api-gw.dev.platform.linuxfoundation.org/insights-service/v2/connectors/confluence' -d"{\"confluence_url\":\"${1}\",\"confluence_debug\":\"${DBG}\"}" | jq -rS '.'
