#!/bin/bash
export AWS_REGION="`cat ./secrets/AWS_REGION.dev.secret`"
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.dev.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.dev.secret`"
export ENCRYPTION_KEY="`cat ./secrets/ENCRYPTION_KEY.dev.secret`"
export ENCRYPTION_BYTES="`cat ./secrets/ENCRYPTION_BYTES.dev.secret`"
export ESURL="`cat ./secrets/ES_URL.prod.secret`"
export STREAM=''
export CONFLUENCE_NO_INCREMENTAL=1
# curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/last-update-cache/_delete_by_query" -d'{"query":{"term":{"key.keyword":"Confluence:https://wiki.lfnetworking.org"}}}' | jq -rS '.' || exit 1
#../insights-datasource-github/encrypt "`cat ./secrets/user.secret`" > ./secrets/user.encrypted.secret || exit 2
#../insights-datasource-github/encrypt "`cat ./secrets/token.secret`" > ./secrets/token.encrypted.secret || exit 3
#./confluence --confluence-url='https://wiki.riscv.org' --confluence-debug=0 --confluence-es-url="${ESURL}" --confluence-user="`cat ./secrets/user.encrypted.secret`" --confluence-token="`cat ./secrets/token.encrypted.secret`" --confluence-stream="${STREAM}" $* 2>&1 | tee run.log
#./confluence --confluence-url='https://wiki.lfai.foundation' --confluence-debug=0 --confluence-es-url="${ESURL}" --confluence-stream="${STREAM}" $* 2>&1 | tee run2.log
./confluence --confluence-url='https://wiki.openmainframeproject.org' --confluence-debug=1 --confluence-es-url="${ESURL}" --confluence-stream="${STREAM}" $* 2>&1 | tee run3.log
