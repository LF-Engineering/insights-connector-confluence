#!/bin/bash
cookie="`cat ./secrets/cookie.secret`"
client_id="`cat ./secrets/client_id.secret`"
client_secret="`cat ./secrets/client_secret.secret`"
audience="`cat ./secrets/audience.secret`"
if ( [ -z "${cookie}" ] || [ -z "${client_id}" ] || [ -z "${client_secret}" ] || [ -z "${audience}" ] )
then
  echo "$0: you need to have a ./secrets/cookie.secret, ./secrets/client_id.secret, ./secrets/client_secret.secret and ./secrets/audience.secret files"
  exit 1
fi
curl -s -XPOST -H 'Content-Type: application/json' -H "${cookie}" 'https://linuxfoundation-dev.auth0.com/oauth/token' -d"{\"client_id\":\"${client_id}\",\"grant_type\":\"client_credentials\",\"audience\":\"${audience}\",\"client_secret\":\"${client_secret}\"}" | jq -r '.access_token'
