#!/bin/bash
# ESENV=prod|test
if [ -z "${ESENV}" ]
then
  ESENV=test
fi
./confluence --confluence-url='https://wiki.lfnetworking.org' --confluence-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" --confluence-user="`cat ./secrets/user.secret`" --confluence-token="`cat ./secrets/token.secret`" $*
