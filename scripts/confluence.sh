#!/bin/bash
./confluence --confluence-url='https://wiki.lfnetworking.org' --confluence-es-url="`cat ./secrets/ES_URL.prod.secret`" --confluence-user="`cat ./secrets/user.secret`" --confluence-token="`cat ./secrets/token.secret`" $*
