FROM alpine:3.14

WORKDIR /app

ENV REPO_URL='<CONFLUENCE-REPO-URL>'
ENV ES_URL='<CONFLUENCE-ES-URL>'
ENV CONFLUENCE_USER='<CONFLUENCE_USER>'
ENV CONFLUENCE_TOKEN='<CONFLUENCE_TOKEN>'
RUN apk update && apk add git
RUN apk add --no-cache bash
COPY confluence ./

CMD ./confluence --confluence-url=${CONFLUENCE_URL} --confluence-es-url=${ES_URL}  --confluence-user=${CONFLUENCE_USER} --confluence-token=${CONFLUENCE_TOKEN} $*