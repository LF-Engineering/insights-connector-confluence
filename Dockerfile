FROM alpine:3.14

WORKDIR /app

ENV CONFLUENCE_URL='<CONFLUENCE_URL>'
ENV CONFLUENCE_USER='<CONFLUENCE_USER>'
ENV CONFLUENCE_TOKEN='<CONFLUENCE_TOKEN>'
RUN apk update && apk add git
RUN apk add --no-cache bash
COPY confluence ./

CMD ./confluence --confluence-url=${CONFLUENCE_URL}  --confluence-user=${CONFLUENCE_USER} --confluence-token=${CONFLUENCE_TOKEN} $*