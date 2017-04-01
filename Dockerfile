FROM mhart/alpine-node:latest

RUN apk update && apk upgrade && apk --update add bash curl vim && rm -rf /var/cache/apk/*

ADD . .
RUN npm install

EXPOSE 5000

CMD ["node", "src/server.js"]
