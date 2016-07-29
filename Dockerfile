FROM node:6

RUN apt-get update && apt-get -y install python2.7

WORKDIR /var/www/
ADD package.json /tmp/package.json
RUN cd /tmp && npm install
RUN npm install -g sails grunt-cli
RUN cp -a /tmp/node_modules /var/www/node_modules

COPY api /var/www/api
COPY assets /var/www/assets
COPY config /var/www/config
COPY tasks /var/www/tasks
COPY views /var/www/views
COPY Gruntfile.js app.js package.json /var/www/

EXPOSE 3000
ENTRYPOINT ["sails","lift","--prod","--port=3000"]