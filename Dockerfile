FROM node:14.16.0
ENV TZ=Europe/Rome
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
WORKDIR /usr/src/app
COPY package*.json ./
RUN apt update && apt install libreoffice -y
RUN npm install
COPY . .
CMD [ "node", "index.js" ]
