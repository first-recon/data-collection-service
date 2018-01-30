FROM node:9.4

WORKDIR /src

COPY . .

RUN npm install -g yarn
RUN yarn install

EXPOSE 3000

CMD ["npm", "start"]