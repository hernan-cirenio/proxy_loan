FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY src ./src
COPY public ./public
COPY worker ./worker
COPY .env.example ./

EXPOSE 3000

CMD ["node", "src/server.js"]
