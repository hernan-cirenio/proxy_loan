FROM node:20-alpine

WORKDIR /app

RUN apk add --no-cache python3 py3-pip make g++

COPY package.json ./
COPY worker/requirements.txt ./worker/requirements.txt

# Alpine's system pip enforces PEP 668 (externally managed environment).
# We explicitly allow system-wide installs inside the container build.
RUN npm install --omit=dev \
  && python3 -m pip install --no-cache-dir --break-system-packages -r worker/requirements.txt

COPY src ./src
COPY public ./public
COPY worker ./worker
COPY .env.example ./

EXPOSE 3000

CMD ["node", "src/server.js"]
