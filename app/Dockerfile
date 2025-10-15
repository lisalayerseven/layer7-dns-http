FROM node:20.17.0-slim
WORKDIR /usr/src/app
RUN apt update && apt install -y dnsutils && rm -rf /var/lib/apt/lists/*
COPY package*.json ./
RUN npm install --omit=dev
COPY . .
CMD ["node", "index.js"]
