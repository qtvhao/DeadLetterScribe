# Use official Node.js image
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files first (for layer caching)
COPY package*.json ./

# Install dependencies
RUN npm install --production

# Copy the rest of the app
COPY . .

# Set environment variables if needed (can be overridden at runtime)
ENV NODE_ENV=production

# Default command to run the app
CMD ["npm", "start"]
