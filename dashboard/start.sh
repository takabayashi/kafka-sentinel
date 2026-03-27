#!/bin/bash

# Quick start script for the dashboard

echo "Starting Kafka Anomaly Detection Dashboard..."
echo ""

# Check if .env files exist
if [ ! -f backend/.env ]; then
    echo "⚠️  backend/.env not found. Please copy backend/.env.example and configure it."
    exit 1
fi

# Install dependencies if needed
if [ ! -d "backend/node_modules" ]; then
    echo "📦 Installing backend dependencies..."
    cd backend && npm install && cd ..
fi

if [ ! -d "frontend/node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    cd frontend && npm install && cd ..
fi

# Start backend and frontend in parallel
echo ""
echo "🚀 Starting services..."
echo "   Backend WebSocket: ws://localhost:8080"
echo "   Frontend UI: http://localhost:3000"
echo ""

trap 'kill 0' EXIT

cd backend && npm start &
cd frontend && npm run dev &

wait
