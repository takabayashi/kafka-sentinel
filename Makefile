.PHONY: help setup clean install-simulator install-velocity-monitor install-dashboard-backend install-dashboard-frontend
.PHONY: run-simulator run-velocity-monitor run-dashboard-backend run-dashboard-frontend run-dashboard
.PHONY: config-simulator config-velocity-monitor config-dashboard-backend config-dashboard-frontend config-all
.PHONY: dev start stop
.PHONY: check-platform check-flink check-topics check-schemas check-service-account

# Default target
help:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Kafka Anomaly Detection - Makefile Commands"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "📦 SETUP & INSTALLATION"
	@echo "  make setup                    Install all dependencies"
	@echo "  make install-simulator        Install simulator dependencies"
	@echo "  make install-velocity-monitor Install velocity monitor dependencies"
	@echo "  make install-dashboard-backend Install dashboard backend dependencies"
	@echo "  make install-dashboard-frontend Install dashboard frontend dependencies"
	@echo ""
	@echo "⚙️  CONFIGURATION"
	@echo "  make config-all               Copy all .env.example files"
	@echo "  make config-simulator         Copy simulator .env.example"
	@echo "  make config-velocity-monitor  Copy velocity-monitor .env.example"
	@echo "  make config-dashboard-backend Copy dashboard backend .env.example"
	@echo "  make config-dashboard-frontend Copy dashboard frontend .env.example"
	@echo ""
	@echo "🚀 RUN COMPONENTS"
	@echo "  make dev                      Start all components (recommended for demo)"
	@echo "  make run-simulator            Run data simulator"
	@echo "  make run-velocity-monitor     Run velocity monitor"
	@echo "  make run-dashboard-backend    Run dashboard backend (WebSocket server)"
	@echo "  make run-dashboard-frontend   Run dashboard frontend (React UI)"
	@echo "  make run-dashboard            Run both dashboard backend & frontend"
	@echo ""
	@echo "🔍 PLATFORM HEALTH CHECK"
	@echo "  make check-platform           Check all platform components (Flink, Kafka, Schema Registry)"
	@echo "  make check-flink              Check Flink compute pool and statements"
	@echo "  make check-topics             Check Kafka topics"
	@echo "  make check-schemas            Check Schema Registry"
	@echo "  make check-service-account    Check service account and permissions"
	@echo ""
	@echo "🧹 CLEANUP"
	@echo "  make clean                    Remove all virtual envs and node_modules"
	@echo "  make clean-python             Remove Python virtual environments"
	@echo "  make clean-node               Remove node_modules"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ============================================================================
# SETUP & INSTALLATION
# ============================================================================

setup: install-simulator install-velocity-monitor install-dashboard-backend install-dashboard-frontend
	@echo "✅ All dependencies installed successfully"

install-simulator:
	@echo "📦 Installing simulator dependencies..."
	@cd simulator && python3 -m venv venv && \
		./venv/bin/pip install --upgrade pip && \
		./venv/bin/pip install -r requirements.txt
	@echo "✅ Simulator dependencies installed"

install-velocity-monitor:
	@echo "📦 Installing velocity-monitor dependencies..."
	@cd velocity-monitor && python3 -m venv venv && \
		./venv/bin/pip install --upgrade pip && \
		./venv/bin/pip install -r requirements.txt
	@echo "✅ Velocity-monitor dependencies installed"

install-dashboard-backend:
	@echo "📦 Installing dashboard backend dependencies..."
	@cd dashboard/backend && npm install
	@echo "✅ Dashboard backend dependencies installed"

install-dashboard-frontend:
	@echo "📦 Installing dashboard frontend dependencies..."
	@cd dashboard/frontend && npm install
	@echo "✅ Dashboard frontend dependencies installed"

# ============================================================================
# CONFIGURATION
# ============================================================================

config-all: config-simulator config-velocity-monitor config-dashboard-backend config-dashboard-frontend
	@echo "✅ All .env files created. Please edit them with your Confluent Cloud credentials."

config-simulator:
	@if [ ! -f simulator/.env ]; then \
		cp simulator/.env.example simulator/.env; \
		echo "✅ Created simulator/.env"; \
	else \
		echo "⚠️  simulator/.env already exists, skipping"; \
	fi

config-velocity-monitor:
	@if [ ! -f velocity-monitor/.env ]; then \
		cp velocity-monitor/.env.example velocity-monitor/.env; \
		echo "✅ Created velocity-monitor/.env"; \
	else \
		echo "⚠️  velocity-monitor/.env already exists, skipping"; \
	fi

config-dashboard-backend:
	@if [ ! -f dashboard/backend/.env ]; then \
		cp dashboard/backend/.env.example dashboard/backend/.env; \
		echo "✅ Created dashboard/backend/.env"; \
	else \
		echo "⚠️  dashboard/backend/.env already exists, skipping"; \
	fi

config-dashboard-frontend:
	@if [ ! -f dashboard/frontend/.env ]; then \
		cp dashboard/frontend/.env.example dashboard/frontend/.env; \
		echo "✅ Created dashboard/frontend/.env"; \
	else \
		echo "⚠️  dashboard/frontend/.env already exists, skipping"; \
	fi

# ============================================================================
# RUN COMPONENTS
# ============================================================================

run-simulator:
	@echo "🚀 Starting simulator..."
	@if [ ! -f simulator/.env ]; then \
		echo "❌ Error: simulator/.env not found. Run 'make config-simulator' first"; \
		exit 1; \
	fi
	@cd simulator && ./venv/bin/python main.py

run-velocity-monitor:
	@echo "🚀 Starting velocity monitor..."
	@if [ ! -f velocity-monitor/.env ]; then \
		echo "❌ Error: velocity-monitor/.env not found. Run 'make config-velocity-monitor' first"; \
		exit 1; \
	fi
	@cd velocity-monitor && ./venv/bin/python monitor.py

run-dashboard-backend:
	@echo "🚀 Starting dashboard backend (WebSocket server)..."
	@if [ ! -f dashboard/backend/.env ]; then \
		echo "❌ Error: dashboard/backend/.env not found. Run 'make config-dashboard-backend' first"; \
		exit 1; \
	fi
	@cd dashboard/backend && npm start

run-dashboard-frontend:
	@echo "🚀 Starting dashboard frontend..."
	@cd dashboard/frontend && npm run dev

run-dashboard:
	@echo "🚀 Starting dashboard (backend + frontend)..."
	@if [ ! -f dashboard/backend/.env ]; then \
		echo "❌ Error: dashboard/backend/.env not found. Run 'make config-dashboard-backend' first"; \
		exit 1; \
	fi
	@trap 'kill 0' EXIT; \
		(cd dashboard/backend && npm start) & \
		(cd dashboard/frontend && npm run dev) & \
		wait

dev:
	@echo "🚀 Starting all components for demo..."
	@echo ""
	@echo "   📊 Dashboard UI: http://localhost:3000"
	@echo "   🔌 WebSocket: ws://localhost:8080"
	@echo ""
	@echo "Press Ctrl+C to stop all services"
	@echo ""
	@trap 'kill 0' EXIT; \
		(cd simulator && ./venv/bin/python main.py) & \
		(cd velocity-monitor && ./venv/bin/python monitor.py) & \
		(cd dashboard/backend && npm start) & \
		(cd dashboard/frontend && npm run dev) & \
		wait

# ============================================================================
# CLEANUP
# ============================================================================

clean: clean-python clean-node
	@echo "✅ Cleanup complete"

clean-python:
	@echo "🧹 Removing Python virtual environments..."
	@rm -rf simulator/venv simulator/__pycache__
	@rm -rf velocity-monitor/venv velocity-monitor/__pycache__
	@echo "✅ Python environments removed"

clean-node:
	@echo "🧹 Removing node_modules..."
	@rm -rf dashboard/backend/node_modules
	@rm -rf dashboard/frontend/node_modules
	@echo "✅ Node modules removed"

# ============================================================================
# DEVELOPMENT HELPERS
# ============================================================================

.PHONY: check-config check-deps logs-simulator logs-velocity-monitor

check-config:
	@echo "📋 Checking configuration files..."
	@echo ""
	@if [ -f simulator/.env ]; then \
		echo "✅ simulator/.env exists"; \
	else \
		echo "❌ simulator/.env missing - run 'make config-simulator'"; \
	fi
	@if [ -f velocity-monitor/.env ]; then \
		echo "✅ velocity-monitor/.env exists"; \
	else \
		echo "❌ velocity-monitor/.env missing - run 'make config-velocity-monitor'"; \
	fi
	@if [ -f dashboard/backend/.env ]; then \
		echo "✅ dashboard/backend/.env exists"; \
	else \
		echo "❌ dashboard/backend/.env missing - run 'make config-dashboard-backend'"; \
	fi
	@if [ -f dashboard/frontend/.env ]; then \
		echo "✅ dashboard/frontend/.env exists"; \
	else \
		echo "❌ dashboard/frontend/.env missing - run 'make config-dashboard-frontend'"; \
	fi

check-deps:
	@echo "📋 Checking dependencies..."
	@echo ""
	@if [ -d simulator/venv ]; then \
		echo "✅ simulator dependencies installed"; \
	else \
		echo "❌ simulator dependencies missing - run 'make install-simulator'"; \
	fi
	@if [ -d velocity-monitor/venv ]; then \
		echo "✅ velocity-monitor dependencies installed"; \
	else \
		echo "❌ velocity-monitor dependencies missing - run 'make install-velocity-monitor'"; \
	fi
	@if [ -d dashboard/backend/node_modules ]; then \
		echo "✅ dashboard backend dependencies installed"; \
	else \
		echo "❌ dashboard backend dependencies missing - run 'make install-dashboard-backend'"; \
	fi
	@if [ -d dashboard/frontend/node_modules ]; then \
		echo "✅ dashboard frontend dependencies installed"; \
	else \
		echo "❌ dashboard frontend dependencies missing - run 'make install-dashboard-frontend'"; \
	fi

# ============================================================================
# PLATFORM HEALTH CHECK
# ============================================================================

check-platform:
	@./infra/check-platform-simple.sh

check-flink:
	@echo "🔍 Checking Flink infrastructure..."
	@cd infra/terraform && terraform output flink_compute_pool_id && terraform output flink_catalog_tables

check-topics:
	@echo "🔍 Checking Kafka topics..."
	@cd infra/terraform && terraform output topic_names

check-schemas:
	@echo "🔍 Checking Schema Registry..."
	@echo "Run velocity-monitor to register schemas automatically"

check-service-account:
	@echo "🔍 Checking service account..."
	@cd infra/terraform && terraform output flink_service_account_id
