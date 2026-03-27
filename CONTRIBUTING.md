# Contributing to Kafka Sentinel

Thank you for your interest in contributing! This document provides guidelines for contributing to Kafka Sentinel.

## How to Contribute

### Reporting Issues

- Use GitHub Issues to report bugs or request features
- Search existing issues before creating a new one
- Provide clear reproduction steps for bugs
- Include environment details (Python version, Confluent Cloud cluster tier, etc.)

### Development Setup

1. **Fork and clone the repository**

```bash
git clone https://github.com/YOUR_USERNAME/kafka-sentinel.git
cd kafka-sentinel
```

2. **Set up development environment**

Each component has its own virtual environment:

```bash
# Simulator
cd simulator
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Velocity Monitor
cd ../velocity-monitor
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. **Configure Confluent Cloud access**

- Create a test Kafka cluster (Basic or Standard tier)
- Generate Kafka API keys at cluster level
- Copy `.env.example` to `.env` in each component directory
- Add your credentials

### Making Changes

1. **Create a feature branch**

```bash
git checkout -b feature/your-feature-name
```

2. **Make your changes**

- Follow existing code style (Python: PEP 8)
- Add docstrings to new functions/classes
- Update README if adding new features
- Test your changes thoroughly

3. **Commit your changes**

```bash
git add .
git commit -m "feat: add awesome new feature"
```

Use conventional commit messages:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `refactor:` - Code refactoring
- `test:` - Adding tests
- `chore:` - Maintenance tasks

4. **Push and create a pull request**

```bash
git push origin feature/your-feature-name
```

Then create a PR on GitHub with:
- Clear description of changes
- Reference to related issues
- Test results/screenshots if applicable

### Code Style

**Python:**
- Follow PEP 8 style guide
- Use type hints where appropriate
- Maximum line length: 100 characters
- Use meaningful variable names

**Terraform:**
- Use consistent indentation (2 spaces)
- Add comments for complex logic
- Follow HashiCorp style conventions

### Testing

- Test locally before submitting PR
- Verify changes don't break existing functionality
- Test against real Confluent Cloud cluster when possible
- Document manual testing steps in PR description

### Documentation

- Update README.md for user-facing changes
- Update AGENTS.md for architecture changes
- Add inline comments for complex logic
- Include examples in docstrings

## Project Structure

```
├── infra/terraform/    # Infrastructure as code
├── simulator/          # Python data generator
├── velocity-monitor/   # Python metrics collector
├── flink/             # Flink SQL (future)
├── ai-agent/          # AI enrichment (future)
├── dashboard/         # React UI (future)
└── config/            # Generated configs
```

## Development Workflow

1. Check PROGRESS.md for current status
2. Review AGENTS.md for architecture details
3. Read CLAUDE.md for development guidance
4. Start with small, focused changes
5. Test thoroughly before submitting
6. Update documentation

## API Key Best Practices

⚠️ **Never commit API keys or credentials**

- Use `.env` files (gitignored)
- Use `.example` files for templates
- Document which credentials are needed where
- **Kafka API Keys**: Use for both REST API v3 AND Kafka produce/consume
- **Cloud API Keys**: Only for Terraform (infrastructure management)

## Questions?

- Open a GitHub Discussion for general questions
- Use Issues for bugs and feature requests
- Check existing documentation in `AGENTS.md` and component READMEs

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
