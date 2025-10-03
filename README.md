# s3_vectors_rag_hands_on

A hands-on project for S3 vectors RAG implementation.

## Usage

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd s3_vectors_rag_hands_on

# Install dependencies
uv sync

# Install development dependencies
uv sync --dev

# Install pre-commit hooks
uv run pre-commit install
```

### Development

```bash
# Run tests
uv run pytest

# Run linting
uv run ruff check .

# Run type checking
uv run pyright

# Format code
uv run ruff format .
```

### Running the application

```bash
uv run s3_vectors_rag_hands_on
```

## Project Structure

```
.
├── src/
│   └── s3_vectors_rag_hands_on/
│       ├── __init__.py
│       └── config.py
├── tests/
├── pyproject.toml
└── uv.lock
```

## Requirements

- Python >= 3.12
- uv package manager

## License

MIT License