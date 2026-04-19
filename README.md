# Corporate Credit Rating Data Pipeline

A production-ready data pipeline that extracts corporate metadata from Excel MASTER sheets,
models it in a PostgreSQL dimensional warehouse with temporal tracking, and exposes it
through a comprehensive RESTful API. Fully containerized with Docker Compose.

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Excel Files    │────▶│  Extraction  │────▶│  Validation  │────▶│  Transform   │
│  (.xlsm)        │     │  (Parser)    │     │  (Rules)     │     │  (Dims/Facts)│
└─────────────────┘     └──────────────┘     └──────────────┘     └──────┬───────┘
                                                                         │
┌─────────────────┐     ┌──────────────┐     ┌──────────────┐           │
│  FastAPI        │◀────│  PostgreSQL  │◀────│    Load      │◀──────────┘
│  REST API       │     │  Warehouse   │     │  (Idempotent)│
└─────────────────┘     └──────────────┘     └──────────────┘
```

## Quick Start

### One-command startup:

```bash
docker-compose up -d
```

This will:
1. Start PostgreSQL 15 with persistent storage
2. Initialize the database schema
3. Run the ETL pipeline (extract, validate, transform, load)
4. Start the FastAPI server on port 8000

### Access the API:

- **Swagger Docs:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **Health Check:** http://localhost:8000/health

## Project Structure

```
├── src/
│   ├── config.py                    # Environment-based configuration
│   ├── extraction/
│   │   ├── excel_parser.py          # MASTER sheet parser (key-value structure)
│   │   └── data_quality.py          # Per-file quality assessment
│   ├── models/
│   │   ├── database.py              # SQLAlchemy engine/session management
│   │   ├── schema.py                # ORM models (star schema)
│   │   └── pydantic_models.py       # API request/response schemas
│   ├── etl/
│   │   ├── pipeline.py              # ETL orchestrator (Extract→Validate→Transform→Load)
│   │   ├── transformer.py           # Dimension resolution, SCD Type 2, versioning
│   │   ├── loader.py                # DB loading with idempotency
│   │   └── validator.py             # 8-rule validation framework
│   ├── api/
│   │   ├── main.py                  # FastAPI app with lifespan events
│   │   └── routes/
│   │       ├── companies.py         # /companies endpoints
│   │       ├── snapshots.py         # /snapshots endpoints
│   │       └── uploads.py           # /uploads and /pipeline endpoints
│   └── db/
│       └── init_schema.sql          # DDL (star schema + views)
├── tests/
│   ├── test_extraction.py           # 31 extraction tests
│   ├── test_validation.py           # 14 validation tests
│   ├── test_quality.py              # 6 quality assessment tests
│   └── test_api.py                  # API endpoint structure tests
├── data/                            # Excel source files
├── sample_outputs/                  # Example outputs
│   ├── api_examples.md              # 12+ API response examples
│   ├── data_quality_report.json     # Sample quality report
│   └── pipeline_execution_log.txt   # Sample pipeline log
├── docker-compose.yml               # Full container orchestration
├── Dockerfile                       # API container build
├── requirements.txt                 # Python dependencies
├── run_pipeline.py                  # CLI pipeline runner
└── AI_USAGE.md                      # AI usage disclosure
```

## Data Model

### Star Schema (PostgreSQL)

| Table | Type | Purpose |
|-------|------|---------|
| `dim_company` | Dimension (SCD Type 2) | Company identity with temporal tracking |
| `dim_sector` | Dimension | Corporate sector classification |
| `dim_country` | Dimension | Country of origin |
| `dim_currency` | Dimension | Reporting currency |
| `dim_rating_methodology` | Dimension | Rating methodologies |
| `fact_company_snapshot` | Fact | One row per file extraction (immutable) |
| `file_upload` | Audit | File lineage and processing status |
| `pipeline_run` | State | Pipeline execution tracking |
| `data_quality_report` | Quality | Per-file quality metrics |

### BI-Friendly Views

- `vw_company_snapshot_detail` — Flattened join of all dimensions
- `vw_latest_company_snapshot` — Latest snapshot per company

## API Endpoints

### Companies
| Method | Endpoint | Description | Requirement |
|--------|----------|-------------|-------------|
| GET | `/api/v1/companies` | List all companies | — |
| GET | `/api/v1/companies/{id}` | Get company latest version | — |
| GET | `/api/v1/companies/{id}/versions` | All versions for a company | #4 Version Control |
| GET | `/api/v1/companies/{id}/history` | Time-series evolution | #3, #6 Time-Series |
| GET | `/api/v1/companies/compare` | Point-in-time comparison | #2 Comparison |

### Snapshots
| Method | Endpoint | Description | Requirement |
|--------|----------|-------------|-------------|
| GET | `/api/v1/snapshots` | List with filters (sector, country, currency, date) | #5, #8 BI |
| GET | `/api/v1/snapshots/{id}` | Specific snapshot details | — |
| GET | `/api/v1/snapshots/latest` | Latest snapshot per company | — |

### Uploads & Audit
| Method | Endpoint | Description | Requirement |
|--------|----------|-------------|-------------|
| GET | `/api/v1/uploads` | List all file uploads | #1 Historical Tracking |
| GET | `/api/v1/uploads/stats` | Upload statistics | — |
| GET | `/api/v1/uploads/{id}/details` | Upload details with lineage | — |
| GET | `/api/v1/uploads/{id}/file` | Download original file | #1 Audit |
| GET | `/api/v1/uploads/pipeline/runs` | Pipeline execution history | — |
| GET | `/api/v1/uploads/pipeline/quality-reports` | Quality reports | #7 Validation |

## Business Requirements Mapping

| # | Requirement | Implementation |
|---|-------------|---------------|
| 1 | Historical tracking | `file_upload` table + `/uploads` endpoints |
| 2 | Point-in-time comparison | `/companies/compare?company_ids=A,B&as_of_date=...` |
| 3 | Time-series analysis | `/companies/{id}/history` |
| 4 | Version control | `version_number` in snapshots + `/companies/{id}/versions` |
| 5 | Data classification | Dimension tables for country, sector, currency |
| 6 | Time-series data | Credit metrics JSONB with year-by-year data |
| 7 | Data validation | 8-rule validation framework + quality reports |
| 8 | BI tool integration | Filtered `/snapshots` endpoint + database views |

## Pipeline Features

- **Idempotent:** Re-running won't create duplicates (file hash deduplication)
- **Incremental:** Only processes new/changed files
- **Retry:** Exponential backoff (2s, 4s, 8s) for transient failures
- **Audit Trail:** Every file tracked from ingestion to database
- **Quality Reports:** Per-file completeness, validity, and quality scores
- **Transaction Safety:** Each file processed in its own transaction

## Running Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
python -m pytest tests/ -v

# Run specific test modules
python -m pytest tests/test_extraction.py -v
python -m pytest tests/test_validation.py -v
```

## Tech Stack

- **Python 3.11** — Core language
- **FastAPI** — REST API framework
- **PostgreSQL 15** — Data warehouse
- **SQLAlchemy 2.0** — ORM with type hints
- **Pydantic v2** — Request/response validation
- **openpyxl** — Excel .xlsm parsing
- **Docker & Docker Compose** — Containerization
- **pytest** — Testing framework

## Development

### Local Development (without Docker)

```bash
# Start PostgreSQL separately
# Set environment variables (see .env.example)

# Install dependencies
pip install -r requirements.txt

# Run ETL pipeline
python run_pipeline.py --data-dir ./data

# Start API server
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Building & Stopping

```bash
# Build and start
docker-compose up -d --build

# View logs
docker-compose logs -f api

# Stop
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```
