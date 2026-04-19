# Sample API Responses

Below are example API calls and their expected responses. These examples demonstrate
the full range of API functionality after the ETL pipeline has processed the 4 test files.

---

## 1. Root Endpoint

**Request:** `GET /`

```json
{
  "name": "Corporate Credit Rating Data Pipeline API",
  "version": "1.0.0",
  "documentation": "/docs",
  "redoc": "/redoc",
  "health": "/health",
  "api_base": "/api/v1"
}
```

---

## 2. Health Check

**Request:** `GET /health`

```json
{
  "status": "healthy",
  "database": "connected",
  "api_version": "1.0.0",
  "timestamp": "2026-04-18T17:45:00.000000"
}
```

---

## 3. List All Companies

**Request:** `GET /api/v1/companies`

```json
[
  {
    "company_id": "company_A",
    "company_name": "Company A",
    "sector": "Personal & Household Goods",
    "country": "Federal Republic of Germany",
    "currency": "EUR",
    "latest_version": 2,
    "total_versions": 2,
    "latest_snapshot_date": "2026-04-18T17:45:00.000000"
  },
  {
    "company_id": "company_B",
    "company_name": "Company B",
    "sector": "Automobiles & Parts",
    "country": "Swiss Confederation",
    "currency": "CHF",
    "latest_version": 2,
    "total_versions": 2,
    "latest_snapshot_date": "2026-04-18T17:45:01.000000"
  }
]
```

---

## 4. Get Company Detail (Latest Version)

**Request:** `GET /api/v1/companies/company_A`

```json
{
  "company_id": "company_A",
  "company_name": "Company A",
  "sector": "Personal & Household Goods",
  "country": "Federal Republic of Germany",
  "currency": "EUR",
  "version_number": 2,
  "rating_methodologies": [
    "General Corporate Rating Methodology"
  ],
  "industry_risks": [
    {
      "industry": "Consumer Products: Non-Discretionary",
      "score": "BBB",
      "weight": 1.0
    }
  ],
  "segmentation_criteria": "EBITDA contribution",
  "accounting_principles": "IFRS",
  "year_end_month": "December",
  "business_risk_profile": "B",
  "blended_industry_risk_profile": "A",
  "competitive_positioning": "B+",
  "market_share": "B+",
  "diversification": "B+",
  "operating_profitability": "B",
  "sector_specific_factor_1": "B-",
  "sector_specific_factor_2": null,
  "financial_risk_profile": "CC",
  "leverage": "CCC",
  "interest_cover": "B-",
  "cash_flow_cover": "CCC",
  "liquidity": "-2 notches",
  "credit_metrics": {
    "Scope-adjusted EBITDA interest cover": {
      "2018": 27.329, "2019": 27.329, "2020": 27.329,
      "2021": 4.862, "2022": 4.862, "2023": 4.862,
      "2024": 36.8, "2025E": 36.8, "2026E": 18.491, "2027E": 18.491
    },
    "Scope-adjusted debt/EBITDA": { "...": "..." }
  },
  "quality_score": 0.9,
  "snapshot_date": "2026-04-18T17:45:00.000000",
  "source_file": "corporates_A_2.xlsm"
}
```

---

## 5. Get Company Versions (Version Control)

**Request:** `GET /api/v1/companies/company_A/versions`

```json
[
  {
    "version_number": 2,
    "snapshot_id": 2,
    "snapshot_date": "2026-04-18T17:45:01.000000",
    "source_file": "corporates_A_2.xlsm",
    "quality_score": 0.9,
    "business_risk_profile": "B",
    "financial_risk_profile": "CC",
    "industry_risks": [
      {"industry": "Consumer Products: Non-Discretionary", "score": "BBB", "weight": 1.0}
    ]
  },
  {
    "version_number": 1,
    "snapshot_id": 1,
    "snapshot_date": "2026-04-18T17:45:00.000000",
    "source_file": "corporates_A_1.xlsm",
    "quality_score": 0.9,
    "business_risk_profile": "B+",
    "financial_risk_profile": "C",
    "industry_risks": [
      {"industry": "Consumer Products: Non-Discretionary", "score": "A", "weight": 1.0}
    ]
  }
]
```

---

## 6. Get Company History (Time-Series)

**Request:** `GET /api/v1/companies/company_B/history`

```json
[
  {
    "version_number": 1,
    "snapshot_date": "2026-04-18T17:45:00.000000",
    "business_risk_profile": "BBB",
    "financial_risk_profile": "BB+",
    "blended_industry_risk_profile": "A+",
    "leverage": "BB+",
    "interest_cover": "A-",
    "cash_flow_cover": "A-",
    "liquidity": "+1 notch",
    "industry_risks": [
      {"industry": "Automotive Suppliers", "score": "BBB", "weight": 0.15},
      {"industry": "Automotive and Commercial Vehicle Manufacturers", "score": "BB", "weight": 0.85}
    ],
    "source_file": "corporates_B_1.xlsm"
  },
  {
    "version_number": 2,
    "snapshot_date": "2026-04-18T17:45:01.000000",
    "business_risk_profile": "BBB-",
    "financial_risk_profile": "BB",
    "blended_industry_risk_profile": "A",
    "leverage": "BB+",
    "interest_cover": "BBB+",
    "cash_flow_cover": "A-",
    "liquidity": "+1 notch",
    "industry_risks": [
      {"industry": "Automotive Suppliers", "score": "BBB", "weight": 0.25},
      {"industry": "Automotive and Commercial Vehicle Manufacturers", "score": "BB", "weight": 0.75}
    ],
    "source_file": "corporates_B_2.xlsm"
  }
]
```

---

## 7. Compare Companies at Point-in-Time

**Request:** `GET /api/v1/companies/compare?company_ids=company_A,company_B`

```json
[
  {
    "company_id": "company_A",
    "company_name": "Company A",
    "version_number": 2,
    "snapshot_date": "2026-04-18T17:45:01.000000",
    "sector": "Personal & Household Goods",
    "country": "Federal Republic of Germany",
    "currency": "EUR",
    "business_risk_profile": "B",
    "financial_risk_profile": "CC",
    "blended_industry_risk_profile": "A",
    "leverage": "CCC",
    "industry_risks": [
      {"industry": "Consumer Products: Non-Discretionary", "score": "BBB", "weight": 1.0}
    ]
  },
  {
    "company_id": "company_B",
    "company_name": "Company B",
    "version_number": 2,
    "snapshot_date": "2026-04-18T17:45:01.000000",
    "sector": "Automobiles & Parts",
    "country": "Swiss Confederation",
    "currency": "CHF",
    "business_risk_profile": "BBB-",
    "financial_risk_profile": "BB",
    "blended_industry_risk_profile": "A",
    "leverage": "BB+",
    "industry_risks": [
      {"industry": "Automotive Suppliers", "score": "BBB", "weight": 0.25},
      {"industry": "Automotive and Commercial Vehicle Manufacturers", "score": "BB", "weight": 0.75}
    ]
  }
]
```

---

## 8. List Snapshots with Filters

**Request:** `GET /api/v1/snapshots?currency=CHF`

```json
[
  {
    "snapshot_id": 3,
    "company_id": "company_B",
    "company_name": "Company B",
    "version_number": 2,
    "sector": "Automobiles & Parts",
    "country": "Swiss Confederation",
    "currency": "CHF",
    "business_risk_profile": "BBB-",
    "financial_risk_profile": "BB",
    "quality_score": 0.9,
    "snapshot_date": "2026-04-18T17:45:01.000000",
    "source_file": "corporates_B_2.xlsm"
  },
  {
    "snapshot_id": 4,
    "company_id": "company_B",
    "company_name": "Company B",
    "version_number": 1,
    "sector": "Automobiles & Parts",
    "country": "Swiss Confederation",
    "currency": "CHF",
    "business_risk_profile": "BBB",
    "financial_risk_profile": "BB+",
    "quality_score": 0.9,
    "snapshot_date": "2026-04-18T17:45:00.000000",
    "source_file": "corporates_B_1.xlsm"
  }
]
```

---

## 9. Latest Snapshots (One Per Company)

**Request:** `GET /api/v1/snapshots/latest`

```json
[
  {
    "snapshot_id": 2,
    "company_id": "company_A",
    "company_name": "Company A",
    "version_number": 2,
    "sector": "Personal & Household Goods",
    "country": "Federal Republic of Germany",
    "currency": "EUR",
    "business_risk_profile": "B",
    "financial_risk_profile": "CC",
    "snapshot_date": "2026-04-18T17:45:01.000000",
    "source_file": "corporates_A_2.xlsm"
  },
  {
    "snapshot_id": 4,
    "company_id": "company_B",
    "company_name": "Company B",
    "version_number": 2,
    "sector": "Automobiles & Parts",
    "country": "Swiss Confederation",
    "currency": "CHF",
    "business_risk_profile": "BBB-",
    "financial_risk_profile": "BB",
    "snapshot_date": "2026-04-18T17:45:01.000000",
    "source_file": "corporates_B_2.xlsm"
  }
]
```

---

## 10. List All Uploads (Audit Trail)

**Request:** `GET /api/v1/uploads`

```json
[
  {
    "upload_id": 4,
    "filename": "corporates_B_2.xlsm",
    "file_hash": "e3b0c44298fc1c149afbf4c8996fb924...",
    "file_size_bytes": 146877,
    "upload_timestamp": "2026-04-18T17:45:03.000000",
    "processing_status": "completed",
    "records_extracted": 1,
    "processing_duration_ms": 156
  },
  {
    "upload_id": 3,
    "filename": "corporates_B_1.xlsm",
    "file_hash": "a1b2c3d4e5f6789012345678abcdef...",
    "file_size_bytes": 146884,
    "upload_timestamp": "2026-04-18T17:45:02.000000",
    "processing_status": "completed",
    "records_extracted": 1,
    "processing_duration_ms": 142
  },
  {
    "upload_id": 2,
    "filename": "corporates_A_2.xlsm",
    "file_hash": "1234567890abcdef1234567890abcdef...",
    "file_size_bytes": 146973,
    "upload_timestamp": "2026-04-18T17:45:01.000000",
    "processing_status": "completed",
    "records_extracted": 1,
    "processing_duration_ms": 138
  },
  {
    "upload_id": 1,
    "filename": "corporates_A_1.xlsm",
    "file_hash": "fedcba0987654321fedcba0987654321...",
    "file_size_bytes": 146963,
    "upload_timestamp": "2026-04-18T17:45:00.000000",
    "processing_status": "completed",
    "records_extracted": 1,
    "processing_duration_ms": 145
  }
]
```

---

## 11. Upload Statistics

**Request:** `GET /api/v1/uploads/stats`

```json
{
  "total_uploads": 4,
  "completed": 4,
  "failed": 0,
  "skipped": 0,
  "pending": 0,
  "total_records_extracted": 4,
  "avg_processing_duration_ms": 145.25,
  "latest_upload": "2026-04-18T17:45:03.000000"
}
```

---

## 12. Upload Details (Data Lineage)

**Request:** `GET /api/v1/uploads/1/details`

```json
{
  "upload_id": 1,
  "filename": "corporates_A_1.xlsm",
  "file_path": "/app/data/corporates_A_1.xlsm",
  "file_hash": "fedcba0987654321fedcba0987654321...",
  "file_size_bytes": 146963,
  "upload_timestamp": "2026-04-18T17:45:00.000000",
  "processing_status": "completed",
  "records_extracted": 1,
  "processing_duration_ms": 145,
  "error_message": null,
  "snapshot_id": 1,
  "company_name": "Company A",
  "quality_score": 0.9
}
```
