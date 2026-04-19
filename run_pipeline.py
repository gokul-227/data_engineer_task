#!/usr/bin/env python3
"""
CLI entry point for running the ETL pipeline.

Usage:
    python run_pipeline.py [--data-dir /path/to/data] [--retries 3]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from src.etl.pipeline import run_pipeline


def setup_logging(level: str = "INFO") -> None:
    """Configure structured logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> None:
    """Run the ETL pipeline from the command line."""
    parser = argparse.ArgumentParser(
        description="Corporate Credit Rating Data Pipeline - ETL Runner",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Directory containing .xlsm files (defaults to config)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Maximum retry attempts per file (default: 3)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Corporate Credit Rating Data Pipeline")
    
    try:
        result = run_pipeline(
            data_dir=args.data_dir,
            max_retries=args.retries,
        )
        
        # Print summary report
        report = result.to_dict()
        print("\n" + "=" * 60)
        print("PIPELINE EXECUTION REPORT")
        print("=" * 60)
        print(json.dumps(report, indent=2, default=str))
        print("=" * 60)
        
        # Exit code based on status
        if result.status == "failed":
            sys.exit(1)
        elif result.status == "partial":
            sys.exit(2)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.exception("Pipeline failed with unexpected error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
