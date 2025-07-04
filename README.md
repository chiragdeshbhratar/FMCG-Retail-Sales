# FMCG Retail Sales Data Pipeline & Validation Framework

![Python](https://img.shields.io/badge/python-3.7+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

An end-to-end data pipeline for processing, validating, and reporting on FMCG (Fast-Moving Consumer Goods) retail sales data with comprehensive data quality checks and automated reporting.

## Features

- **Automated Data Simulation**: Generate realistic synthetic retail sales data with built-in quality issues for testing
- **Data Validation Framework**: Comprehensive business rule validation with 6+ quality checks
- **Database Integration**: SQLite storage with error logging and clean data loading
- **Automated Reporting**: Excel and PowerPoint report generation
- **Alerting System**: Email notifications for data quality issues
- **Performance Monitoring**: Detailed logging and validation metrics

## Technical Stack

- **Core**: Python 3.7+
- **Data Processing**: pandas, numpy
- **Database**: SQLite
- **Reporting**: openpyxl (Excel), python-pptx (PowerPoint)
- **Email**: smtplib
- **Logging**: Python logging module

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/fmcg-data-pipeline.git
   cd fmcg-data-pipeline
   ```

2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate    # Windows
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Edit `config.json` to customize:
- Database settings
- Validation thresholds
- Reporting outputs
- Email alert recipients

Example configuration:
```json
{
    "database": {
        "path": "fmcg_sales.db",
        "table_name": "sales_data"
    },
    "validation": {
        "price_change_threshold": 0.5,
        "volume_outlier_threshold": 3,
        "required_fields": ["sku", "date", "volume", "price", "region"]
    },
    "reporting": {
        "output_dir": "reports",
        "excel_file": "sales_validation_report.xlsx",
        "ppt_file": "sales_summary.pptx"
    },
    "email": {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": "your_email@company.com",
        "recipients": ["manager@company.com"]
    }
}
```

## Usage

### Running the Pipeline

```python
from fmcg_pipeline import FMCGDataPipeline

# Initialize with custom config (optional)
pipeline = FMCGDataPipeline(config_path="config.json")

# Run the complete pipeline (generates data, validates, loads to DB, creates reports)
result = pipeline.run_pipeline(num_records=10000)

# Access validation results
print(pipeline.validation_results['summary'])
```

### Key Methods

| Method | Description |
|--------|-------------|
| `simulate_sales_data()` | Generate synthetic sales data |
| `validate_data()` | Run comprehensive data quality checks |
| `create_database()` | Initialize SQLite database |
| `load_data_to_db()` | Load data to database (with clean-only option) |
| `generate_excel_report()` | Create detailed Excel validation report |
| `generate_powerpoint_summary()` | Generate executive summary PowerPoint |
| `send_alert_email()` | Send email notifications for quality issues |

## Validation Checks

The pipeline performs these data quality checks:

1. **Duplicate Records**: Identifies duplicate SKU-date-region combinations
2. **Missing Values**: Checks required fields for null values
3. **Price Outliers**: Detects extreme prices using IQR method
4. **Volume Outliers**: Flags unusual sales volumes (z-score method)
5. **Invalid Dates**: Identifies future-dated transactions
6. **Negative Values**: Checks for negative prices/volumes

## Reports Generated

1. **Excel Report** (`sales_validation_report.xlsx`):
   - Summary metrics
   - Detailed issues listing
   - Clean data sample
   - Regional analysis

2. **PowerPoint Summary** (`sales_summary.pptx`):
   - Executive summary
   - Key findings
   - Recommendations

## Email Alerts

The system automatically sends email alerts when:
- Data quality score falls below 90%
- Critical validation failures occur

*Note: Requires proper SMTP configuration in `config.json`*

## Customization

To extend the pipeline:

1. **Add New Validation Rules**:
   Modify the `validate_data()` method to include additional business rules

2. **Custom Reports**:
   Extend `generate_excel_report()` or `generate_powerpoint_summary()`

3. **Different Data Sources**:
   Replace `simulate_sales_data()` with your actual data ingestion method

## License

MIT License - See [LICENSE](LICENSE) file for details

## Support

For issues or feature requests, please open an issue on GitHub