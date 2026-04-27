@'
# Local Development Setup

## Environment

This project uses a hybrid local development setup:

- Python 3.11 conda environment for lightweight scripts, validation, and tests
- Docker Compose for PostgreSQL, Spark, and Airflow
- Azure CLI and Terraform for later cloud deployment

## Required Software

- Git
- Python 3.11
- Java 17
- Docker Desktop
- Azure CLI
- Terraform
- VS Code

## Python Environment

```powershell
conda activate climate-risk
python --version
pip install -r requirements-dev.txt