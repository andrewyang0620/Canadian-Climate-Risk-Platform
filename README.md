<div align="center">

# Canadian Climate & Property Risk Data Platform

[![Status: Ongoing](https://img.shields.io/badge/Status-Ongoing-0078D4?style=for-the-badge)](#)

[![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)](#)
[![Apache Spark](https://img.shields.io/badge/apache%20spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](#)
[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](#)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](#)
[![Power BI](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](#)
[![Terraform](https://img.shields.io/badge/terraform-%235835CC.svg?style=for-the-badge&logo=terraform&logoColor=white)](#)

> 🚧 **STATUS: ONGOING** 🚧
> 
> *The `main` branch is currently being initialized. Infrastructure provisioning and core pipeline development are actively running in feature branches. The initial production release will be merged here soon.*

</div>

---

## Project At A Glance

- **Live Demo:** `[Public Power BI Dashboard — Link Coming Soon]`
- **Architecture:** Azure VM + PySpark + Delta Lake + Apache Airflow + dbt Core + PostgreSQL
- **Scope:** British Columbia & Alberta, covering a 10-year historical baseline (2016–2025)
- **Validation:** Out-of-time backtesting against the Canadian Disaster Database (CDD)
- **Data Quality:** Automated checks for source freshness, schema hashing, row count anomalies, and geospatial coordinate validation
- **Deployment:** Infrastructure as Code via Terraform, CI/CD via GitHub Actions, and containerized local/VM execution via Docker Compose

---

## Project Overview

This project is a Spark-based Azure data platform designed to integrate Canadian climate, hydrometric, wildfire, building-permit, and disaster-event data. The pipeline transforms raw public data into validated, municipality-level climate-exposure priority marts. 

The final output is an interactive Power BI dashboard that prioritizes municipalities for monitoring based on historical hazard signals and development exposure, avoiding subjective "risk weight" assumptions by validating against actual historical disaster events.

## Current Development Roadmap

We are currently laying the groundwork before the first major merge to `main`. Here is what is being built right now:

- [x] **Project Scoping & Architecture Design:** Finalizing tech stack and data models.
- [x] **Repository Initialization:** Setting up `.gitignore`, formatting rules, and branch protections.
- [ ] **Infrastructure Provisioning:** Writing Terraform scripts for ADLS Gen2, Azure VM, and PostgreSQL Flexible Server.
- [ ] **Ingestion Layer:** Developing Airflow DAGs for ECCC Climate, HYDAT, and NRCan Wildfire data.
- [ ] **Data Quality Framework:** Implementing Layer 1 (Source/Ingestion) audits.
- [ ] **First PR to Main:** Merging the base Airflow/Spark Docker Compose environment.

---

## Tech Stack Highlights

| Layer | Technology |
|---|---|
| **Cloud & Storage** | Azure VM, ADLS Gen2, Delta Lake |
| **Processing & Geospatial** | PySpark, Apache Sedona |
| **Orchestration** | Apache Airflow |
| **Transformation & Serving** | dbt Core, Azure PostgreSQL |
| **CI/CD & IaC** | GitHub Actions, Terraform, Docker |

---

<div align="center">
  <p><i>Check back soon. First pipelines are scheduled to land in the coming weeks.</i></p>
</div>