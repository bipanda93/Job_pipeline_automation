# Job Pipeline Automation

Pipeline automatise de scraping et candidature emploi.

## Stack technique
- Python 3.12
- Apache Airflow
- PostgreSQL 17
- Docker + Docker Compose
- Claude API (Anthropic)
- AWS (ECS, S3, RDS, Lambda, SES)
- Terraform
- GitHub Actions

## Scrapers
- Indeed (Selenium + undetected-chromedriver)
- LinkedIn (Playwright)
- Welcome to the Jungle (Playwright)
- HelloWork (Playwright)
- France Travail (Playwright)
- Carrefour (Playwright)

## Architecture
Scraper -> PostgreSQL -> Matching Claude API -> Generation lettre -> Envoi AWS SES -> Dashboard Streamlit

## Installation
\`\`\`bash
git clone https://github.com/bipanda93/Job_pipeline_automation.git
cd Job_pipeline_automation
docker-compose up -d
\`\`\`

## Structure
\`\`\`
job-pipeline-automation/
├── dags/          # DAGs Airflow
├── scrapers/      # Scripts scrapers
├── tests/         # Tests pytest
├── docs/          # Documentation
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
\`\`\`

## Auteur
Bipanda Franck Ulrich - Data Engineer oriente Analytics
