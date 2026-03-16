# Streamlit Dashboard Deployment Guide

This guide explains how to deploy the Real Estate Data Platform dashboard to Streamlit Cloud.

## Overview

The Streamlit dashboard (`src/dashboard/app.py`) is automatically deployed to Streamlit Cloud whenever changes are pushed to the `main` or `master` branch and all tests pass.

## Prerequisites

1. **GitHub Repository**: Your code must be in a GitHub repository
2. **Streamlit Cloud Account**: Sign up at [share.streamlit.io](https://share.streamlit.io/)
3. **PostgreSQL Database**: A PostgreSQL database accessible from the internet (required for the dashboard to function)

## Deployment Setup

### Step 1: Connect Your GitHub Repository

1. Go to [share.streamlit.io](https://share.streamlit.io/)
2. Click "New app"
3. Authorize Streamlit to access your GitHub account
4. Select the repository: `youngbad/real-estate-data-platform`
5. Choose the branch to deploy from (typically `main` or `master`)
6. Set the main file path: `src/dashboard/app.py`
7. Click "Deploy"

### Step 2: Configure Environment Variables

The dashboard requires the following environment variables to connect to your PostgreSQL database:

1. In your Streamlit Cloud app settings, click on "Secrets"
2. Add the following secrets in TOML format:

```toml
DB_USER = "your_database_user"
DB_PASSWORD = "your_database_password"
DB_HOST = "your_database_host"
DB_PORT = "5432"
DB_NAME = "real_estate_db"
```

**Important**: Replace the values with your actual database credentials. The database must be accessible from the internet.

### Step 3: Verify Deployment

Once configured, Streamlit Cloud will:
1. Install dependencies from `requirements.txt`
2. Install system packages from `packages.txt` (Java Runtime Environment for PySpark)
3. Apply configuration from `.streamlit/config.toml`
4. Launch your dashboard at a public URL

## Automatic Deployment Pipeline

The deployment pipeline works as follows:

```
Push to main/master → GitHub Actions CI → Tests Pass → Streamlit Cloud Deploys
```

### CI/CD Workflow

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push and pull request:

1. **Linting**: Checks code quality with Ruff
2. **Testing**: Runs pytest test suite
3. **Deployment Trigger**: If tests pass on `main`/`master`, Streamlit Cloud automatically detects the change and redeploys

**No manual deployment step is needed** - Streamlit Cloud monitors your repository and automatically deploys when it detects changes to the watched branch.

## Configuration Files

### `.streamlit/config.toml`
Configures the Streamlit app appearance and behavior:
- Theme colors (primary, background, text)
- Server settings (port, CORS, security)
- Browser settings (usage stats)

### `packages.txt`
Lists system-level dependencies to be installed via `apt-get`:
- `default-jre` - Required for PySpark operations

### `requirements.txt`
Python dependencies including:
- `streamlit==1.32.0`
- `pandas`, `altair`, `sqlalchemy`
- `pyspark==3.5.0`
- And more...

## Database Requirements

The dashboard requires a PostgreSQL database with the following schema:

**Dimension Tables:**
- `dim_date` - Date dimension
- `dim_location` - Location/city dimension
- `dim_property` - Property type dimension
- `dim_source` - Data source dimension

**Fact Tables:**
- `fact_listings` - Property listings data
- `fact_macro_indicators` - Macroeconomic indicators

See `src/database/models.py` for the complete schema definition.

## Monitoring and Logs

- **App Status**: Check your app's status at the Streamlit Cloud dashboard
- **Logs**: View real-time logs in the Streamlit Cloud interface
- **CI Status**: Monitor test results in GitHub Actions

## Troubleshooting

### App Not Deploying
- Check that all tests pass in GitHub Actions
- Verify your branch is correctly selected in Streamlit Cloud settings
- Check Streamlit Cloud logs for error messages

### Database Connection Errors
- Verify database credentials in Streamlit Secrets
- Ensure your database accepts connections from Streamlit Cloud IPs
- Check that firewall rules allow incoming connections
- Verify the database schema is properly initialized

### System Dependencies Missing
- Ensure `packages.txt` includes all required system packages
- Check Streamlit Cloud logs for `apt-get` installation errors

## Local Testing

To test the dashboard locally before deployment:

```bash
# Set environment variables
export DB_USER="your_user"
export DB_PASSWORD="your_password"
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="real_estate_db"

# Run the dashboard
streamlit run src/dashboard/app.py
```

The dashboard will be available at `http://localhost:8501`

## Additional Resources

- [Streamlit Cloud Documentation](https://docs.streamlit.io/streamlit-community-cloud)
- [Streamlit Secrets Management](https://docs.streamlit.io/streamlit-community-cloud/deploy-your-app/secrets-management)
- [Streamlit CI/CD Guide](https://docs.streamlit.io/develop/concepts/app-testing/automate-tests)

## Support

For issues related to:
- **Streamlit Cloud**: Check [Streamlit Community Forum](https://discuss.streamlit.io/)
- **CI/CD Pipeline**: Review GitHub Actions logs
- **Dashboard Code**: See `src/dashboard/app.py` and related tests
