# Variant Analytics Dashboard - Taipy Version

A comprehensive analytics dashboard built with Taipy, featuring BigQuery integration, GCS caching, and authentication.

## Features

- **Authentication System**: Simple username/password authentication
- **Multi-level Caching**: App-level → GCS → BigQuery for optimal performance
- **Dark/Light Themes**: Full theme support with toggle
- **Interactive Charts**: Plotly charts with zoom, pan, and export
- **Pivot Tables**: Data tables with sorting (CSS sticky columns for pinned effect)
- **Admin Panel**: User management interface for administrators

## Project Structure

```
variant-dashboard-taipy/
├── app/
│   ├── __init__.py
│   ├── main.py              # Main Taipy application
│   ├── bigquery_client.py   # Data layer with caching
│   ├── charts.py            # Plotly chart components
│   ├── colors.py            # Color utilities
│   └── config.py            # Configuration & constants
├── styles/
│   └── main.css             # Application styles
├── requirements.txt
├── Dockerfile
├── cloudbuild.yaml
└── README.md
```

## Key Differences from Dash Version

| Feature | Dash | Taipy |
|---------|------|-------|
| State Management | `dcc.Store` + callbacks | Automatic state binding |
| UI Definition | Python components | Markdown syntax |
| Callbacks | `@callback` decorators | Function handlers |
| Routing | Manual with Location | Built-in pages |
| Theme | CSS injection | CSS file |
| Tables | AG Grid | Taipy table + CSS sticky |

## Local Development

### Prerequisites

- Python 3.11+
- Google Cloud credentials (for BigQuery/GCS access)

### Setup

1. Clone the repository:
```bash
cd variant-dashboard-taipy
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set environment variables:
```bash
export GCS_CACHE_BUCKET="your-gcs-bucket-name"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export SECRET_KEY="your-secret-key-here"
```

5. Run the application:
```bash
# Development mode
python app/main.py

# Or using Taipy CLI
taipy run app/main.py --port 8080
```

6. Open http://localhost:8080 in your browser

### Default Credentials

- **Admin**: username `admin`, password `admin123`
- **Viewer**: username `viewer`, password `viewer123`

## Deployment to Cloud Run

### Prerequisites

1. Google Cloud project with billing enabled
2. Cloud Build API enabled
3. Cloud Run API enabled
4. Service account with permissions:
   - BigQuery Data Viewer
   - Storage Object Admin (for GCS bucket)

### Deploy via Cloud Build

1. Create GCS bucket for caching:
```bash
gsutil mb gs://variant-dashboard-cache-$PROJECT_ID
```

2. Create service account:
```bash
gcloud iam service-accounts create variant-dashboard-sa \
    --display-name="Variant Dashboard Service Account"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:variant-dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:variant-dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
```

3. Deploy:
```bash
gcloud builds submit --config=cloudbuild.yaml
```

### Manual Deploy

```bash
# Build image
docker build -t gcr.io/$PROJECT_ID/variant-dashboard-taipy .

# Push to Container Registry
docker push gcr.io/$PROJECT_ID/variant-dashboard-taipy

# Deploy to Cloud Run
gcloud run deploy variant-dashboard-taipy \
    --image gcr.io/$PROJECT_ID/variant-dashboard-taipy \
    --platform managed \
    --region us-central1 \
    --memory 4Gi \
    --cpu 2 \
    --min-instances 1 \
    --allow-unauthenticated \
    --set-env-vars "GCS_CACHE_BUCKET=variant-dashboard-cache-$PROJECT_ID"
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GCS_CACHE_BUCKET` | GCS bucket name for caching | Yes |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON | Local only |
| `SECRET_KEY` | Secret key for session encryption | Recommended |

## Architecture

### Caching Layers

1. **App-level Cache**: In-memory cache within each instance (fastest)
2. **GCS Cache**: Parquet files persisted in GCS (survives restarts)
3. **BigQuery**: Source of truth (slowest, used only when caches miss)

### State Management

Taipy manages state automatically per user session. Key state variables:
- `is_authenticated`: Login status
- `current_user`: User display name
- `active_*` / `inactive_*`: Filter states for each tab
- `chart_*`: Plotly figure objects

## Taipy-Specific Notes

### Markdown Syntax

Taipy uses a Markdown-like syntax for UI:
- `<|{variable}|input|>` - Input bound to variable
- `<|{variable}|selector|lov={options}|>` - Dropdown
- `<|{variable}|table|>` - Table from DataFrame
- `<|chart|figure={fig}|>` - Plotly chart
- `<|Button Text|button|on_action=handler|>` - Button
- `<|{condition}|render={bool_expression}|...|>` - Conditional rendering

### State Updates

Variables bound to UI elements update automatically:
```python
def on_button_click(state):
    state.some_variable = "new value"  # UI updates automatically
```

### Notifications

Use `notify()` for user feedback:
```python
notify(state, "success", "Data loaded!")
notify(state, "error", "Something went wrong")
notify(state, "info", "Processing...")
```

## Troubleshooting

### "No data available" error

1. Check BigQuery connection and table exists
2. Verify service account has BigQuery Data Viewer role
3. Check GCS bucket is configured correctly

### Session not persisting

1. Verify GCS bucket is accessible
2. Check service account has Storage Object Admin role

### Slow initial load

1. First request loads data from BigQuery → GCS cache
2. Subsequent requests use cached data
3. Consider pre-warming cache after deployment

### Pinned Columns Not Working

The Taipy table doesn't natively support pinned columns. We use CSS `position: sticky` to simulate this. If columns aren't sticking:
1. Ensure the table has the `pivot-table` class
2. Check that the table container allows horizontal scrolling

## License

Proprietary - Variant Group
