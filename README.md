# YouTube Channel Analytics Pipeline
 
An end-to-end data engineering project that extracts YouTube channel data via the YouTube Data API v3, transforms it through a **Medallion Architecture** (Bronze → Silver → Gold), and stores it in PostgreSQL for analytics.
 
---
 
## Table of Contents
 
- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Data Model](#data-model)
- [Load Modes](#load-modes)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
- [Airflow DAG](#airflow-dag)
- [Database Setup](#database-setup)
- [Git Workflow](#git-workflow)
---
 
## Architecture Overview
 
```
YouTube Data API v3
        │
        ▼
┌───────────────┐
│    BRONZE     │  Raw JSON/JSONL files per channel/date/run
│  (File Store) │  Watermark-based incremental tracking
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    SILVER     │  Data Vault 2.0 — Hubs, Satellites, Links
│  (Parquet)    │  SCD Type 2 with business-date valid_from/valid_to
└───────┬───────┘
        │
        ▼
┌───────────────┐
│     GOLD      │  Two OBT tables in PostgreSQL
│  (PostgreSQL) │  youtube_statistics_obt (SCD2 incremental)
└───────────────┘  youtube_num_statistics_obt (partitioned snapshot)
```
 
**Silver — Data Vault 2.0** stores three entity types separately:
- **Channels** — hub + satellite (metadata, statistics)
- **Videos** — hub + satellite + link to channel
- **Comments** — hub + satellite + link to video
**Gold — OBT (One Big Table)** denormalizes all three entities into two tables:
- `youtube_statistics_obt` — textual/descriptive attributes, SCD2 (tracks changes over time)
- `youtube_num_statistics_obt` — numeric statistics (views, likes, subscribers), partitioned by `date_of_data`
---
 
## Project Structure
 
```
youtube-channel-analytics/
├── config/
│   └── .env                          # API keys and DB credentials (not committed, gitignored, YOU HAVE TO CREATE)
├── dags/
│   ├── load_youtube_data.py          # Airflow DAG definition
│   └── watermark.py                  # Watermark tracking & run management
├── src/
│   ├── bronze/
│   │   ├── extract.py                # Entry point: process_channel()
│   │   └── extract_api.py            # YoutubeData class — API extraction logic
│   ├── silver/
│   │   ├── transform.py              # Orchestrates Silver load for all entities
│   │   ├── channel.py                # Channel Hub + Satellite SCD2
│   │   ├── video.py                  # Video Hub + Satellite + Link SCD2
│   │   └── comment.py                # Comment Hub + Satellite + Link SCD2 (Polars)
│   ├── gold/
│   │   ├── load.py                   # Gold entry point
│   │   └── load_statistics.py        # OBT load logic (DuckDB → PostgreSQL)
│   └── sql/
│       ├── youtube_gold_db.sql       # DDL: tables, indexes, partitions, manage_partition()
│       ├── tests_of_data.sql         # Ad-hoc data quality queries
│       └── tests_of_data2.sql        # Additional validation queries
├── data/                             # Auto-created at runtime (gitignored)
│   ├── bronze/                       # Raw JSONL files
│   └── silver/                       # Parquet files (hubs/satellites/links)
├── logs/                     # Required by Airflow
├── plugins/                  # Required by Airflow
├── docker-compose.yml        # (optional but recommended)
├── requirements.txt
└── README.md
```
 
---
 
## Tech Stack
 
| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10 (WSL on Windows 10) |
| Extraction | YouTube Data API v3 (`google-api-python-client`) |
| Bronze storage | JSONL files (local filesystem) |
| Silver storage | Parquet files (PyArrow / FastParquet) |
| Silver processing | Pandas 3.0, Polars 1.39 |
| Gold processing | DuckDB 1.5 (in-memory, reads Parquet directly) |
| Gold storage | PostgreSQL (via DuckDB `postgres` extension) |
| Watermark tracking | Custom JSONL-based watermark system (`dags/watermark.py`) |
| Environment | Python 3.11, WSL2 (Ubuntu) on Windows 10 |
 
---
 
## Data Model
 
### Silver — Data Vault 2.0
 
```
HUB_CHANNEL        SAT_CHANNEL
hk_channel ────────► hk_channel
channel_id          title, custom_url, description
load_date           view_count, subscriber_count, video_count
record_source       valid_from, valid_to, is_current, hash_diff
                    load_date
 
HUB_VIDEO          SAT_VIDEO                    LINK_CHANNEL_VIDEO
hk_video ──────────► hk_video                   hk_channel
video_id            title, description           hk_video
load_date           view_count, like_count       hk_link_channel_video
                    duration, category_id        load_date
                    valid_from, valid_to
                    is_current, hash_diff
 
HUB_COMMENT        SAT_COMMENT                  LINK_VIDEO_COMMENT
hk_comment ────────► hk_comment                 hk_video
comment_id          comment_text, author         hk_comment
load_date           like_count, reply_count      hk_link_comment_video
                    publish_date, update_date    load_date
                    valid_from, valid_to
                    is_current, hash_diff
```
 
`valid_from` and `valid_to` are **business dates** (the `date_of_data` parameter), not technical load timestamps. This enables time-travel queries:
 
```sql
WHERE :date_of_data BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')
```
 
### Gold — OBT Tables
 
**`youtube_statistics_obt`** — SCD2, tracks changes across loads:
 
| Column | Type | Description |
|---|---|---|
| `valid_from` | date | Business date from which this version applies |
| `valid_to` | date | Business date until which this version applies (NULL = current) |
| `is_current` | boolean | True for the latest active version |
| `hk_channel/video/comment` | varchar(32) | MD5 hash keys |
| `channel_name`, `video_title`, `comment_text` | varchar | Descriptive attributes |
| `hash_diff` | varchar(32) | MD5 of key attributes — used for change detection |
 
**`youtube_num_statistics_obt`** — partitioned by `date_of_data`, snapshot per date:
 
| Column | Type | Description |
|---|---|---|
| `date_of_data` | date | Partition key — business date of the snapshot |
| `channel_view_count`, `channel_subscrb_count` | int | Channel statistics |
| `video_view_count`, `video_like_count`, `video_comment_count` | int | Video statistics |
| `comment_like_count`, `comment_reply_count` | int | Comment statistics |
 
---
 
## Load Modes
 
The pipeline supports four load modes, selectable in the Airflow UI when triggering manually:
 
| Mode | When to use | Behavior |
|---|---|---|
| `incremental` | Daily scheduled runs | Detects new/changed records via `hash_diff`, updates SCD2 accordingly |
| `full` | First-ever load | Clears all data, loads everything from scratch |
| `reload` | Re-processing after pipeline error | Same as full — clears and reloads |
| `reload_date` | Correcting data for a specific past date | Surgically replaces data for one `date_of_data` while preserving surrounding history |
 
### `reload_date` internals (Gold)
 
Gold executes three steps in a transaction:
 
1. **Build reference snapshot** — find the single record version that was active on `date_of_data` (the "point of comparison")
2. **UPDATE** — close the reference record if its `hash_diff` differs from the new Silver data
3. **DELETE** — remove all records where `valid_from = date_of_data`
4. **INSERT** — re-insert from Silver, computing `valid_to` by looking at the nearest future date already in Gold
This ensures surrounding history (dates before and after) is never touched.
 
---
 
## Prerequisites
 
- Windows 10 with **WSL2** (Ubuntu recommended)
- **Python 3.11** installed in WSL virtual environment
- **PostgreSQL** running on Windows (accessible from WSL)
- **YouTube Data API v3** key ([Google Cloud Console](https://console.cloud.google.com/))
- **Apache Airflow 2.10** installed in WSL venv or **Apache airflow Docker Image**
---
 
## Setup
 
### 1. Clone the repository
 
```bash
git clone https://github.com/your-username/youtube-channel-analytics.git
cd youtube-channel-analytics
```
 
### 2. Create and activate virtual environment (WSL)
 
```bash
python3 -m venv ~/airflow_env/venv
source ~/airflow_env/venv/bin/activate
pip install -r requirements.txt
```
 
### 3. Configure environment variables
 
Create `config/.env`:
 
```env
# YouTube API
YT_API_KEY=your_youtube_api_key_here
 
# PostgreSQL — connection from WSL
PG_HOST_FOR_WSL=172.x.x.x        # IP of Windows host visible from WSL
PG_HOST_FOR_WINDOWS=localhost
PG_USER=YOUR_USERNAME
PG_PSWD=YOUR_PASSWORD
PG_PORT=5432
PG_DB=your_database_name
```
 
To find your Windows IP from WSL:
 
```bash
cat /etc/resolv.conf | grep nameserver | awk '{print $2}'
```
 
### 4. Set up the PostgreSQL database
 
Run the DDL script in your PostgreSQL client (pgAdmin or psql):
 
```sql
-- Execute the full contents of:
src/sql/youtube_gold_db.sql
```
 
This creates: tables, indexes, partition function `manage_partition()`, user `youtube`, and role `yt_role`.
 
### 5. Install the project as an editable package
 
```bash
# In the project root (WSL, venv activated)
pip install -e .
```
 
Or add to `PYTHONPATH` permanently in `~/.bashrc`:
 
```bash
echo 'export PYTHONPATH=/mnt/d/PythonProjects/youtube-channel-analytics:$PYTHONPATH' >> ~/.bashrc
source ~/.bashrc
```
 
---

## Airflow Setup

### Recommened: Docker Compose

```bash
# 1. Create required directories
mkdir -p logs plugins config

# 2. create file docker-compose 
# 3. Bashdocker compose up -d

### Alternative way: Directly on WSL (venv) like in this project
 
## Running the Pipeline
 
### Start Airflow (Windows CMD → WSL)
 
Open two CMD windows, in each run:
 
```cmd
wsl
```
 
Then in WSL (both windows):
 
```bash
source ~/airflow_env/venv/bin/activate
export AIRFLOW_HOME=/home/<your_user>/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/mnt/d/PythonProjects/youtube-channel-analytics/dags
export PYTHONPATH=/mnt/d/PythonProjects/youtube-channel-analytics
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:airflow@<windows_ip>:5432/airflow_db
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```
 
**Window 1 — Webserver:**
 
```bash
# Remove stale PID file if Airflow was previously killed
rm -f $AIRFLOW_HOME/airflow-webserver.pid
airflow webserver -p 8081
```
 
**Window 2 — Scheduler:**
 
```bash
airflow scheduler
```
 
Airflow UI will be available at: `http://localhost:8081`
 
### Initialize Airflow (first time only)
 
```bash
airflow db init
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@example.com
```
 
### Trigger a manual run
 
In the Airflow UI:
1. Find DAG `load_youtube_data`
2. Click **Trigger DAG w/ config**
3. Set parameters:
   - `channels`: `["@nieinformatyk", "@DataWithBaraa"]` (or any YouTube handles)
   - `mode`: `full` / `incremental` / `reload` / `reload_date`
   - `start_ETL_from`: `bronze` / `silver` / `gold` (skip earlier layers if already processed)
   - `date_of_data` - logical date
---
 
## Airflow DAG
 
The DAG `load_youtube_data` runs daily at 09:00 Warsaw time and consists of four tasks:
 
```
prepare_metadata
      │
      ├──► prepare_extraction_args
      │           │
      │           ▼
      │    extract_single_channel   ← dynamic task mapping (one task per channel)
      │
      ▼
   transform   ← Silver load (channels, videos, comments)
      │
      ▼
    load        ← Gold load (OBT tables)
```
 
**Key DAG parameters:**
 
| Parameter | Default | Options |
|---|---|---|
| `channels` | `["@nieinformatyk", "@DataWithBaraa"]` | Any YouTube channel handles |
| `mode` | `incremental` | `full`, `reload`, `incremental`, `reload_date` |
| `start_ETL_from` | `bronze` | `bronze`, `silver`, `gold` |
 
**Email notifications** are sent on success and failure (configure recipient in `load_youtube_data.py`).
 
---
 
## Database Setup
 
The `manage_partition()` PostgreSQL function handles partition lifecycle automatically:
 
```sql
-- Called internally by the pipeline for each date_of_data
SELECT public.manage_partition('youtube_num_statistics_obt', '2026-07-16', 'incremental');
```
 
Behavior by mode:
- `full` / `reload` → drops all existing partitions, creates new one
- `incremental` / `reload_date` → creates partition if missing, truncates if exists
---
 
## Git Workflow
 
### Initial setup
 
```bash
# Configure your identity (once)
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
 
# Initialize repository (if not already done)
git init
git remote add origin https://github.com/your-username/youtube-channel-analytics.git
```
 
### .gitignore — create before first commit
 
```bash
cat > .gitignore << 'EOF'
# Environment & secrets
config/.env
*.env
 
# Data files (potentially large)
data/
 
# Python
__pycache__/
*.py[cod]
*.egg-info/
venv/
.venv/
 
# IDE
.vscode/
.idea/
 
# Airflow
logs/
airflow-webserver.pid
EOF
```
 
### Daily feature branch workflow
 
```bash
# 1. Make sure main is up to date
git checkout main
git pull origin main
 
# 2. Create a new branch for your changes
git checkout -b feature/your-feature-name
 
# 3. Make changes, then stage them
git add .
# or selectively:
git add src/silver/comment.py dags/watermark.py
 
# 4. Commit with a descriptive message
git commit -m "fix: correct reload_date SCD2 reference record logic in comment silver"
 
# 5. Push branch to remote
git push origin feature/your-feature-name
 
# 6. Open a Pull Request on GitHub (UI or CLI):
gh pr create --title "Fix reload_date SCD2" --body "Fixes reference record selection and valid_to calculation"
 
# 7. After review/merge, clean up locally
git checkout main
git pull origin main
git branch -d feature/your-feature-name
```
 
### Useful commit message prefixes
 
| Prefix | When to use |
|---|---|
| `feat:` | New functionality |
| `fix:` | Bug fix |
| `refactor:` | Code restructure without behavior change |
| `docs:` | Documentation changes |
| `chore:` | Dependency updates, config changes |
 
### Example branch names for this project
 
```bash
git checkout -b fix/reload-date-gold-insert

Druga wersja projektu znajduje się w repozytorium: [Youtube-medallion-architecture-project-v2](https://github.com/KacprusJeden/Youtube-medallion-architecture-project-v2)
git checkout -b feat/silver-video-scd2-reload
git checkout -b refactor/watermark-pending-runs
git checkout -b docs/readme-setup-instructions
```
