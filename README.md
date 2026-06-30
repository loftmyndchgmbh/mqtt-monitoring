# MQTT Monitoring Solution

Monitors MQTT publisher activity and host reachability, sends combined alerts via Mailtrap (or Gmail) when issues are detected, and logs each run to a MariaDB on Cyon.

## Features
- Monitors MQTT publisher (`asenta` by default) for message activity
- Parallel ping check (3 attempts) against configured host
- Logs status hourly to MariaDB on Cyon (`mqtt_status` table)
- Sends a single combined email alert when issues occur
- Logs all activities to file and console

## Setup

1. **Clone repository**
   ```bash
   git clone https://github.com/loftmyndchgmbh/mqtt-monitoring.git
   cd mqtt-monitoring
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment (MQTT / SMTP / Ping)**
   ```bash
   cp .env.example .env
   # Edit .env with your MQTT broker, mail and ping settings
   ```

4. **Configure database (Cyon MariaDB)**
   ```bash
   cp config.ini.example config.ini
   # Edit config.ini with your Cyon MariaDB credentials
   ```

5. **Create the table on Cyon**

   Run `database_setup.sql` once against the target database:
   ```bash
   mysql -h <host> -u <user> -p <database> < database_setup.sql
   ```

6. **Test manually**
   ```bash
   python mqtt_monitor.py
   ```

7. **Schedule hourly (cron)**
   ```bash
   crontab -e
   # Add:
   0 * * * * cd /path/to/mqtt-monitoring && /usr/bin/python3 mqtt_monitor.py >> logs/mqtt_monitor.log 2>&1
   ```

## Configuration

### `.env` (MQTT / SMTP / Ping)
- **MQTT Settings**: broker credentials and publisher client ID
- **Ping Settings**:
  - `PING_HOST`: target host to ping (default: `193.5.176.14`)
  - `PING_ATTEMPTS`: parallel ping attempts (default: `3`)
  - `PING_TIMEOUT`: per-attempt timeout in seconds (default: `2`)
- **Alert Settings**:
  - `ALERT_AFTER_MINUTES`: silence threshold (default: `60`)
  - Email addresses for alerts
- **Mailtrap Settings** (or Gmail — see below)
- `MQTT_WARMUP_SECONDS`: how long to subscribe before each check (default: `10`)
- `MQTT_MONITOR_CONFIG`: path to `config.ini` (default: `./config.ini`)

### `config.ini` (Cyon MariaDB)
```ini
[REMOTE_DB]
host = your-cyon-host.cyon.ch
port = 3306
database = your-database-name
user = your-db-user
password = your-db-password
```

Pattern follows the existing `topic2cyon` setup. Restrict file permissions (`chmod 600 config.ini`) since it contains plaintext credentials.

## Email Provider: Gmail instead of Mailtrap

Yes, Gmail works. Requirements:

1. Enable 2-Step Verification on the Google account.
2. Generate an App Password: https://myaccount.google.com/apppasswords → "App: Mail", "Device: Other" → 16-character password.
3. Update `.env`:
   ```
   SMTP_SERVER=smtp.gmail.com
   SMTP_PORT=587
   SMTP_USERNAME=your@gmail.com
   SMTP_PASSWORD=xxxx xxxx xxxx xxxx
   ALERT_EMAIL_FROM=your@gmail.com
   ```

Port 587 with STARTTLS (used by the script) is correct. The normal Gmail login password does **not** work since 2022 — you must use the App Password.

## Monitoring Logic

1. Connects to MQTT broker using provided credentials
2. Subscribes to all topics under publisher client ID (e.g., `asenta/#`)
3. Runs 3 parallel pings against `PING_HOST`
4. On each run (hourly via cron):
   - Checks if silence duration exceeds threshold
   - Logs combined status to `mqtt_status` (with `ping_failed` flag)
   - Sends one email summarizing all detected issues
   - Resets alert flag when communication resumes

## Database Schema (MariaDB / Cyon)

Table `mqtt_status`:

| Column | Type | Description |
|---|---|---|
| id | INT AUTO_INCREMENT | Primary key |
| timestamp | TIMESTAMP | When status was checked (default: now) |
| publisher | VARCHAR(50) | Client ID being monitored |
| status | VARCHAR(10) | `OK` or `ERROR` |
| message_count | INT | 1 if active, 0 if silent |
| last_message | TIMESTAMP NULL | Timestamp of last received message |
| ping_failed | TINYINT(1) | 1 if all ping attempts failed, else 0 |

Indexes on `timestamp` and `publisher` for fast time-range queries.

## Alert Email Format

Single email per cycle, one section per detected issue:

```
MQTT monitoring alert for publisher 'asenta':

--- Publisher silence ---
Publisher silent for 75.3 minutes

--- Ping failure ---
Host 193.5.176.14 unreachable (3 parallel attempts all failed)
```

## Logging

- Console output and file logging to `mqtt_monitor.log`
- Log levels: INFO (status), DEBUG (messages), WARNING/ERROR (issues)

## Testing

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt pytest
.venv/bin/pytest test_mqtt_monitor.py -v
```

22 tests cover config parsing, ping (parallel/serial behavior), publisher activity thresholds, DB logging (OK/ERROR combinations + MySQL placeholder), combined email alerts (subject, body, anti-spam flag, SMTP failure), MQTT message handling, and full `run()` lifecycle including cleanup on exceptions. All external dependencies (`paho-mqtt`, `mysql.connector`, `smtplib`) are mocked — no broker, DB, or SMTP server required.