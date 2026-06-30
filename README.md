# MQTT Monitoring Solution

Monitors MQTT publisher activity and host reachability, sends combined alerts via Mailtrap (or Gmail) when issues are detected, and logs each run to a MariaDB on Cyon.

## Features
- Monitors MQTT publisher (`asenta` by default) for message activity
- Parallel ping check (3 attempts) against configured host
- Logs status hourly to MariaDB on Cyon (`mqtt_status` table)
- Sends a single combined email alert with To/Cc/Bcc support
- All configuration in a single `config.ini` file

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

3. **Configure**
   ```bash
   cp config.ini.example config.ini
   chmod 600 config.ini
   # Edit config.ini with your credentials
   ```

4. **Create the table on Cyon**
   ```bash
   mysql -h <host> -u <user> -p <database> < database_setup.sql
   ```

5. **Test manually**
   ```bash
   python mqtt_monitor.py
   ```

6. **Schedule hourly (cron)**
   ```bash
   crontab -e
   # Add:
   0 * * * * cd /path/to/mqtt-monitoring && /usr/bin/python3 mqtt_monitor.py >> logs/mqtt_monitor.log 2>&1
   ```

## Configuration (`config.ini`)

All settings live in `config.ini`, organized into five sections. Any value can be overridden by an environment variable named `<SECTION>_<KEY>` (e.g. `MQTT_BROKER`, `ALERT_AFTER_MINUTES`, `SMTP_PASSWORD`).

### `[MQTT]`
- `broker`: broker hostname
- `port`: broker port (default `1883`)
- `username`, `password`: optional credentials
- `publisher_client_id`: client ID to monitor (default `asenta`)
- `warmup_seconds`: how long to subscribe before checking (default `10`)

### `[PING]`
- `host`: target host (default `193.5.176.14`)
- `attempts`: parallel ping attempts (default `3`)
- `timeout`: per-attempt timeout in seconds (default `2`)

### `[ALERT]`
- `after_minutes`: silence threshold (default `60`)
- `email_from`: sender address
- `email_to`: comma-separated list of primary recipients
- `email_cc`, `email_bcc`: optional comma-separated lists

### `[SMTP]`
- `server`, `port`: SMTP host (default port `587`)
- `username`, `password`: SMTP credentials
- `tls`: `auto` (default), `ssl`, `starttls`, or `none`
  - `auto`: port 465 → SSL, otherwise STARTTLS (gracefully skipped if server rejects it)

### `[REMOTE_DB]`
- `host`, `port`, `database`, `user`, `password`: Cyon MariaDB connection

### Override path
- `MQTT_MONITOR_CONFIG=/path/to/config.ini` — use a different config file

## Email Provider: Gmail instead of Mailtrap

Gmail works with an App Password:

1. Enable 2-Step Verification on the Google account.
2. Generate an App Password: https://myaccount.google.com/apppasswords → "App: Mail", "Device: Other".
3. Update `[SMTP]` in `config.ini`:
   ```ini
   server = smtp.gmail.com
   port = 587
   username = your@gmail.com
   password = xxxx xxxx xxxx xxxx
   ```
4. Update `[ALERT]`:
   ```ini
   email_from = your@gmail.com
   ```

Port 587 with STARTTLS is correct. The normal Gmail login password does **not** work since 2022 — use the App Password.

## Monitoring Logic

1. Connects to MQTT broker using `[MQTT]` credentials
2. Subscribes to all topics under `publisher_client_id/#`
3. Runs N parallel pings against `[PING] host`
4. On each run (hourly via cron):
   - Checks if silence duration exceeds `[ALERT] after_minutes`
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

35 tests cover config parsing (incl. ENV override), ping (parallel/serial behavior), publisher activity thresholds, DB logging (OK/ERROR combinations + MySQL placeholder), recipient parsing (comma lists, blanks), SMTP TLS auto-detection per port and explicit modes (incl. graceful STARTTLS-not-supported fallback), combined email alerts (`EmailMessage` API, subject, body, To/Cc/Bcc, anti-spam flag, SMTP failure, missing recipients), MQTT message handling, and full `run()` lifecycle including cleanup on exceptions. All external dependencies (`paho-mqtt`, `mysql.connector`, `smtplib`) are mocked — no broker, DB, or SMTP server required.