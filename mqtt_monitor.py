import os
import sys
import configparser
import time as _time
import smtplib
import logging
import subprocess
import threading
import fcntl
from datetime import datetime, timedelta
from email.message import EmailMessage
from paho.mqtt import client as mqtt
import mysql.connector

CONFIG_PATH_DEFAULT = 'config.ini'
LOCK_FILE_PATH = '/tmp/mqtt_monitor.lock'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_monitor.log'),
        logging.StreamHandler()
    ]
)


def _get(parser, section, key, default=None, cast=None):
    """Read a key from ENV first, then INI, falling back to default.
    Empty INI values fall back to default so the template can ship blanks."""
    env_key = f"{section.upper()}_{key.upper()}"
    value = os.getenv(env_key)
    if value is None:
        if parser.has_option(section, key):
            raw = parser.get(section, key)
            value = raw if raw != '' else default
        else:
            value = default
    if value is None:
        return None
    if cast:
        return cast(value)
    return value


def load_config(path=None):
    path = path or os.getenv('MQTT_MONITOR_CONFIG', CONFIG_PATH_DEFAULT)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Config file not found: {path}. Copy config.ini.example to {path}."
        )
    parser = configparser.RawConfigParser()
    parser.read(path)

    required = ['MQTT', 'PING', 'ALERT', 'SMTP', 'REMOTE_DB']
    missing = [s for s in required if not parser.has_section(s)]
    if missing:
        raise ValueError(f"Missing sections in {path}: {', '.join(missing)}")

    return {
        'mqtt': {
            'broker': _get(parser, 'MQTT', 'broker'),
            'port': _get(parser, 'MQTT', 'port', '1883', int),
            'username': _get(parser, 'MQTT', 'username'),
            'password': _get(parser, 'MQTT', 'password'),
            'publisher_client_id': _get(parser, 'MQTT', 'publisher_client_id', 'asenta'),
            'warmup_seconds': _get(parser, 'MQTT', 'warmup_seconds', '10', int),
        },
        'ping': {
            'host': _get(parser, 'PING', 'host', '193.5.176.14'),
            'attempts': _get(parser, 'PING', 'attempts', '3', int),
            'timeout': _get(parser, 'PING', 'timeout', '2', int),
        },
        'alert': {
            'after_minutes': _get(parser, 'ALERT', 'after_minutes', '60', int),
            'email_from': _get(parser, 'ALERT', 'email_from'),
            'email_to': _get(parser, 'ALERT', 'email_to'),
            'email_cc': _get(parser, 'ALERT', 'email_cc', ''),
            'email_bcc': _get(parser, 'ALERT', 'email_bcc', ''),
        },
        'smtp': {
            'server': _get(parser, 'SMTP', 'server'),
            'port': _get(parser, 'SMTP', 'port', '587', int),
            'username': _get(parser, 'SMTP', 'username'),
            'password': _get(parser, 'SMTP', 'password'),
            'tls': _get(parser, 'SMTP', 'tls', 'auto'),
        },
        'db': {
            'host': _get(parser, 'REMOTE_DB', 'host'),
            'port': _get(parser, 'REMOTE_DB', 'port', '3306', int),
            'database': _get(parser, 'REMOTE_DB', 'database'),
            'user': _get(parser, 'REMOTE_DB', 'user'),
            'password': _get(parser, 'REMOTE_DB', 'password'),
        },
    }


class MQTTMonitor:
    def __init__(self):
        self.cfg = load_config()
        self.last_message_time = None
        self.alert_sent = False
        self.conn = None
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        if self.cfg['mqtt']['username']:
            self.client.username_pw_set(
                self.cfg['mqtt']['username'],
                self.cfg['mqtt']['password']
            )

        self.connect_db()

    def connect_db(self):
        db = self.cfg['db']
        try:
            self.conn = mysql.connector.connect(
                host=db['host'],
                port=db['port'],
                database=db['database'],
                user=db['user'],
                password=db['password'],
                charset='utf8mb4',
                use_pure=True,
                connection_timeout=10,
            )
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logging.info("Connected to MQTT broker")
            client.subscribe(f"{self.cfg['mqtt']['publisher_client_id']}/#")
        else:
            logging.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_message(self, client, userdata, msg):
        self.last_message_time = datetime.now()
        self.alert_sent = False
        logging.debug(f"Message received on {msg.topic}")

    def _ping_attempt(self, host, timeout):
        try:
            result = subprocess.run(
                ['ping', '-c', '1', '-W', str(timeout), host],
                capture_output=True,
                text=True,
                timeout=timeout + 1
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, Exception) as e:
            logging.debug(f"Ping attempt error: {e}")
            return False

    def check_ping(self):
        host = self.cfg['ping']['host']
        attempts = self.cfg['ping']['attempts']
        timeout = self.cfg['ping']['timeout']
        results = []

        threads = []
        for _ in range(attempts):
            t = threading.Thread(target=lambda: results.append(self._ping_attempt(host, timeout)))
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=timeout + 2)

        successes = sum(1 for r in results if r)
        failed = successes == 0
        if failed:
            logging.error(f"Ping to {host} failed ({attempts} attempts, {successes} succeeded)")
        else:
            logging.info(f"Ping to {host} ok ({successes}/{attempts} succeeded)")
        return failed

    def check_publisher_activity(self):
        if self.last_message_time is None:
            return False, "No messages received yet"

        silence_duration = datetime.now() - self.last_message_time
        threshold = timedelta(minutes=self.cfg['alert']['after_minutes'])

        is_active = silence_duration < threshold
        status_msg = (
            f"Publisher active (last message: {silence_duration.total_seconds()/60:.1f} min ago)"
            if is_active else
            f"Publisher silent for {silence_duration.total_seconds()/60:.1f} minutes"
        )

        return is_active, status_msg

    def log_status(self, is_active, message, ping_failed):
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO mqtt_status
                (publisher, status, message_count, last_message, ping_failed)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                self.cfg['mqtt']['publisher_client_id'],
                'OK' if (is_active and not ping_failed) else 'ERROR',
                1 if is_active else 0,
                self.last_message_time if is_active else None,
                1 if ping_failed else 0,
            ))
            self.conn.commit()
            cur.close()
            logging.info(f"Status logged: {message} | ping_failed={ping_failed}")
        except Exception as e:
            logging.error(f"Failed to log status: {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass

    def _recent_alert_sent(self, within_minutes=60):
        """Check if an ERROR status was logged recently for this publisher.
        Prevents spamming one mail per cron run when the same condition persists."""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT timestamp FROM mqtt_status
                WHERE publisher = %s AND status = 'ERROR'
                ORDER BY id DESC LIMIT 1
            """, (self.cfg['mqtt']['publisher_client_id'],))
            row = cur.fetchone()
            cur.close()
            if not row:
                return False
            age_minutes = (datetime.now() - row[0]).total_seconds() / 60
            return age_minutes < within_minutes
        except Exception as e:
            logging.error(f"Failed to check recent alert: {e}")
            return False

    def _parse_recipients(self, value):
        if not value:
            return []
        return [addr.strip() for addr in value.split(',') if addr.strip()]

    def _open_smtp(self):
        smtp = self.cfg['smtp']
        host = smtp['server']
        port = smtp['port']
        mode = (smtp.get('tls') or 'auto').lower()
        if mode == 'auto':
            if port == 465:
                server = smtplib.SMTP_SSL(host, port, timeout=10)
            else:
                server = smtplib.SMTP(host, port, timeout=10)
                try:
                    server.starttls()
                except smtplib.SMTPNotSupportedError:
                    pass
        elif mode == 'ssl':
            server = smtplib.SMTP_SSL(host, port, timeout=10)
        elif mode == 'starttls':
            server = smtplib.SMTP(host, port, timeout=10)
            server.starttls()
        else:
            server = smtplib.SMTP(host, port, timeout=10)
        return server

    def send_alert(self, sections):
        if self.alert_sent or not sections:
            return

        publisher = self.cfg['mqtt']['publisher_client_id']
        subject = f"MQTT Alert - {publisher}"
        if len(sections) > 1:
            subject += f" ({len(sections)} issues)"

        body_lines = [f"MQTT monitoring alert for publisher '{publisher}':", ""]
        for title, content in sections:
            body_lines.append(f"--- {title} ---")
            body_lines.append(content)
            body_lines.append("")

        sender = self.cfg['alert']['email_from']
        to_addrs = self._parse_recipients(self.cfg['alert']['email_to'])
        cc_addrs = self._parse_recipients(self.cfg['alert']['email_cc'])
        bcc_addrs = self._parse_recipients(self.cfg['alert']['email_bcc'])

        if not sender or not to_addrs:
            logging.error("ALERT email_from or email_to missing; cannot send alert")
            return

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ', '.join(to_addrs)
        if cc_addrs:
            msg['Cc'] = ', '.join(cc_addrs)
        msg.set_content("\n".join(body_lines))

        all_recipients = to_addrs + cc_addrs + bcc_addrs

        try:
            server = self._open_smtp()
            server.login(
                self.cfg['smtp']['username'],
                self.cfg['smtp']['password']
            )
            server.sendmail(sender, all_recipients, msg.as_string())
            server.quit()
            self.alert_sent = True
            logging.info(
                f"Alert email sent to {len(all_recipients)} recipient(s): "
                f"{len(to_addrs)} to, {len(cc_addrs)} cc, {len(bcc_addrs)} bcc"
            )
        except Exception as e:
            logging.error(f"Failed to send alert: {e}")

    def run(self):
        try:
            self.client.connect(
                self.cfg['mqtt']['broker'],
                self.cfg['mqtt']['port'],
                60
            )
            self.client.loop_start()

            _time.sleep(self.cfg['mqtt']['warmup_seconds'])

            is_active, status_msg = self.check_publisher_activity()
            ping_failed = self.check_ping()
            self.log_status(is_active, status_msg, ping_failed)

            sections = []
            if not is_active:
                sections.append(("Publisher silence", status_msg))
            if ping_failed:
                host = self.cfg['ping']['host']
                attempts = self.cfg['ping']['attempts']
                sections.append((
                    "Ping failure",
                    f"Host {host} unreachable ({attempts} parallel attempts all failed)"
                ))

            if sections:
                logging.warning(f"Issues detected: {len(sections)}")
                if self._recent_alert_sent():
                    logging.info(
                        "Skipping email: similar ERROR status was logged within "
                        "the last hour. The new entry is recorded in the DB."
                    )
                else:
                    self.send_alert(sections)
            else:
                logging.info(f"All checks passed: {status_msg}")

        except Exception as e:
            logging.error(f"Monitor error: {e}")
        finally:
            self.client.loop_stop()
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass


if __name__ == "__main__":
    lock_file = open(LOCK_FILE_PATH, 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.warning("Another instance of mqtt_monitor.py is already running. Exiting.")
        sys.exit(0)

    monitor = MQTTMonitor()
    monitor.run()