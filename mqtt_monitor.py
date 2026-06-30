import os
import configparser
import time as _time
import smtplib
import logging
import subprocess
import threading
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

CONFIG_PATH = os.getenv('MQTT_MONITOR_CONFIG', 'config.ini')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_monitor.log'),
        logging.StreamHandler()
    ]
)


def load_db_config(path=CONFIG_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Config file not found: {path}. Copy config.ini.example to {path}."
        )
    parser = configparser.RawConfigParser()
    parser.read(path)
    section = 'REMOTE_DB'
    if not parser.has_section(section):
        raise ValueError(f"Missing [{section}] section in {path}")
    cfg = dict(parser.items(section))
    if 'port' not in cfg:
        cfg['port'] = '3306'
    return cfg


class MQTTMonitor:
    def __init__(self):
        self.last_message_time = None
        self.alert_sent = False
        self.conn = None
        self.ping_result = None
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.client.username_pw_set(
            os.getenv('MQTT_USERNAME'),
            os.getenv('MQTT_PASSWORD')
        )

        self.connect_db()

    def connect_db(self):
        try:
            cfg = load_db_config(os.getenv('MQTT_MONITOR_CONFIG', CONFIG_PATH))
            self.conn = mysql.connector.connect(
                host=cfg['host'],
                port=int(cfg.get('port', 3306)),
                database=cfg['database'],
                user=cfg['user'],
                password=cfg['password'],
                charset='utf8mb4',
                use_pure=True,
                connection_timeout=10,
            )
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker")
            client.subscribe(f"{os.getenv('PUBLISHER_CLIENT_ID', 'asenta')}/#")
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
        host = os.getenv('PING_HOST', '193.5.176.14')
        attempts = int(os.getenv('PING_ATTEMPTS', 3))
        timeout = int(os.getenv('PING_TIMEOUT', 2))
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
        threshold = timedelta(minutes=int(os.getenv('ALERT_AFTER_MINUTES', 60)))

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
                os.getenv('PUBLISHER_CLIENT_ID', 'asenta'),
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

    def send_alert(self, sections):
        if self.alert_sent or not sections:
            return

        publisher = os.getenv('PUBLISHER_CLIENT_ID', 'asenta')
        subject = f"MQTT Alert - {publisher}"
        if len(sections) > 1:
            subject += f" ({len(sections)} issues)"

        body_lines = [f"MQTT monitoring alert for publisher '{publisher}':", ""]
        for title, content in sections:
            body_lines.append(f"--- {title} ---")
            body_lines.append(content)
            body_lines.append("")

        msg = f"Subject: {subject}\n\n" + "\n".join(body_lines)

        try:
            server = smtplib.SMTP(
                os.getenv('SMTP_SERVER'),
                int(os.getenv('SMTP_PORT', 2525))
            )
            server.starttls()
            server.login(
                os.getenv('SMTP_USERNAME'),
                os.getenv('SMTP_PASSWORD')
            )
            server.sendmail(
                os.getenv('ALERT_EMAIL_FROM'),
                os.getenv('ALERT_EMAIL_TO'),
                msg
            )
            server.quit()
            self.alert_sent = True
            logging.info(f"Alert email sent ({len(sections)} issue(s))")
        except Exception as e:
            logging.error(f"Failed to send alert: {e}")

    def run(self):
        try:
            self.client.connect(
                os.getenv('MQTT_BROKER'),
                int(os.getenv('MQTT_PORT', 1883)),
                60
            )
            self.client.loop_start()

            _time.sleep(int(os.getenv('MQTT_WARMUP_SECONDS', '10')))

            is_active, status_msg = self.check_publisher_activity()
            ping_failed = self.check_ping()
            self.log_status(is_active, status_msg, ping_failed)

            sections = []
            if not is_active:
                sections.append(("Publisher silence", status_msg))
            if ping_failed:
                host = os.getenv('PING_HOST', '193.5.176.14')
                attempts = int(os.getenv('PING_ATTEMPTS', 3))
                sections.append((
                    "Ping failure",
                    f"Host {host} unreachable ({attempts} parallel attempts all failed)"
                ))

            if sections:
                logging.warning(f"Issues detected: {len(sections)}")
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
    monitor = MQTTMonitor()
    monitor.run()