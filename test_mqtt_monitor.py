"""
Tests for mqtt_monitor.py using unittest.mock — no real broker, DB, or SMTP.
"""
import os
import sys
import threading
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import mqtt_monitor as mm


@pytest.fixture
def env_only(monkeypatch):
    """Sets ENV vars but does NOT touch MQTT_MONITOR_CONFIG."""
    monkeypatch.setenv("MQTT_BROKER", "test-broker")
    monkeypatch.setenv("MQTT_PORT", "1883")
    monkeypatch.setenv("MQTT_USERNAME", "user")
    monkeypatch.setenv("MQTT_PASSWORD", "pass")
    monkeypatch.setenv("PUBLISHER_CLIENT_ID", "asenta")
    monkeypatch.setenv("PING_HOST", "193.5.176.14")
    monkeypatch.setenv("PING_ATTEMPTS", "3")
    monkeypatch.setenv("PING_TIMEOUT", "2")
    monkeypatch.setenv("ALERT_AFTER_MINUTES", "60")
    monkeypatch.setenv("ALERT_EMAIL_FROM", "from@test.com")
    monkeypatch.setenv("ALERT_EMAIL_TO", "to@test.com")
    monkeypatch.setenv("SMTP_SERVER", "smtp.test.com")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USERNAME", "smtpuser")
    monkeypatch.setenv("SMTP_PASSWORD", "smtppass")
    return monkeypatch


@pytest.fixture
def db_config_file(tmp_path, monkeypatch):
    p = tmp_path / "config.ini"
    p.write_text(
        "[REMOTE_DB]\n"
        "host = localhost\n"
        "database = mqtt_monitoring\n"
        "user = user\n"
        "password = pass\n"
    )
    monkeypatch.setenv("MQTT_MONITOR_CONFIG", str(p))
    return str(p)


@pytest.fixture
def monitor(env_only, db_config_file, monkeypatch):
    monkeypatch.setattr(mm.mqtt, "Client", MagicMock())
    monkeypatch.setattr(mm.mysql.connector, "connect", MagicMock(return_value=MagicMock()))
    return mm.MQTTMonitor()


# ---------- load_db_config ----------

def test_load_db_config_missing_file():
    with pytest.raises(FileNotFoundError):
        mm.load_db_config("/tmp/does-not-exist-12345.ini")


def test_load_db_config_missing_section(tmp_path):
    p = tmp_path / "config.ini"
    p.write_text("[OTHER]\nfoo = bar\n")
    with pytest.raises(ValueError):
        mm.load_db_config(str(p))


def test_load_db_config_default_port(tmp_path):
    p = tmp_path / "config.ini"
    p.write_text("[REMOTE_DB]\nhost = h\ndatabase = d\nuser = u\npassword = p\n")
    cfg = mm.load_db_config(str(p))
    assert cfg['port'] == '3306'
    assert cfg['host'] == 'h'


# ---------- check_ping ----------

def test_check_ping_all_success(monitor):
    with patch.object(monitor, "_ping_attempt", return_value=True):
        assert monitor.check_ping() is False


def test_check_ping_all_fail(monitor):
    with patch.object(monitor, "_ping_attempt", return_value=False):
        assert monitor.check_ping() is True


def test_check_ping_parallel_three_attempts(monitor):
    """3 threads run in parallel (deadlocks if serial)."""
    barrier = threading.Barrier(3)

    def fake_ping(host, timeout):
        barrier.wait(timeout=3)
        return True

    with patch.object(monitor, "_ping_attempt", side_effect=fake_ping):
        assert monitor.check_ping() is False


# ---------- check_publisher_activity ----------

def test_publisher_no_messages_yet(monitor):
    monitor.last_message_time = None
    active, msg = monitor.check_publisher_activity()
    assert active is False
    assert "No messages" in msg


def test_publisher_active(monitor):
    monitor.last_message_time = datetime.now() - timedelta(minutes=10)
    active, msg = monitor.check_publisher_activity()
    assert active is True
    assert "active" in msg


def test_publisher_silent(monitor):
    monitor.last_message_time = datetime.now() - timedelta(minutes=90)
    active, msg = monitor.check_publisher_activity()
    assert active is False
    assert "silent" in msg


# ---------- log_status ----------

def _cur_args(monitor):
    return monitor.conn.cursor.return_value.execute.call_args[0][1]


def test_log_status_ok(monitor):
    monitor.last_message_time = datetime.now()
    monitor.log_status(True, "ok", ping_failed=False)
    args = _cur_args(monitor)
    assert args[1] == "OK"
    assert args[2] == 1
    assert args[4] == 0


def test_log_status_error_silence(monitor):
    monitor.log_status(False, "silent", ping_failed=False)
    args = _cur_args(monitor)
    assert args[1] == "ERROR"
    assert args[2] == 0


def test_log_status_error_ping_only(monitor):
    """Publisher active but ping failed → still ERROR, message_count=1, ping_failed=1."""
    monitor.last_message_time = datetime.now()
    monitor.log_status(True, "ok", ping_failed=True)
    args = _cur_args(monitor)
    assert args[1] == "ERROR"
    assert args[2] == 1
    assert args[4] == 1


def test_log_status_uses_mysql_placeholder(monitor):
    monitor.log_status(True, "ok", ping_failed=False)
    sql = monitor.conn.cursor.return_value.execute.call_args[0][0]
    assert "%s" in sql
    assert "?" not in sql


# ---------- send_alert ----------

def test_send_alert_combined(monitor):
    smtp_mock = MagicMock()
    with patch.object(mm.smtplib, "SMTP", return_value=smtp_mock):
        monitor.send_alert([
            ("Publisher silence", "silent 75 min"),
            ("Ping failure", "host unreachable"),
        ])
    sent_msg = smtp_mock.sendmail.call_args[0][2]
    assert "Publisher silence" in sent_msg
    assert "Ping failure" in sent_msg
    assert "silent 75 min" in sent_msg
    assert "host unreachable" in sent_msg


def test_send_alert_only_silence(monitor):
    smtp_mock = MagicMock()
    with patch.object(mm.smtplib, "SMTP", return_value=smtp_mock):
        monitor.send_alert([("Publisher silence", "silent")])
    sent_msg = smtp_mock.sendmail.call_args[0][2]
    assert "(2 issues)" not in sent_msg.split("\n")[0]


def test_send_alert_no_sections(monitor):
    smtp_mock = MagicMock()
    with patch.object(mm.smtplib, "SMTP", return_value=smtp_mock):
        monitor.send_alert([])
    smtp_mock.sendmail.assert_not_called()


def test_send_alert_not_resent_when_flag_set(monitor):
    monitor.alert_sent = True
    smtp_mock = MagicMock()
    with patch.object(mm.smtplib, "SMTP", return_value=smtp_mock):
        monitor.send_alert([("Ping failure", "x")])
    smtp_mock.sendmail.assert_not_called()


def test_send_alert_smtp_failure(monitor):
    with patch.object(mm.smtplib, "SMTP", side_effect=Exception("smtp boom")):
        monitor.send_alert([("Ping failure", "x")])
    assert monitor.alert_sent is False


# ---------- on_message ----------

def test_on_message_resets_alert_and_updates_time(monitor):
    monitor.alert_sent = True
    monitor.last_message_time = None
    msg = MagicMock()
    msg.topic = "asenta/test"
    monitor.on_message(monitor.client, None, msg)
    assert monitor.last_message_time is not None
    assert monitor.alert_sent is False


# ---------- run() integration ----------

def test_run_all_ok(monitor):
    monitor.client.connect = MagicMock()
    monitor.client.loop_start = MagicMock()
    monitor.client.loop_stop = MagicMock()
    monitor.check_publisher_activity = MagicMock(return_value=(True, "Publisher active (1.0 min ago)"))
    monitor.check_ping = MagicMock(return_value=False)
    monitor.log_status = MagicMock()
    monitor.send_alert = MagicMock()
    with patch.dict(os.environ, {"MQTT_WARMUP_SECONDS": "0"}):
        monitor.run()
    monitor.log_status.assert_called_once_with(True, "Publisher active (1.0 min ago)", False)
    monitor.send_alert.assert_not_called()


def test_run_both_issues(monitor):
    monitor.client.connect = MagicMock()
    monitor.client.loop_start = MagicMock()
    monitor.client.loop_stop = MagicMock()
    monitor.check_publisher_activity = MagicMock(return_value=(False, "Publisher silent 90 min"))
    monitor.check_ping = MagicMock(return_value=True)
    monitor.log_status = MagicMock()
    monitor.send_alert = MagicMock()
    with patch.dict(os.environ, {"MQTT_WARMUP_SECONDS": "0"}):
        monitor.run()
    monitor.send_alert.assert_called_once()
    sections = monitor.send_alert.call_args[0][0]
    assert len(sections) == 2


def test_run_cleanup_on_exception(monitor):
    monitor.client.connect = MagicMock(side_effect=Exception("boom"))
    monitor.client.loop_stop = MagicMock()
    monitor.run()
    monitor.client.loop_stop.assert_called_once()
    monitor.conn.close.assert_called_once()