"""
Tests for mqtt_monitor.py using unittest.mock — no real broker, DB, or SMTP.
"""
import os
import sys
import smtplib
import threading
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import mqtt_monitor as mm


def write_config(path, **overrides):
    base = {
        'MQTT': {
            'broker': 'test-broker',
            'port': '1883',
            'username': 'user',
            'password': 'pass',
            'publisher_client_id': 'asenta',
            'warmup_seconds': '10',
        },
        'PING': {
            'host': '193.5.176.14',
            'attempts': '3',
            'timeout': '2',
        },
        'ALERT': {
            'after_minutes': '60',
            'email_from': 'from@test.com',
            'email_to': 'to@test.com',
            'email_cc': '',
            'email_bcc': '',
        },
        'SMTP': {
            'server': 'smtp.test.com',
            'port': '587',
            'username': 'smtpuser',
            'password': 'smtppass',
            'tls': 'auto',
        },
        'REMOTE_DB': {
            'host': 'localhost',
            'port': '3306',
            'database': 'mqtt_monitoring',
            'user': 'user',
            'password': 'pass',
        },
    }
    for section, values in overrides.items():
        base[section].update(values)

    with open(path, 'w') as f:
        for section, values in base.items():
            f.write(f"[{section}]\n")
            for k, v in values.items():
                f.write(f"{k} = {v}\n")
            f.write("\n")


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    p = tmp_path / "config.ini"
    write_config(str(p))
    monkeypatch.setenv('MQTT_MONITOR_CONFIG', str(p))
    return str(p)


@pytest.fixture
def monitor(config_file, monkeypatch):
    monkeypatch.setattr(mm.mqtt, "Client", MagicMock())
    monkeypatch.setattr(mm.mysql.connector, "connect", MagicMock(return_value=MagicMock()))
    return mm.MQTTMonitor()


# ---------- load_config ----------

def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        mm.load_config('/tmp/does-not-exist-12345.ini')


def test_load_config_missing_section(tmp_path, monkeypatch):
    p = tmp_path / "config.ini"
    p.write_text("[MQTT]\nbroker = h\n")
    monkeypatch.setenv('MQTT_MONITOR_CONFIG', str(p))
    with pytest.raises(ValueError):
        mm.load_config(str(p))


def test_load_config_default_values(tmp_path, monkeypatch):
    p = tmp_path / "config.ini"
    p.write_text(
        "[MQTT]\nbroker = h\npublisher_client_id =\n"
        "[PING]\n"
        "[ALERT]\nemail_from = a\nemail_to = b\n"
        "[SMTP]\nserver = s\nusername = u\npassword = p\n"
        "[REMOTE_DB]\nhost = h\ndatabase = d\nuser = u\npassword = p\n"
    )
    monkeypatch.setenv('MQTT_MONITOR_CONFIG', str(p))
    cfg = mm.load_config(str(p))
    assert cfg['mqtt']['publisher_client_id'] == 'asenta'
    assert cfg['mqtt']['port'] == 1883
    assert cfg['ping']['host'] == '193.5.176.14'
    assert cfg['smtp']['port'] == 587


def test_load_config_env_override(tmp_path, monkeypatch):
    p = tmp_path / "config.ini"
    write_config(str(p))
    monkeypatch.setenv('MQTT_MONITOR_CONFIG', str(p))
    monkeypatch.setenv('MQTT_BROKER', 'env-override.example.com')
    monkeypatch.setenv('ALERT_AFTER_MINUTES', '5')
    cfg = mm.load_config(str(p))
    assert cfg['mqtt']['broker'] == 'env-override.example.com'
    assert cfg['alert']['after_minutes'] == 5


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
    assert args[0] == 'asenta'
    assert args[1] == "OK"
    assert args[2] == 1
    assert args[4] == 0


def test_log_status_error_silence(monitor):
    monitor.log_status(False, "silent", ping_failed=False)
    args = _cur_args(monitor)
    assert args[1] == "ERROR"
    assert args[2] == 0


def test_log_status_error_ping_only(monitor):
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


# ---------- _parse_recipients ----------

def test_parse_recipients_single(monitor):
    assert monitor._parse_recipients('a@b.c') == ['a@b.c']


def test_parse_recipients_multiple(monitor):
    assert monitor._parse_recipients('a@b.c, d@e.f ,g@h.i') == ['a@b.c', 'd@e.f', 'g@h.i']


def test_parse_recipients_empty(monitor):
    assert monitor._parse_recipients('') == []
    assert monitor._parse_recipients(None) == []


# ---------- _open_smtp (TLS auto) ----------

def test_smtp_auto_port_465_uses_ssl(monitor):
    monitor.cfg['smtp']['port'] = 465
    with patch.object(mm.smtplib, "SMTP_SSL") as ssl_mock, \
         patch.object(mm.smtplib, "SMTP") as plain_mock:
        monitor._open_smtp()
        ssl_mock.assert_called_once()
        plain_mock.assert_not_called()


def test_smtp_auto_port_587_uses_starttls(monitor):
    monitor.cfg['smtp']['port'] = 587
    server = MagicMock()
    with patch.object(mm.smtplib, "SMTP", return_value=server) as plain_mock, \
         patch.object(mm.smtplib, "SMTP_SSL") as ssl_mock:
        monitor._open_smtp()
        plain_mock.assert_called_once()
        server.starttls.assert_called_once()
        ssl_mock.assert_not_called()


def test_smtp_auto_port_2525_no_tls(monitor):
    """Mailtrap 2525 doesn't support STARTTLS — must not raise."""
    monitor.cfg['smtp']['port'] = 2525
    server = MagicMock()
    server.starttls.side_effect = smtplib.SMTPNotSupportedError()
    with patch.object(mm.smtplib, "SMTP", return_value=server):
        result = monitor._open_smtp()
        assert result is server


def test_smtp_auto_unsupported_starttls_raises_other_errors(monitor):
    """Non-SMTPNotSupportedError STARTTLS errors must propagate."""
    monitor.cfg['smtp']['port'] = 2525
    server = MagicMock()
    server.starttls.side_effect = RuntimeError("tls broken")
    with patch.object(mm.smtplib, "SMTP", return_value=server):
        with pytest.raises(RuntimeError):
            monitor._open_smtp()


def test_smtp_explicit_starttls(monitor):
    monitor.cfg['smtp']['tls'] = 'starttls'
    server = MagicMock()
    with patch.object(mm.smtplib, "SMTP", return_value=server):
        monitor._open_smtp()
        server.starttls.assert_called_once()


def test_smtp_explicit_ssl(monitor):
    monitor.cfg['smtp']['tls'] = 'ssl'
    monitor.cfg['smtp']['port'] = 587  # explicit override beats auto
    with patch.object(mm.smtplib, "SMTP_SSL") as ssl_mock, \
         patch.object(mm.smtplib, "SMTP") as plain_mock:
        monitor._open_smtp()
        ssl_mock.assert_called_once()
        plain_mock.assert_not_called()


def test_smtp_explicit_none(monitor):
    monitor.cfg['smtp']['tls'] = 'none'
    server = MagicMock()
    with patch.object(mm.smtplib, "SMTP", return_value=server):
        monitor._open_smtp()
        server.starttls.assert_not_called()


# ---------- send_alert ----------

def _sendmail_args(smtp_mock):
    """Extract (sender, recipients, payload) from sendmail call."""
    return smtp_mock.sendmail.call_args[0]


def test_send_alert_combined(monitor):
    smtp_mock = MagicMock()
    with patch.object(monitor, "_open_smtp", return_value=smtp_mock):
        monitor.send_alert([
            ("Publisher silence", "silent 75 min"),
            ("Ping failure", "host unreachable"),
        ])
    sender, recipients, payload = _sendmail_args(smtp_mock)
    assert "Publisher silence" in payload
    assert "Ping failure" in payload
    assert "silent 75 min" in payload


def test_send_alert_multiple_recipients(monitor):
    monitor.cfg['alert']['email_to'] = 'a@x.ch, b@y.ch'
    monitor.cfg['alert']['email_cc'] = 'c@z.ch'
    smtp_mock = MagicMock()
    with patch.object(monitor, "_open_smtp", return_value=smtp_mock):
        monitor.send_alert([("Ping failure", "x")])
    sender, recipients, payload = _sendmail_args(smtp_mock)
    assert recipients == ['a@x.ch', 'b@y.ch', 'c@z.ch']


def test_send_alert_uses_email_message(monitor):
    smtp_mock = MagicMock()
    with patch.object(monitor, "_open_smtp", return_value=smtp_mock):
        monitor.send_alert([("Ping failure", "host x")])
    payload = _sendmail_args(smtp_mock)[2]
    assert payload.startswith("Subject:")
    assert "asenta" in payload
    assert "host x" in payload


def test_send_alert_no_sections(monitor):
    smtp_mock = MagicMock()
    with patch.object(monitor, "_open_smtp", return_value=smtp_mock):
        monitor.send_alert([])
    smtp_mock.sendmail.assert_not_called()


def test_send_alert_not_resent_when_flag_set(monitor):
    monitor.alert_sent = True
    smtp_mock = MagicMock()
    with patch.object(monitor, "_open_smtp", return_value=smtp_mock):
        monitor.send_alert([("Ping failure", "x")])
    smtp_mock.sendmail.assert_not_called()


def test_send_alert_smtp_failure(monitor):
    with patch.object(monitor, "_open_smtp", side_effect=Exception("smtp boom")):
        monitor.send_alert([("Ping failure", "x")])
    assert monitor.alert_sent is False


def test_send_alert_missing_recipients(monitor):
    monitor.cfg['alert']['email_to'] = ''
    smtp_mock = MagicMock()
    with patch.object(monitor, "_open_smtp", return_value=smtp_mock):
        monitor.send_alert([("Ping failure", "x")])
    smtp_mock.sendmail.assert_not_called()


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
    monitor._recent_alert_sent = MagicMock(return_value=False)
    monitor.cfg['mqtt']['warmup_seconds'] = 0
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
    monitor._recent_alert_sent = MagicMock(return_value=False)
    monitor.cfg['mqtt']['warmup_seconds'] = 0
    monitor.run()
    monitor.send_alert.assert_called_once()
    sections = monitor.send_alert.call_args[0][0]
    assert len(sections) == 2


def test_run_skips_alert_when_recently_sent(monitor):
    """Anti-spam: if ERROR was logged <60min ago, skip email."""
    monitor.client.connect = MagicMock()
    monitor.client.loop_start = MagicMock()
    monitor.client.loop_stop = MagicMock()
    monitor.check_publisher_activity = MagicMock(return_value=(True, "Publisher active"))
    monitor.check_ping = MagicMock(return_value=True)
    monitor.log_status = MagicMock()
    monitor.send_alert = MagicMock()
    monitor._recent_alert_sent = MagicMock(return_value=True)
    monitor.cfg['mqtt']['warmup_seconds'] = 0
    monitor.run()
    monitor.send_alert.assert_not_called()


def test_recent_alert_sent_true(monitor):
    cur = monitor.conn.cursor.return_value
    cur.fetchone.return_value = (datetime.now() - timedelta(minutes=10),)
    assert monitor._recent_alert_sent(within_minutes=60) is True


def test_recent_alert_sent_false_old(monitor):
    cur = monitor.conn.cursor.return_value
    cur.fetchone.return_value = (datetime.now() - timedelta(hours=2),)
    assert monitor._recent_alert_sent(within_minutes=60) is False


def test_recent_alert_sent_false_no_row(monitor):
    cur = monitor.conn.cursor.return_value
    cur.fetchone.return_value = None
    assert monitor._recent_alert_sent(within_minutes=60) is False


def test_recent_alert_sent_db_error(monitor):
    monitor.conn.cursor.side_effect = Exception("db boom")
    assert monitor._recent_alert_sent(within_minutes=60) is False


def test_run_cleanup_on_exception(monitor):
    monitor.client.connect = MagicMock(side_effect=Exception("boom"))
    monitor.client.loop_stop = MagicMock()
    monitor.run()
    monitor.client.loop_stop.assert_called_once()
    monitor.conn.close.assert_called_once()