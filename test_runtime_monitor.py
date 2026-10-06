"""Synthetic runtime observation: no accounts, real processes or service changes."""
import asyncio
import importlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock

from aiohttp.test_utils import TestClient, TestServer
from monitoring_dashboard import application


def module(case):
    try:
        return importlib.import_module('runtime_monitor')
    except ModuleNotFoundError:
        case.fail('The isolated runtime observer has not been implemented')


class RuntimeMetricsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.proc = self.root / 'proc'
        self.proc.mkdir()
        (self.proc / 'meminfo').write_text('MemTotal: 8000 kB\nMemAvailable: 6000 kB\n')
        (self.proc / 'pressure').mkdir()
        (self.proc / 'pressure/io').write_text(
            'some avg10=12.50 avg60=10.00 avg300=5.00 total=123\n'
            'full avg10=4.00 avg60=3.00 avg300=2.00 total=100\n')

    def process(self, pid, *args):
        p = self.proc / str(pid)
        p.mkdir()
        (p / 'comm').write_text(Path(str(args[0])).name)
        (p / 'cmdline').write_bytes(b'\0'.join(str(a).encode() for a in args) + b'\0')
        (p / 'stat').write_text(f'{pid} (node) ' + ' '.join(['S'] + ['0'] * 18 + [str(1000 + pid)]))

    def telemetry(self, **changes):
        folder = self.root / 'run/mcp-runtime/screenwright'
        folder.mkdir(parents=True, exist_ok=True)
        payload = dict(schema_version=1, kind='screenwright', supervisor_pid=2,
                       supervisor_start_ticks=1002, upstream_pid=1, upstream_start_ticks=1001,
                       pending_requests=2, active_jobs=1, uncertain=False,
                       last_activity_ms=99900, sampled_at_ms=100000, idle_timeout_ms=90000)
        payload.update(changes)
        p = folder / '2.json'
        p.write_text(json.dumps(payload))
        p.chmod(0o600)
        return p

    def test_counts_known_entries_without_returning_arguments_or_secrets(self):
        mod = module(self)
        self.process(1, 'node', self.root / 'codebase/screenwright/src/index.js', 'secret-token')
        self.process(2, 'node', self.root / 'codebase/screenwright/scripts/remote-bootstrap.mjs')
        self.process(3, 'python', self.root / 'services/samuel-reputacion/server.py')
        self.process(4, 'python', self.root / 'unrelated.py', 'screenwright')
        self.process(5, 'vim', self.root / 'codebase/screenwright/src/index.js')
        # Opening this cmdline would fail: comm must exclude it before reading.
        excluded = self.proc / '5/cmdline'
        excluded.unlink()
        excluded.mkdir()
        data = mod.collect_snapshot(self.proc, self.root, self.root / 'run')
        self.assertTrue(data['inventory_complete'])
        self.assertEqual(data['services']['screenwright']['processes'], 2)
        self.assertEqual(data['services']['samuel']['processes'], 1)
        self.assertEqual(data['host']['memory_available_bytes'], 6000 * 1024)
        self.assertEqual(data['host']['io_full_avg10'], 4.0)
        for service in data['services'].values():
            self.assertIsNone(service['active_requests'])
            self.assertIsNone(service['protected_jobs'])
            self.assertIsNone(service['backend_limit'])
        text = json.dumps(data)
        self.assertNotIn('secret-token', text)
        self.assertNotIn(str(self.root), text)

    def test_unavailable_process_inventory_is_unknown_not_zero(self):
        mod = module(self)
        data = mod.collect_snapshot(self.root / 'missing-proc', self.root, self.root / 'run')
        self.assertFalse(data['inventory_complete'])
        self.assertTrue(all(x['processes'] is None for x in data['services'].values()))
        self.assertIsNone(data['host']['memory_available_bytes'])

    def test_warning_requires_consecutive_successful_samples_and_never_claims_a_limit(self):
        mod = module(self)
        state = mod.RuntimeState(warning_processes=2, warning_samples=3)
        data = mod.collect_snapshot(self.proc, self.root, self.root / 'run')
        data['services']['screenwright']['processes'] = 3
        for stamp in [100, 160]:
            state.record(data, stamp)
            self.assertEqual(state.snapshot(stamp)['warnings'], [])
        state.record(data, 220)
        snapshot = state.snapshot(220)
        self.assertEqual(snapshot['warnings'][0]['service'], 'screenwright')
        self.assertEqual(snapshot['warning_policy']['processes_per_service'], 2)
        self.assertIsNone(snapshot['services']['screenwright']['backend_limit'])
        data['services']['screenwright']['processes'] = 1
        state.record(data, 280)
        self.assertEqual(state.snapshot(280)['warnings'], [])

    def test_error_preserves_last_sample_but_resets_consecutive_warning_count(self):
        mod = module(self)
        state = mod.RuntimeState(warning_processes=2, warning_samples=2)
        data = mod.collect_snapshot(self.proc, self.root, self.root / 'run')
        data['services']['samuel']['processes'] = 3
        state.record(data, 100)
        state.failed(OSError('private detail never returned'))
        cached = state.snapshot(300)
        self.assertEqual(cached['services']['samuel']['processes'], 3)
        self.assertEqual(cached['error'], 'OSError')
        self.assertTrue(cached['stale'])
        self.assertNotIn('private detail', json.dumps(cached))
        state.record(data, 301)
        self.assertEqual(state.snapshot(301)['warnings'], [])

    def test_warning_configuration_is_bounded_and_separate_from_runtime_limits(self):
        mod = module(self)
        from unittest.mock import patch
        with patch.dict(os.environ, {'MONITORING_RUNTIME_WARN_PROCESSES': '12',
                                     'MONITORING_RUNTIME_WARN_SAMPLES': '4'}):
            state = mod.RuntimeState.from_environment()
        self.assertEqual(state.snapshot(0)['warning_policy'],
                         {'processes_per_service': 12, 'consecutive_samples': 4})
        with patch.dict(os.environ, {'MONITORING_RUNTIME_WARN_PROCESSES': '-1',
                                     'MONITORING_RUNTIME_WARN_SAMPLES': 'bad'}):
            self.assertEqual(mod.RuntimeState.from_environment().snapshot(0)['warning_policy'],
                             {'processes_per_service': 8, 'consecutive_samples': 3})

    def test_guard_telemetry_requires_live_pid_identity_and_reports_only_observed_counts(self):
        mod = module(self)
        self.process(1, 'node', self.root / 'codebase/screenwright/src/index.js')
        self.process(2, 'node', self.root / 'codebase/screenwright/scripts/remote-bootstrap.mjs')
        self.telemetry()
        result = mod.read_screenwright_telemetry(self.proc, self.root / 'run', now=100)
        self.assertEqual(result['telemetry_status'], 'available')
        self.assertEqual(result['active_requests'], 2)
        self.assertEqual(result['protected_jobs'], 1)
        self.assertEqual(result['backend_processes'], 1)
        self.assertNotIn('supervisor_pid', json.dumps(result))
        self.assertNotIn('backend_limit', result)
        self.telemetry(upstream_pid=None, upstream_start_ticks=None, active_jobs=0)
        result = mod.read_screenwright_telemetry(self.proc, self.root / 'run', now=100)
        self.assertEqual(result['backend_processes'], 0)

    def test_stale_reused_wrong_version_or_public_guard_file_is_not_accepted(self):
        mod = module(self)
        self.process(1, 'node', self.root / 'codebase/screenwright/src/index.js')
        self.process(2, 'node', self.root / 'codebase/screenwright/scripts/remote-bootstrap.mjs')
        for changes in [{'supervisor_start_ticks': 999}, {'upstream_start_ticks': 999},
                        {'sampled_at_ms': 1}, {'sampled_at_ms': 110000},
                        {'schema_version': 2}, {'schema_version': True}, {'pending_requests': True},
                        {'active_jobs': -1}]:
            with self.subTest(changes=changes):
                self.telemetry(**changes)
                result = mod.read_screenwright_telemetry(self.proc, self.root / 'run', now=100)
                self.assertEqual(result['telemetry_status'], 'unknown')
                self.assertIsNone(result['active_requests'])
        self.telemetry().chmod(0o644)
        self.assertEqual(mod.read_screenwright_telemetry(self.proc, self.root / 'run', now=100)
                         ['telemetry_status'], 'unknown')

    def test_collector_merges_guard_data_but_marks_incomplete_coverage_unknown(self):
        mod = module(self)
        self.process(1, 'node', self.root / 'codebase/screenwright/src/index.js')
        self.process(2, 'node', self.root / 'codebase/screenwright/scripts/remote-bootstrap.mjs')
        self.telemetry(sampled_at_ms=int(time.time() * 1000), uncertain=True)
        result = mod.collect_snapshot(self.proc, self.root, self.root / 'run')
        self.assertEqual(result['services']['screenwright']['telemetry_status'], 'uncertain')
        self.assertEqual(result['services']['screenwright']['protected_jobs'], 1)
        self.process(3, 'node', self.root / 'codebase/screenwright/scripts/remote-bootstrap.mjs')
        result = mod.collect_snapshot(self.proc, self.root, self.root / 'run')
        self.assertEqual(result['services']['screenwright']['telemetry_status'], 'partial')
        self.assertIsNone(result['services']['screenwright']['active_requests'])


class RuntimeHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.dashboard = Mock()
        self.dashboard.inventory.return_value = []
        self.client = None
        self.monitor = None
        self.release = threading.Event()

    async def asyncTearDown(self):
        self.release.set()
        if self.monitor:
            await self.monitor.close()
        if self.client:
            await self.client.close()

    async def start(self, monitor=None):
        kwargs = {'run_background': False}
        if monitor:
            kwargs['runtime_monitor'] = monitor
        self.client = TestClient(TestServer(application(self.dashboard, **kwargs)))
        await self.client.start_server()

    async def test_runtime_endpoint_and_panel_start_unknown_and_keep_host_protection(self):
        await self.start()
        response = await self.client.get('/api/runtime')
        self.assertEqual(response.status, 200)
        data = await response.json()
        self.assertEqual(data['status'], 'pending')
        self.assertIsNone(data['services']['screenwright']['active_requests'])
        bad = await self.client.get('/api/runtime', headers={'Host': 'attacker.example'})
        self.assertEqual(bad.status, 403)
        page = await (await self.client.get('/')).text()
        self.assertIn('id="runtime-panel"', page)
        self.assertIn('/api/runtime', page)

    async def test_slow_sampler_does_not_block_groups_or_selection_or_spawn_overlapping_samples(self):
        mod = module(self)
        started = threading.Event()
        calls = []
        def slow():
            calls.append(1)
            started.set()
            self.release.wait(3)
            return mod.empty_snapshot()
        self.monitor = mod.RuntimeMonitor(sampler=slow, interval=.01, timeout=.03)
        await self.start(self.monitor)
        self.monitor.start()
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        await asyncio.sleep(.08)
        response = await asyncio.wait_for(self.client.get('/api/runtime'), .5)
        self.assertEqual((await response.json())['error'], 'TimeoutError')
        self.assertEqual(len(calls), 1)
        self.assertEqual((await asyncio.wait_for(self.client.get('/api/groups'), .5)).status, 200)
        import re
        page = await (await self.client.get('/')).text()
        token = re.search("const csrf='([^']+)'", page).group(1)
        response = await asyncio.wait_for(self.client.post('/api/toggle',
            json={'platform': 'telegram', 'id': '-1', 'enabled': False},
            headers={'X-CSRF-Token': token}), .5)
        self.assertEqual(response.status, 200)
        self.dashboard.toggle.assert_called_once_with('telegram', '-1', False)

    async def test_failed_sampler_keeps_http_responsive_and_does_not_expose_exception_text(self):
        mod = module(self)
        def fail():
            raise OSError('sensitive path or credential')
        self.monitor = mod.RuntimeMonitor(sampler=fail)
        await self.start(self.monitor)
        self.monitor.start()
        for _ in range(50):
            response = await self.client.get('/api/runtime')
            data = await response.json()
            if data['status'] == 'error':
                break
            await asyncio.sleep(.01)
        self.assertEqual(data['error'], 'OSError')
        self.assertNotIn('sensitive', json.dumps(data))
        self.assertEqual((await self.client.get('/api/groups')).status, 200)
