"""Local observation only: no account access, messages, process control or limits.

The dashboard samples once a minute in a dedicated, single-worker executor.
Only known entrypoints owned by this user are counted. A count is not evidence
that a request is active or that a process may be stopped. Screenwright schema-1
telemetry is accepted only while private, fresh and tied to live PID identities.
Other guard telemetry and unpublished limits remain null. These are observed
guard counters, not proof that a business operation has finished.

MONITORING_RUNTIME_WARN_PROCESSES (default 8, range 1..10000) and
MONITORING_RUNTIME_WARN_SAMPLES (default 3, range 2..60) configure UI warnings,
not admission limits. State is deliberately in memory; restart clears warnings.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import math
import json
import os
from pathlib import Path
import stat
import time


ENTRYPOINTS = {
    'screenwright': ('codebase/screenwright/src/index.js',
                    'codebase/screenwright/scripts/remote-bootstrap.mjs'),
    'samuel': ('services/samuel-reputacion/server.py',),
    'gmail': ('.local/share/gmail-mcp-proxy/server.mjs',
              '.local/share/gmail-mcp-proxy/http_stdio.py'),
    'gmail-merauto': ('.local/share/gmail-merauto-mcp/server.mjs',
                      '.local/share/gmail-merauto-mcp/http_stdio.py'),
    'gmail-signara': ('.local/share/signara-gmail-mcp/server.mjs',
                      '.local/share/signara-gmail-mcp/http_stdio.py'),
    'proton': ('.local/share/proton-mail-mcp/proton_mcp.py',
               '.local/share/proton-mail-mcp/proton_http.py'),
    'qcdr': ('.local/share/qcdr-mail-mcp/mail_mcp.py',
             '.local/share/qcdr-mail-mcp/qcdr_http.py'),
}


def empty_snapshot():
    return {
        'host': {'memory_total_bytes': None, 'memory_available_bytes': None,
                 'io_some_avg10': None, 'io_full_avg10': None},
        'inventory_complete': False,
        'services': {name: {'processes': None, 'backend_processes': None, 'active_requests': None,
                            'protected_jobs': None, 'backend_limit': None,
                            'telemetry_status': 'unknown'} for name in ENTRYPOINTS},
    }


def collect_snapshot(proc=Path('/proc'), home=None, runtime_dir=None):
    """Read small proc files only. Never return command lines, IDs or paths."""
    proc, home = Path(proc), Path(home) if home is not None else Path.home()
    result = empty_snapshot()
    try:
        memory = dict(line.split(':', 1) for line in (proc / 'meminfo').read_text().splitlines())
        for source, target in [('MemTotal', 'memory_total_bytes'),
                               ('MemAvailable', 'memory_available_bytes')]:
            value = int(memory[source].split()[0]) * 1024
            if value >= 0:
                result['host'][target] = value
    except (OSError, ValueError, KeyError, IndexError):
        pass
    try:
        for line in (proc / 'pressure/io').read_text().splitlines():
            kind, *fields = line.split()
            if kind not in ('some', 'full'):
                continue
            value = float(dict(item.split('=', 1) for item in fields)['avg10'])
            if math.isfinite(value) and 0 <= value <= 100:
                result['host']['io_' + kind + '_avg10'] = value
    except (OSError, ValueError, KeyError):
        pass
    known = {str(home / path): name for name, paths in ENTRYPOINTS.items() for path in paths}
    counts = dict.fromkeys(ENTRYPOINTS, 0)
    supervisors, backends = set(), set()
    complete = True
    try:
        for process in proc.iterdir():
            if not process.name.isdecimal():
                continue
            try:
                if process.stat().st_uid != os.getuid():
                    continue
                comm = (process / 'comm').read_text().strip()
                if comm not in ('node', 'nodejs') and not comm.startswith('python'):
                    continue
                with (process / 'cmdline').open('rb') as stream:
                    args = stream.read(16384).split(b'\0')[:-1]
                argv = [arg.decode(errors='replace') for arg in args]
                executable = Path(argv[0]).name if argv else ''
                if executable not in ('node', 'nodejs') and not executable.startswith('python'):
                    continue
                name = next((known[arg] for arg in argv[1:] if arg in known), None)
                if name:
                    counts[name] += 1
                    if name == 'screenwright':
                        if str(home / ENTRYPOINTS[name][0]) in argv:
                            backends.add(int(process.name))
                        if str(home / ENTRYPOINTS[name][1]) in argv:
                            supervisors.add(int(process.name))
            except FileNotFoundError:  # process exited during this observation
                continue
            except OSError:
                complete = False
    except OSError:
        complete = False
    result['inventory_complete'] = complete
    if complete:
        for name, count in counts.items():
            result['services'][name]['processes'] = count
    runtime_dir = Path(runtime_dir) if runtime_dir is not None else Path('/run/user') / str(os.getuid())
    telemetry = read_screenwright_telemetry(proc, runtime_dir,
        expected_supervisors=supervisors, expected_backends=backends,
        inventory_complete=complete)
    result['services']['screenwright'].update(telemetry)
    return result


def read_screenwright_telemetry(proc, runtime_dir, now=None, expected_supervisors=None,
                               expected_backends=None, inventory_complete=True):
    """Bounded tmpfs reads; 45-second freshness covers three 15-second updates.

Reject reused PIDs, public/symlink files, invalid counters and stale records.
If known processes lack valid telemetry, totals remain unknown rather than
silently treating an unobserved request or job as idle.
"""
    result = {'telemetry_status': 'unknown', 'active_requests': None,
              'protected_jobs': None, 'backend_processes': None, 'uncertain': None}
    now = time.time() if now is None else now
    directory = Path(runtime_dir) / 'mcp-runtime/screenwright'
    records, supervisors, backends = [], set(), set()

    def integer(value, minimum=0):
        return type(value) is int and value >= minimum

    def matches(pid, ticks):
        if not integer(pid, 1) or not integer(ticks, 1):
            return False
        path = Path(proc) / str(pid)
        if path.stat().st_uid != os.getuid():
            return False
        fields = (path / 'stat').read_text().rsplit(')', 1)[1].split()
        return fields[0] != 'Z' and int(fields[19]) == ticks

    try:
        for index, path in enumerate(directory.iterdir()):
            if index >= 1024:  # cap observation work, never process creation
                return result
            if path.suffix != '.json' or not path.stem.isdecimal():
                continue
            try:
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor, 'rb') as stream:
                    info = os.fstat(stream.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                        continue
                    payload = stream.read(4097)
                if len(payload) > 4096:
                    continue
                data = json.loads(payload)
                if (not isinstance(data, dict) or type(data.get('schema_version')) is not int
                        or data['schema_version'] != 1 or data.get('kind') != 'screenwright'):
                    continue
                pid, ticks = data.get('supervisor_pid'), data.get('supervisor_start_ticks')
                if not matches(pid, ticks) or str(pid) != path.stem:
                    continue
                sampled = data.get('sampled_at_ms')
                if not integer(sampled) or not -5000 <= now * 1000 - sampled <= 45000:
                    continue
                if not all(integer(data.get(key)) for key in ('pending_requests', 'active_jobs', 'last_activity_ms')):
                    continue
                if type(data.get('uncertain')) is not bool or not integer(data.get('idle_timeout_ms'), 1):
                    continue
                upstream, upstream_ticks = data.get('upstream_pid'), data.get('upstream_start_ticks')
                if upstream is not None:
                    if not matches(upstream, upstream_ticks):
                        continue
                    backends.add(upstream)
                elif upstream_ticks is not None:
                    continue
                supervisors.add(pid)
                records.append(data)
            except (OSError, ValueError, TypeError, KeyError, IndexError):
                continue
    except OSError:
        return result
    if not records:
        return result
    if (not inventory_complete
            or expected_supervisors is not None and supervisors != expected_supervisors
            or expected_backends is not None and backends != expected_backends):
        return {**result, 'telemetry_status': 'partial'}
    uncertain = any(row['uncertain'] for row in records)
    return {**result, 'telemetry_status': 'uncertain' if uncertain else 'available',
            'active_requests': sum(row['pending_requests'] for row in records),
            'protected_jobs': sum(row['active_jobs'] for row in records),
            'backend_processes': len(backends), 'uncertain': uncertain,
            'reporting_supervisors': len(supervisors),
            'telemetry_age_seconds': round(max(0, now - min(row['sampled_at_ms'] for row in records) / 1000), 1)}


class RuntimeState:
    def __init__(self, warning_processes=8, warning_samples=3):
        self.warning_processes = warning_processes
        self.warning_samples = warning_samples
        self.data = empty_snapshot()
        self.sampled_at = None
        self.error = None
        self.streaks = {}
        self.warnings = []

    @classmethod
    def from_environment(cls):
        def number(name, default, minimum, maximum):
            try:
                value = int(os.environ.get(name, default))
                return value if minimum <= value <= maximum else default
            except ValueError:
                return default
        return cls(number('MONITORING_RUNTIME_WARN_PROCESSES', 8, 1, 10000),
                   number('MONITORING_RUNTIME_WARN_SAMPLES', 3, 2, 60))

    def record(self, data, sampled_at):
        self.data = deepcopy(data)
        self.sampled_at, self.error = sampled_at, None
        self.warnings = []
        for name, service in self.data['services'].items():
            count = service['processes']
            high = count is not None and count > self.warning_processes
            self.streaks[name] = self.streaks.get(name, 0) + 1 if high else 0
            if self.streaks[name] >= self.warning_samples:
                self.warnings.append({'code': 'persistent_process_count', 'service': name,
                    'processes': count, 'warning_threshold': self.warning_processes,
                    'consecutive_samples': self.streaks[name]})

    def failed(self, error):
        self.error = type(error).__name__  # errors can contain private paths or values
        self.streaks.clear()

    def snapshot(self, now=None):
        now = time.time() if now is None else now
        age = max(0, now - self.sampled_at) if self.sampled_at is not None else None
        return {**deepcopy(self.data), 'sampled_at': self.sampled_at,
                'age_seconds': round(age, 1) if age is not None else None,
                'stale': age is None or age > 120,
                'status': 'error' if self.error else 'pending' if age is None else 'ok',
                'error': self.error, 'warnings': deepcopy(self.warnings),
                'warning_policy': {'processes_per_service': self.warning_processes,
                                   'consecutive_samples': self.warning_samples}}


class RuntimeMonitor:
    def __init__(self, sampler=collect_snapshot, interval=60, timeout=5, state=None):
        self.sampler, self.interval, self.timeout = sampler, interval, timeout
        self.state = state or RuntimeState.from_environment()
        self.task = self.pool = None
        self.sampling = False

    def snapshot(self):
        return {**self.state.snapshot(), 'sampling': self.sampling}

    def start(self):
        if self.task is None:
            self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='runtime-observer')
            self.task = asyncio.create_task(self.run())

    async def run(self):
        while True:
            self.sampling = True
            future = asyncio.get_running_loop().run_in_executor(self.pool, self.sampler)
            try:
                try:
                    data = await asyncio.wait_for(asyncio.shield(future), self.timeout)
                except asyncio.TimeoutError as error:
                    self.state.failed(error)
                    # A slow read must never create a growing queue of more samples.
                    data = await future
                self.state.record(data, time.time())
            except Exception as error:
                self.state.failed(error)
            finally:
                self.sampling = False
            await asyncio.sleep(self.interval)

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        if self.pool is not None:
            self.pool.shutdown(wait=False, cancel_futures=True)
            self.pool = None

    async def background(self, app):
        self.start()
        try:
            yield
        finally:
            await self.close()


PANEL = '''
<section id="runtime-panel" aria-labelledby="runtime-title" style="margin-top:32px;border-top:1px solid #303a42;padding-top:18px">
<h2 id="runtime-title" style="font-size:20px">Estado del equipo</h2>
<p id="runtime-summary" role="status" aria-live="polite">Esperando la primera medición…</p>
<div id="runtime-warning" role="status" aria-live="polite" hidden style="border-left:3px solid #e1b86a;padding:10px 14px;color:#efd095"></div>
<div class="table" style="overflow-x:auto"><table><thead><tr><th>Servicio</th><th>Procesos</th><th>Backends observados</th><th>Solicitudes activas</th><th>Trabajos protegidos</th><th>Límite de backends</th></tr></thead><tbody id="runtime-rows"></tbody></table></div>
<small>Los procesos incluyen servidores y conexiones auxiliares. “Sin datos” indica que el servicio aún no publica esa medida. Los avisos de acumulación sólo se muestran aquí.</small>
<small id="runtime-telemetry"></small>
<small id="runtime-updated"></small></section>
<script>
(()=>{
const names={screenwright:'Screenwright',samuel:'Samuel',gmail:'Gmail', 'gmail-merauto':'Gmail Merauto','gmail-signara':'Gmail Signara',proton:'Proton',qcdr:'QCDR'};
const el=id=>document.getElementById(id),value=x=>x===null||x===undefined?'Sin datos':String(x);
async function refreshRuntime(){
try{
const response=await fetch('/api/runtime',{signal:AbortSignal.timeout(5000)});if(!response.ok)throw Error();const d=await response.json();
const h=d.host||{},parts=[];
if(h.memory_available_bytes!==null)parts.push('RAM disponible: '+(h.memory_available_bytes/1073741824).toFixed(1)+' GiB');
if(h.io_full_avg10!==null)parts.push('Espera de disco: '+h.io_full_avg10.toFixed(1)+'%');
el('runtime-summary').textContent=d.status==='pending'?'Esperando la primera medición…':(d.status==='error'||d.stale?'La medición no está actualizada. Últimos datos: ':'')+(parts.join(' · ')||'Medidas del equipo no disponibles.');
const rows=el('runtime-rows');rows.replaceChildren();for(const [name,s] of Object.entries(d.services||{})){const row=document.createElement('tr');for(const text of [names[name]||name,value(s.processes),value(s.backend_processes),value(s.active_requests),value(s.protected_jobs),value(s.backend_limit)]){const cell=document.createElement('td');cell.textContent=text;row.append(cell)}rows.append(row)}
const telemetry=d.services?.screenwright?.telemetry_status;el('runtime-telemetry').textContent=telemetry==='uncertain'?'Screenwright: hay actividad cuyo estado no está confirmado.':telemetry==='partial'?'Telemetría parcial de Screenwright; no se conocen los totales de solicitudes y trabajos.':telemetry==='available'?'Contadores observados del supervisor de Screenwright.':'Telemetría de actividad todavía no disponible.';
const warnings=d.warnings||[],notice=el('runtime-warning');notice.hidden=!warnings.length;notice.textContent=warnings.map(w=>(names[w.service]||w.service)+': '+w.processes+' procesos durante '+w.consecutive_samples+' mediciones. Umbral de aviso: '+w.warning_threshold+'.').join(' ');
el('runtime-updated').textContent=d.sampled_at?'Medido '+new Date(d.sampled_at*1000).toLocaleTimeString('es-MX')+' · Observación cada 60 segundos.':'Observación cada 60 segundos.';
}catch{el('runtime-summary').textContent='No se pudo actualizar el estado del equipo.'}
}refreshRuntime();setInterval(refreshRuntime,60000);
})();
</script>
'''


def add_runtime_panel(page):
    return page.replace('</main>', PANEL + '</main>', 1)
