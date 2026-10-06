"""Exercise only the HTTP tool module, never import the Telegram client."""
import asyncio
import importlib.util
import json
from pathlib import Path
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest

MODULE_PATH = Path(__file__).with_name('memory_proxy.py')
module = None
if MODULE_PATH.exists():
    spec = importlib.util.spec_from_file_location('memory_proxy', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


class ProxyTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(module, 'Memory proxy not implemented')
        self.requests = []
        self.status = 200
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def handle_request(self):
                payload = json.loads(self.rfile.read(int(self.headers['Content-Length']))) if self.command == 'POST' else None
                owner.requests.append((self.command, self.path, payload))
                self.send_response(owner.status)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'ok': True, 'hits': [{'message_id': 'synthetic-id'}]}).encode())
            do_GET = handle_request
            do_POST = handle_request
            def log_message(self, *args):
                pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop)
        self.tools = {}
        self.schemas = {}
        def tool(name, description, schema):
            self.schemas[name] = schema
            def register(fn):
                self.tools[name] = fn
                return fn
            return register
        module.register_memory_tools(tool, endpoint='http://127.0.0.1:' + str(self.server.server_port))

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def call(self, name, args):
        return asyncio.run(self.tools[name](args))

    def test_routes_and_fixed_source(self):
        for name, method, path, args in [
            ('semantic_search', 'POST', '/search', {'query': 'synthetic', 'after': '2026-01-01', 'limit': 4}),
            ('index_status', 'GET', '/status?source=telegram', {}),
            ('history_analytics', 'POST', '/analytics', {'group_by': 'day', 'before': 1789398000}),
        ]:
            with self.subTest(name=name):
                result = self.call(name, args)
                self.assertEqual(result, {'ok': True, 'hits': [{'message_id': 'synthetic-id'}]})
                actual_method, actual_path, payload = self.requests[-1]
                self.assertEqual((actual_method, actual_path), (method, path))
                if payload is not None:
                    self.assertEqual(payload['source'], 'telegram')
                    for key, value in args.items():
                        self.assertEqual(payload[key], value)

    def test_rejects_source_override_and_invalid_arguments_before_http(self):
        for args in [
            {'query': 'synthetic', 'source': 'whatsapp'}, {'query': 'synthetic', 'limit': 51},
            {'query': 'synthetic', 'limit': 1.5}, {'query': 'synthetic', 'mode': 'sql'},
            {'query': ' '}, {'query': 'synthetic', 'after': True},
        ]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.call('semantic_search', args)
        with self.assertRaises(ValueError):
            self.call('index_status', {'source': 'whatsapp'})
        with self.assertRaises(ValueError):
            self.call('history_analytics', {'group_by': 'sql'})
        self.assertEqual(self.requests, [])

    def test_backend_unavailable_is_a_useful_error(self):
        self.status = 503
        with self.assertRaisesRegex(RuntimeError, 'unavailable'):
            self.call('index_status', {})

    def test_search_plan_is_forwarded_with_fixed_source(self):
        for plan in [
            {}, {'keywords': []}, {'person': 'Synthetic Person'},
            {'keywords': ['invoice', 'receipt'], 'semantic_queries': ['paid the invoice'],
             'person': 'Synthetic Person', 'context': 'event', 'context_terms': ['payment']},
            {'keywords': ['ñ' * 80] * 8, 'semantic_queries': ['🧪' * 150] * 2,
             'person': 'ñ' * 100, 'context': 'none', 'context_terms': ['ñ' * 80] * 6},
            {'context': 'neighbors'},
        ]:
            with self.subTest(plan=plan):
                args = {'query': 'synthetic question', 'plan': plan, 'mode': 'keyword',
                        'chat': 'synthetic chat', 'sender': 'synthetic sender',
                        'after': '2026-01-01', 'before': 1789398000, 'limit': 3}
                self.call('semantic_search', args)
                self.assertEqual(self.requests[-1], ('POST', '/search', {**args, 'source': 'telegram'}))

    def test_search_plan_rejects_invalid_fields_before_http(self):
        invalid_plans = [None, [], 'keywords', 1, True,
                         {'unknown': 'x'}, {'source': 'whatsapp'},
                         {'person': ''}, {'person': '  '}, {'person': 3},
                         {'person': {}}, {'person': 'ñ' * 101},
                         {'context': None}, {'context': []}, {'context': {}},
                         {'context': 'sql'}, {'context': {'unknown': 'x'}}]
        for field, maximum, length in [('keywords', 8, 80), ('semantic_queries', 2, 600), ('context_terms', 6, 80)]:
            invalid_plans.extend({field: value} for value in [
                None, 'text', {}, True, ['x'] * (maximum + 1),
                [''], ['  '], [1], [False], [None], [['x']], [{'unknown': 'x'}],
                ['x' * (length + 1)],
            ])
        invalid_plans.extend([
            {'semantic_queries': ['🧪' * 150 + 'a']},
            {'semantic_queries': ['ñ' * 301]},
            {'semantic_queries': ['\ud800']},
        ])
        for plan in invalid_plans:
            with self.subTest(plan=plan), self.assertRaises(ValueError):
                self.call('semantic_search', {'query': 'synthetic', 'plan': plan})
        for tool in ['index_status', 'history_analytics']:
            with self.subTest(tool=tool), self.assertRaises(ValueError):
                self.call(tool, {'plan': {}})
        with self.assertRaises(ValueError):
            self.call('semantic_search', {'plan': {'keywords': ['synthetic']}})
        self.assertEqual(self.requests, [])

    def test_search_plan_schema_is_bounded_and_optional(self):
        schema = self.schemas['semantic_search']
        self.assertEqual(schema['required'], ['query'])
        self.assertFalse(schema['additionalProperties'])
        plan = schema['properties']['plan']
        self.assertEqual(plan['type'], 'object')
        self.assertFalse(plan['additionalProperties'])
        self.assertFalse(plan.get('required'))
        self.assertEqual(set(plan['properties']), {'keywords', 'semantic_queries', 'person', 'context', 'context_terms'})
        for field, maximum, length in [('keywords', 8, 80), ('semantic_queries', 2, 600), ('context_terms', 6, 80)]:
            array = plan['properties'][field]
            self.assertEqual((array['type'], array['maxItems']), ('array', maximum))
            self.assertEqual((array['items']['type'], array['items']['minLength'], array['items']['maxLength']), ('string', 1, length))
        self.assertEqual(plan['properties']['person']['maxLength'], 100)
        self.assertEqual(plan['properties']['context']['enum'], ['none', 'neighbors', 'event'])
        self.assertEqual(plan['properties']['context']['default'], 'neighbors')
        for tool in ['index_status', 'history_analytics']:
            self.assertNotIn('plan', self.schemas[tool]['properties'])

    def test_total_timeout_does_not_block_the_daemon_event_loop(self):
        previous_request, previous_timeout = module._request, module.TIMEOUT
        self.addCleanup(setattr, module, '_request', previous_request)
        self.addCleanup(setattr, module, 'TIMEOUT', previous_timeout)
        module.TIMEOUT = 0.01
        def slow_request(*args):
            time.sleep(0.15)
            return {}
        module._request = slow_request
        async def exercise():
            started = time.monotonic()
            with self.assertRaisesRegex(RuntimeError, 'unavailable'):
                await self.tools['index_status']({})
            self.assertLess(time.monotonic() - started, 0.1)
        asyncio.run(exercise())


if __name__ == '__main__':
    unittest.main()
