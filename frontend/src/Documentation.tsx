import { Link } from 'react-router-dom';

const sections = [['overview', 'Overview'], ['architecture', 'Architecture'], ['data-flow', 'Data flow'], ['stack', 'Tech stack'], ['operations', 'Operations'], ['configuration', 'Configuration'], ['api', 'API reference']] as const;
const flow = [
  ['Register', 'Add a channel and its hops on the canvas. HLS probe targets create stream registrations. MongoDB is the inventory source of truth.'],
  ['Synchronize', 'The inventory worker projects registrations into Redis. Version checks prevent old updates from replacing newer configuration; failures retry with backoff.'],
  ['Schedule', 'The scheduler checks due streams every 500 ms and queues bounded batches. Leases prevent duplicate outstanding jobs. Each server has its own Redis Stream.'],
  ['Probe', 'Consumers fetch the playlist, follow supported master variants and request bytes 0–1023 of a media segment. Reading stops after the first nonempty chunk. Frozen-playlist tracking verifies progress.'],
  ['Publish and persist', 'Results update the Redis status feed for live clients and enter per-server persistence queues. MongoDB stores observation history; acknowledgement follows successful persistence. Live status can arrive before history is saved.'],
  ['Recover', 'Pending deliveries are reclaimed after worker interruption. Retryable failures back off; exhausted or terminal processing failures enter the DLQ and are projected into searchable MongoDB history.'],
  ['Evaluate and report', 'The pipeline monitor combines hop observations with topology to identify root causes and fallback paths. Hourly rollups feed reports; MongoDB combines hourly totals per stream before sending them to the app.'],
];
const stack = [
  ['Runtime', 'Node.js · TypeScript · tsx', 'Typed backend; tsx watches source during development.'],
  ['HTTP API', 'Fastify 5', 'Routes, validation, lifecycle hooks and authenticated API access.'],
  ['Frontend', 'React · Vite · React Router', 'Responsive application shell, client routes and production assets.'],
  ['Canvas', '@xyflow/react', 'Channel nodes, directional links and workspace visualization.'],
  ['Scheduling and delivery', 'Redis · ioredis · Redis Streams', 'Due schedules, leases, consumer groups, pending recovery, live status and sessions.'],
  ['Durable storage', 'MongoDB · native driver', 'Inventory, channel topology, metrics, hourly rollups, incidents, users and failure history.'],
  ['Authentication', 'Passport Local · Fastify sessions · scrypt', 'Username/password sign-in, Redis sessions, account updates and rate limiting.'],
  ['Live updates', 'Server-Sent Events', 'Snapshot plus cursor replay; periodic observations keep freshness visible.'],
  ['Notifications', 'Web Push · optional webhook', 'Background incident alerts and device subscriptions.'],
  ['API documentation', 'OpenAPI · @fastify/swagger · Scalar', 'Generated route specification and interactive API reference.'],
  ['Validation', 'Node test runner · Playwright', 'Unit/API tests, opt-in database integration tests and browser tests.'],
  ['Local infrastructure', 'Docker Compose', 'Redis, MongoDB and Redis Commander for local development.'],
];
const config = [
  ['PORT / HOST', '3000 / 0.0.0.0', 'HTTP listener.'],
  ['SESSION_SECRET', 'Required', 'At least 32 characters; keep it private and stable across instances.'],
  ['MONGO_URI', 'Or individual Mongo credentials', 'MongoDB connection; individual settings include MONGO_USER, MONGO_PASSWORD, MONGO_HOST, MONGO_PORT and MONGO_DATABASE.'],
  ['REDIS_PASSWORD', 'Required for Compose', 'Redis authentication. Configure the backend to reach the same Redis instance.'],
  ['HLS_MONITORING_ENABLED', 'true', 'Set false to disable stream monitoring.'],
  ['HLS_MAX_CONCURRENT_PROBES', '64', 'Global active HLS probe limit.'],
  ['HLS_MAX_WAITING_PROBES', '256', 'Bounded queue waiting for a probe slot.'],
  ['HLS_SERVER_PROBE_CONCURRENCY', '10', 'Per-server consumer probe concurrency.'],
  ['HLS_SYNC_POLL_MS', '1000', 'Inventory synchronization cadence, milliseconds.'],
  ['PIPELINE_INTERVAL_MS', '10000', 'Pipeline evaluation cadence, milliseconds.'],
  ['PIPELINE_HOLD_DOWN_MS', '30000', 'Recovery hold-down window, milliseconds.'],
  ['DLQ_MAX_ATTEMPTS', '5', 'Processing retry limit before dead-letter handling.'],
  ['PIPELINE_ALERT_WEBHOOK_URL', 'Optional', 'HTTP(S) incident receiver.'],
];

export function Documentation() {
  return <div className="documentation">
    <header className="docs-heading"><p className="hc-eyebrow">HLS monitor / Knowledge base</p><h1>Documentation</h1><p>Understand the system, trace a stream and operate the monitoring pipeline.</p></header>
    <div className="docs-layout">
      <nav className="docs-toc" aria-label="Documentation sections">{sections.map(([id, title]) => <a key={id} href={`#${id}`}>{title}</a>)}</nav>
      <div className="docs-body">
        <section id="overview"><h2>Overview</h2><p>The monitor checks HLS delivery across channel signal paths, identifies affected hops, tracks availability and records processing failures. Channel topology describes where the signal travels; probe targets describe how each hop is checked.</p>
          <div className="docs-cards"><Link to="/create"><b>Build a channel</b><span>Create nodes and connect PRIMARY, FAILOVER, BYPASS or FALLBACK paths.</span></Link><Link to="/workspace"><b>Explore the workspace</b><span>See channels and shared servers across the complete topology.</span></Link><a href="/docs/scalar/" target="_blank" rel="noopener noreferrer"><b>API reference ↗</b><span>Browse endpoints and try authenticated requests with Scalar.</span></a></div>
        </section>
        <section id="architecture"><h2>Software architecture</h2><p>A single Node.js application serves the frontend and API and runs background monitoring workers. Redis handles coordination and live state; MongoDB stores durable configuration and history.</p>
          <div className="docs-architecture" aria-label="Application architecture"><div>Browser<span>React UI · Canvas · Scalar</span></div><span aria-hidden="true">↕ HTTP / SSE</span><div>Application<span>Fastify API · Auth · Monitoring workers</span></div><div className="docs-storage"><div>Redis<span>Schedule · Streams · Status · Sessions</span></div><div>MongoDB<span>Inventory · Metrics · Reports · Failures</span></div></div></div>
          <h3>Reliability boundaries</h3><ul><li>MongoDB inventory changes converge into Redis through a retrying sync worker.</li><li>Delivery is at least once. Durable writes use message identities to tolerate repeated delivery.</li><li>Persistence queues include in-flight data in their capacity limits. Failed persistence is retained for retry.</li><li>SSE begins with a snapshot and resumes from its cursor. Missing replay history triggers resynchronization.</li><li>Graceful shutdown stops workers and drains accepted work before closing database connections.</li></ul>
          <p>Pipeline rounds do not overlap. Probe work and incident tasks use bounded concurrency. Multiple application instances currently run their own pipeline probing; a single elected prober is a future scaling option.</p>
        </section>
        <section id="data-flow"><h2>HLS data flow</h2><p>The detailed whiteboard includes the scheduler, queues, storage, alerts and recovery paths.</p><a className="btn" href="/docs/data-flow" target="_blank" rel="noopener noreferrer">Open complete architecture diagram ↗</a>
          <ol className="docs-steps">{flow.map(([name, text]) => <li key={name}><h3>{name}</h3><p>{text}</p></li>)}</ol>
          <h3>Health semantics</h3><p>HLS checks verify playlist access, media bytes and progress. Requests have a 3-second timeout within a 6-second overall probe budget. Failure hysteresis avoids flipping a previously healthy stream offline after one transient fetch failure.</p><p>SRT server ping verifies host reachability only. It does not establish SRT handshake success, received bitrate or media health. A hop without a verified probe result stays unknown.</p>
        </section>
        <section id="stack"><h2>Technology stack</h2><div className="docs-table"><table><thead><tr><th>Layer</th><th>Technology</th><th>Responsibility</th></tr></thead><tbody>{stack.map(([layer, tech, purpose]) => <tr key={layer}><th scope="row">{layer}</th><td>{tech}</td><td>{purpose}</td></tr>)}</tbody></table></div></section>
        <section id="operations"><h2>Development and operations</h2><h3>Start locally</h3><p>Use Node.js 22 or newer and an accessible Redis and MongoDB instance. Configure your private environment values before starting Compose or the backend.</p><pre><code>{'npm install\ndocker compose up -d\nnpm run dev'}</code></pre><p>Build frontend assets for the backend-served pages, or use the separate Vite development server while editing the UI.</p><pre><code>{'npm run build       # compile backend and frontend\nnpm start           # run compiled backend\nnpm run web:dev     # Vite frontend development server\nnpm run typecheck\nnpm test'}</code></pre><p>Database integration tests are opt-in. Use isolated test databases and follow <code>tests/integration/README.md</code> before running <code>npm run test:integration</code>.</p>
          <h3>Users and access</h3><p>Create the first operator from a terminal; the CLI asks for the password without echoing it. There are no roles: signed-in users can manage the monitoring configuration. Account lets users change their username and password.</p><pre><code>{'npm run user -- add operator\nnpm run user -- list\nnpm run user -- disable operator'}</code></pre>
          <h3>Install as an app</h3><p>On HTTPS or localhost, supporting browsers can install HLS Monitor as a standalone app. Use Install app in Account or your browser menu. On iPhone/iPad, use Safari’s Share → Add to Home Screen. The existing service worker also handles Web Push. Only public install assets are cached; authenticated pages, APIs and live monitoring data remain network-only. Offline navigation shows a reconnect page.</p><h3>Configuration import and export</h3><p>Account can export channels, nodes, links, HLS/TCP probe targets and stream settings as versioned JSON. Select a file to validate it and review counts before importing. Existing identities are rejected, so an import never intentionally overwrites current configuration. Confirm with your current password and IMPORT. Accounts and historical data are excluded. Exports contain full URLs and may include access tokens; store them privately. Browser-only canvas positions are excluded.</p><h3>Reset monitoring data</h3><p>Account contains a full monitoring reset. Enter your current password, type RESET and select Confirm reset. This permanently removes channels, streams, probe targets, metrics, rollups, incidents, failures and Redis monitoring state for the installation. Accounts, sessions and notification settings remain. Workers pause during deletion and resume with empty configuration; legacy/file seeds are not imported again automatically.</p><h3>Storage and retention</h3><p>Raw metrics and hourly rollups have 7-day retention. Redis job history is trimmed after one hour. Resolved failure history has 30-day retention; open failures are retained. MongoDB TTL deletion is asynchronous.</p>
          <h3>Troubleshooting</h3><dl><dt>Port already in use</dt><dd>Check the existing listener on port 3000 before starting another instance, or configure PORT. MongoDB Compose also needs its configured host port to be free.</dd><dt>Unknown / not probed</dt><dd>Check the node target, enabled stream registration and latest observation time. A server ping alone cannot prove stream health.</dd><dt>Live status lost</dt><dd>Check the authenticated session, SSE connection and Redis availability. Clients reconnect and resynchronize when replay is unavailable.</dd><dt>Processing failures</dt><dd>Use DLQ to inspect the stage and reason. Correct the underlying issue before requesting replay.</dd><dt>No recent metrics</dt><dd>Inspect database connectivity, scheduler and consumer activity. The watchdog reports stalled saving; the health endpoint reports database availability.</dd></dl>
        </section>
        <section id="configuration"><h2>Configuration reference</h2><p>These are application defaults, not the values of your private environment. Timing values use milliseconds unless stated otherwise.</p><div className="docs-table"><table><thead><tr><th>Variable</th><th>Default</th><th>Purpose</th></tr></thead><tbody>{config.map(([name, value, purpose]) => <tr key={name}><th scope="row"><code>{name}</code></th><td>{value}</td><td>{purpose}</td></tr>)}</tbody></table></div></section>
        <section id="api"><h2>API reference</h2><p>Scalar provides the generated endpoint specification, request schemas and interactive requests. Sign in first; requests use the existing HttpOnly session cookie. State-changing requests must be same-origin. Stream edits and deletion require the current version in <code>If-Match</code>.</p><div className="docs-cards"><a href="/docs/scalar/" target="_blank" rel="noopener noreferrer"><b>Open Scalar API ↗</b><span>Streams, pipelines, reports, failures, account and notifications.</span></a><a href="/api/openapi.json" target="_blank" rel="noopener noreferrer"><b>OpenAPI specification ↗</b><span>Downloadable JSON generated from registered routes.</span></a></div><p>Live event endpoints are long-lived SSE connections. Use EventSource for streaming consumers rather than waiting for a complete JSON response.</p></section>
      </div>
    </div>
  </div>;
}
