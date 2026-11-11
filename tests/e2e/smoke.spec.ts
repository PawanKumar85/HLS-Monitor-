import { test, expect, type Page } from '@playwright/test';

async function signIn(page: Page, next = '/') {
  await page.goto(`/login?next=${encodeURIComponent(next)}`);
  await page.getByLabel(/username/i).fill('e2e-user');
  await page.getByLabel(/password/i).fill(process.env['E2E_PASSWORD']!);
  await page.getByRole('button', { name: /sign in/i }).click();
}

test.beforeEach(async ({ page }) => {
  page.on('pageerror', error => { throw error; }); // any uncaught script error fails the test
});
test.afterEach(async ({ page }) => {
  await page.request.post('/logout', { headers: { origin: 'http://127.0.0.1:3200' } }); // never leave a session behind
});

test('sidebar order: Dashboard, Workspace, Add Stream, Reports, DLQ, Account', async ({ page }) => {
  await signIn(page);
  await expect(page.getByRole('navigation', { name: 'Main navigation' }).getByRole('link')).toHaveText(['Dashboard', 'Workspace', 'Add Stream', 'Reports', 'DLQ', 'Account']);
  await expect(page.getByRole('link', { name: 'Dashboard', exact: true })).toHaveClass(/active/);
  await expect(page.getByRole('link', { name: 'Workspace', exact: true })).not.toHaveClass(/active/);
});

test('sign in, then every page in the sidebar renders', async ({ page }) => {
  await signIn(page);
  await expect(page).toHaveURL('http://127.0.0.1:3200/');
  for (const [link, title] of [['Workspace', 'Workspace'], ['Add Stream', 'Add Stream'], ['Reports', 'Reports'], ['DLQ', 'DLQ'], ['Dashboard', 'Dashboard']] as const) {
    await page.getByRole('navigation', { name: 'Main navigation' }).getByRole('link', { name: link, exact: true }).click();
    await expect(page).toHaveTitle(`${title} | HLS monitor`);
  }
});

test('an unknown address shows the app’s Page not found', async ({ page }) => {
  await signIn(page, '/pipline');
  await expect(page.getByRole('heading', { name: 'Page not found' })).toBeVisible();
});

test('unsaved canvas changes ask before a sidebar link leaves the page', async ({ page }) => {
  await signIn(page, '/pipeline'); // old address: redirects to /create
  await expect(page).toHaveURL(/\/create$/);
  await page.getByRole('button', { name: 'Add channel' }).first().click();
  await page.getByRole('button', { name: '+ Transcoder' }).click();
  await page.getByRole('navigation', { name: 'Main navigation' }).getByRole('link', { name: 'Workspace', exact: true }).click();
  await expect(page.getByText('Discard your unsaved canvas changes?')).toBeVisible();
  await expect(page).toHaveURL(/\/create$/);
  await page.getByRole('button', { name: 'Keep editing' }).click();
  await expect(page.getByText('Discard your unsaved canvas changes?')).toBeHidden();
  await page.getByRole('navigation', { name: 'Main navigation' }).getByRole('link', { name: 'Workspace', exact: true }).click();
  await page.getByRole('button', { name: 'Discard changes' }).click();
  await expect(page).toHaveURL(/\/workspace$/);
});

test('Reports: a server opens to hourly bars, an outage log and channel names for pipeline streams', async ({ page }) => {
  // Seed the e2e database directly: a channel, hourly totals (what the report reads) and raw checks (what the outage log reads).
  await import('dotenv/config');
  const { MongoClient } = await import('mongodb');
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db('gate_test_e2e');
  const now = Date.now(); const HOUR = 3_600_000; const hour = (h: number) => new Date(Math.floor((now - h * HOUR) / HOUR) * HOUR);
  const streamId = 'pipeline:e2e_channel/cdn'; const serverIp = 'e2e-origin.test';
  await db.collection('pipeline_channels').insertOne({ _id: 'e2e_channel' as never, channelName: 'E2E Channel', nodes: [{ id: 'cdn', name: 'CDN', type: 'EGRESS_CDN' }], edges: [], updatedAt: new Date() });
  await db.collection('hls_stream_hourly').insertMany([0, 1, 2].map(h => ({
    _id: { serverIp, streamId, hour: hour(h) } as never, serverIp, streamId, hour: hour(h), checks: 360, observedMs: HOUR, downtimeMs: h === 1 ? 10 * 60_000 : 0, outages: h === 1 ? 1 : 0,
    lastOfflineAt: h === 1 ? hour(1) : null, latencyCount: 360, latencySum: 360 * 200, latencyMax: 900, latencyBuckets: [0, 0, 300, 50, 10, 0, 0, 0, 0, 0, 0, 0], reasons: h === 1 ? { TIMEOUT: 60 } : {},
  })));
  const check = (minutesAgo: number, status: 0 | 1) => { const t = new Date(now - minutesAgo * 60_000); return { serverIp, streamId, streamUrl: 'http://x/l.m3u8', createdAt: t, receivedAt: t, checkedAt: t, ingestedAt: t,
    pipelineDelayMs: 5, health: { status, segmentLatencyMs: status ? 200 : 3000, ...(status ? {} : { reason: 'TIMEOUT' }) }, redisStreamKey: 'k', redisMessageId: `${minutesAgo}-0` }; };
  await db.collection('hls_stream_metrics').insertMany([check(30, 1), check(20, 0), check(10, 0), check(5, 1)]);
  await mongo.close();

  await signIn(page, '/reports?hours=168'); // 168 h: its own cache entry, not the empty 24 h report an earlier test loaded
  await page.getByRole('button', { name: serverIp }).click();
  await expect(page.getByRole('link', { name: 'E2E Channel › CDN' }).first()).toBeVisible();
  await expect(page.getByRole('list', { name: /Hourly availability of E2E Channel › CDN/ }).getByRole('listitem')).toHaveCount(169);
  const outage = page.getByRole('row').filter({ hasText: 'Timeout' }).last();
  await expect(outage).toContainText('E2E Channel › CDN');
  await expect(outage).toContainText('15 min');
  const info = page.getByRole('button', { name: 'About hourly availability' });
  await info.click();
  await expect(page.getByRole('note')).toContainText('Grey: no checks that hour');
  await info.press('Escape');
  await expect(page.getByRole('note')).toBeHidden();

  // A Dashboard incident on this host links straight to its history: Reports filtered to it and opened.
  const { MongoClient: Client } = await import('mongodb');
  const m2 = new Client(process.env['MONGO_URI']!);
  await m2.db('gate_test_e2e').collection('pipeline_incidents').insertOne({ key: `node:${serverIp}:443`, kind: 'CONVICT', title: `CDN (${serverIp}:443) is down`, channels: ['E2E Channel'], openedAt: new Date(), resolvedAt: null, open: true });
  await m2.close();
  await page.getByRole('navigation', { name: 'Main navigation' }).getByRole('link', { name: 'Dashboard', exact: true }).click();
  await page.getByRole('link', { name: 'Server history' }).first().click();
  await expect(page).toHaveURL(/\/reports\?hours=168&server=e2e-origin\.test/);
  await expect(page.getByRole('list', { name: /Hourly availability of E2E Channel › CDN/ })).toBeVisible();

  // The navbar bell counts the alert and lists it; opening the list marks it seen.
  await page.reload();
  const bell = page.getByRole('button', { name: /Notifications, [1-9]\d* unread/ });
  await expect(bell).toBeVisible();
  await bell.click();
  await expect(page.getByRole('region', { name: 'Notifications' })).toContainText(`CDN (${serverIp}:443) is down`);
  await expect(page.getByRole('button', { name: 'Notifications, 0 unread' })).toBeVisible();
});

test('DLQ: whole-history counts, grouped repeats, server-side search, Load older and a confirmed replay', async ({ page }) => {
  await import('dotenv/config');
  const { MongoClient } = await import('mongodb');
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const col = mongo.db('gate_test_e2e').collection('hls_processing_failures');
  const at = (i: number) => new Date(Date.now() - 60_000 - i * 1000); // older than the feed's 5 s settle window
  const failure = (i: number, over: Record<string, unknown> = {}) => ({
    originalStreamKey: 'hls:streams:10.9.9.9', originalMessageId: `${i}-0`, groupName: 'hls-workers', serverIp: '10.9.9.9', consumerName: 'c', rawFields: [],
    failureId: `e2e-f${i}`, failureStage: 'PERSISTENCE', errorCode: `ERR_${i}`, errorMessage: `write failed ${i}`, attempts: 5,
    firstFailedAt: at(i).toISOString(), lastFailedAt: at(i).toISOString(), updatedAt: at(i).toISOString(), retryable: true, bounded: true,
    generation: 1, revision: 1, resolution: 'OPEN', streamId: `e2e-s${i}`, projectedAt: at(i), ...over,
  });
  await col.insertMany([
    ...[0, 1, 2].map(i => failure(i, { errorCode: 'MONGO_TIMEOUT', streamId: 'repeat-stream', errorMessage: 'database write timed out' })),
    ...Array.from({ length: 102 }, (_, i) => failure(i + 3)),
  ]);
  await mongo.close();

  await signIn(page, '/dlq');
  await expect(page.getByLabel('Failure counts, whole history')).toContainText('105');
  await expect(page.getByRole('row').filter({ hasText: 'repeat-stream' }).first()).toContainText('Show 3'); // three repeats grouped in one row
  await expect(page.getByRole('button', { name: 'Load older failures' })).toBeVisible(); // 105 open > first page of 100
  await page.getByRole('button', { name: 'Load older failures' }).click();
  await expect(page.getByText('e2e-s104')).toBeVisible();

  await page.getByLabel('Search failures').fill('ERR_57');
  await expect(page.getByRole('row').filter({ hasText: 'e2e-s57' })).toHaveCount(1);
  await expect(page.getByText('e2e-s58')).toHaveCount(0);

  await page.getByRole('row').filter({ hasText: 'e2e-s57' }).getByRole('button', { name: 'Details' }).click();
  await page.getByRole('button', { name: 'Replay…' }).click();
  await expect(page.getByText('Process this message again?')).toBeVisible();
  await page.getByRole('button', { name: 'Replay message' }).click();
  // Seeded only in MongoDB, so Redis has no dead-lettered entry: the server refuses instead of guessing.
  await expect(page.getByText(/no longer waiting in the dead-letter queue/)).toBeVisible();
});

test('Account: the background alerts card checks the device and reports the outcome of turning alerts on', async ({ page, context }) => {
  // Automated (Playwright) Chrome cannot register with Google's push service: subscribe() fails with
  // "Registration failed". So this checks the card and that the outcome is reported either way; real delivery
  // is covered by tests/integration/web-push.test.ts and a manual check in a normal browser.
  await context.grantPermissions(['notifications'], { origin: 'http://127.0.0.1:3200' });
  await signIn(page, '/account');
  await expect(page.getByRole('heading', { name: 'Background alerts' })).toBeVisible();
  const status = page.getByRole('status').filter({ hasText: /device|browser/ });
  await expect(status).toContainText('Alerts are not sent to this device.');
  await page.getByRole('button', { name: 'Turn on background alerts' }).click();
  await expect(page.getByText(/Background alerts are on for this device|Could not change background alerts/)).toBeVisible({ timeout: 15_000 });
});

test('phone (390px): no page scrolls sideways, and the menu button opens the sidebar as a drawer', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await signIn(page);
  for (const url of ['/', '/workspace', '/create', '/reports?hours=168', '/dlq', '/account']) {
    await page.goto(url);
    await expect(page.locator('#main-content')).toBeVisible();
    await page.waitForTimeout(800); // let the page's data render
    const { scrollWidth, clientWidth } = await page.evaluate(() => ({ scrollWidth: document.documentElement.scrollWidth, clientWidth: document.documentElement.clientWidth }));
    expect(scrollWidth, `${url} must not scroll sideways`).toBeLessThanOrEqual(clientWidth);
  }
  const reports = page.getByRole('navigation', { name: 'Main navigation' }).getByRole('link', { name: 'Reports', exact: true });
  await expect(reports).toBeHidden(); // the sidebar is off-screen until asked for
  await page.getByRole('button', { name: 'Menu' }).click();
  await expect(reports).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(reports).toBeHidden();
  await page.getByRole('button', { name: 'Menu' }).click();
  await reports.click();
  await expect(page).toHaveTitle('Reports | HLS monitor');
  await expect(reports).toBeHidden(); // navigating closes the drawer
  // Notifications fit the screen.
  await page.getByRole('button', { name: /Notifications/ }).click();
  const box = await page.getByRole('region', { name: 'Notifications' }).boundingBox();
  expect(box!.x).toBeGreaterThanOrEqual(0); expect(box!.x + box!.width).toBeLessThanOrEqual(390);
});
