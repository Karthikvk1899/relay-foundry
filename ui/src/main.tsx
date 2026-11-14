import React, { useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Delivery = { id: string; event_id: string; endpoint_id: string; status: string; attempts: number; last_status: number | null; last_error: string | null; created_at: string };
type Endpoint = { id: string; url: string; enabled: boolean };

function App() {
  const [key, setKey] = useState('');
  const [deliveries, setDeliveries] = useState<Delivery[]>([]);
  const [endpoints, setEndpoints] = useState<Endpoint[]>([]);
  const [filter, setFilter] = useState('');
  const [error, setError] = useState('');
  const [detail, setDetail] = useState('');
  const [url, setUrl] = useState('');
  const [eventType, setEventType] = useState('order.created');
  const [payload, setPayload] = useState('{"order_id":"demo-1"}');

  async function request(path: string, options: RequestInit = {}) {
    const response = await fetch(path, { ...options, headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json', ...options.headers } });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
    return body;
  }
  async function refresh() {
    try {
      const [d, e] = await Promise.all([request(`/v1/deliveries${filter ? `?status=${filter}` : ''}`), request('/v1/endpoints')]);
      setDeliveries(d); setEndpoints(e); setError('');
    } catch (err) { setError(String(err)); }
  }
  async function act(task: () => Promise<unknown>) {
    try { await task(); setError(''); await refresh(); } catch (err) { setError(String(err)); }
  }
  return <main>
    <header><div><span className="eyebrow">EVENT DELIVERY CONTROL PLANE</span><h1>Relay Foundry</h1><p>Inspect every delivery, investigate failures, and replay when ready.</p></div><div className="pulse">● System console</div></header>
    <section className="auth"><label>Tenant API key<input type="password" value={key} onChange={e => setKey(e.target.value)} placeholder="Paste your tenant credential" autoComplete="off" /></label><button onClick={refresh}>Connect / refresh</button></section>
    {error && <div role="alert" className="error">{error}</div>}
    <section className="stats"><div><strong>{deliveries.length}</strong><span>Recent deliveries</span></div><div><strong>{deliveries.filter(x => x.status === 'delivered').length}</strong><span>Delivered in view</span></div><div><strong>{deliveries.filter(x => x.status === 'failed').length}</strong><span>Failed in view</span></div><div><strong>{endpoints.length}</strong><span>Endpoints</span></div></section>
    <div className="grid"><section className="panel"><h2>Send an event</h2><label>Type<input value={eventType} onChange={e => setEventType(e.target.value)} /></label><label>JSON data<textarea value={payload} onChange={e => setPayload(e.target.value)} rows={5} /></label><button onClick={() => act(async () => { await request('/v1/events', { method: 'POST', headers: { 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ type: eventType, data: JSON.parse(payload) }) }); })}>Publish event</button></section>
    <section className="panel"><h2>Endpoints</h2><label>HTTPS destination<input value={url} onChange={e => setUrl(e.target.value)} placeholder="https://example.com/webhooks" /></label><button onClick={() => act(async () => { const created = await request('/v1/endpoints', { method: 'POST', body: JSON.stringify({ url }) }); setDetail(`Save this signing secret now; it is shown once: ${created.signing_secret}`); setUrl(''); })}>Create endpoint</button><ul>{endpoints.map(e => <li key={e.id}>{e.url}</li>)}</ul>{detail && <p className="notice">{detail}</p>}</section></div>
    <section className="panel"><div className="tablehead"><h2>Delivery ledger</h2><label>Status <select value={filter} onChange={e => setFilter(e.target.value)}><option value="">All</option><option>pending</option><option>leased</option><option>delivered</option><option>failed</option></select></label></div><div className="scroll"><table><thead><tr><th>Delivery</th><th>Status</th><th>Attempts</th><th>HTTP</th><th>Last error</th><th>Created</th><th>Action</th></tr></thead><tbody>{deliveries.map(d => <tr key={d.id}><td><button className="link" onClick={() => act(async () => { const result = await request(`/v1/deliveries/${d.id}`); setDetail(JSON.stringify(result.attempt_history, null, 2)); })}>{d.id.slice(0, 8)}…</button></td><td><span className={`badge ${d.status}`}>{d.status}</span></td><td>{d.attempts}</td><td>{d.last_status ?? '—'}</td><td>{d.last_error ?? '—'}</td><td>{new Date(d.created_at).toLocaleString()}</td><td>{d.status === 'failed' && <button className="small" onClick={() => act(() => request(`/v1/deliveries/${d.id}/replay`, { method: 'POST' }))}>Replay</button>}</td></tr>)}</tbody></table></div>{deliveries.length === 0 && <p className="empty">No deliveries in this view. Connect with a tenant key or publish an event.</p>}</section>
    <footer>At-least-once delivery · Verify signatures and deduplicate event IDs at the receiver.</footer>
  </main>;
}
createRoot(document.getElementById('root')!).render(<React.StrictMode><App /></React.StrictMode>);
