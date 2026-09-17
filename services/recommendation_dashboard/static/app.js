const state = { dashboard: null, holdings: null, filter: 'all', query: '', sort: 'impression', selected: null };
const $ = (id) => document.getElementById(id);
const priceImpressionOrder = ['明显低估', '偏低估', '合理偏低估', '合理', '合理偏贵', '偏贵', '明显高估', '泡沫'];

function priceImpressionRank(value) {
  const rank = priceImpressionOrder.indexOf(value);
  return rank === -1 ? priceImpressionOrder.length : rank;
}

function text(tag, value, className = '') {
  const node = document.createElement(tag);
  node.textContent = value == null || value === '' ? '—' : String(value);
  if (className) node.className = className;
  return node;
}

function pct(value) {
  if (value == null) return '—';
  return `${value > 0 ? '+' : ''}${Number(value).toFixed(2)}%`;
}

function number(value, digits = 2) {
  return value == null ? '—' : Number(value).toLocaleString('zh-CN', { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function tone(value) { return value == null ? 'muted' : value > 0 ? 'positive' : value < 0 ? 'negative' : ''; }

function actionLabel(action) {
  return ({ BUY: '买入', SELL: '卖出', HOLD: '持有', FLAT: '空仓' })[action] || action || '—';
}

function fillSummary(data) {
  $('asOf').textContent = `数据观察日 ${data.as_of_date}`;
  $('totalReturn').textContent = pct(data.equal_weight_return_pct);
  $('totalReturn').className = `metric-value ${tone(data.equal_weight_return_pct)}`;
  $('analyzedCount').textContent = String(data.analyzed_count);
  $('buyCount').textContent = String(data.buy_stock_count);
  $('buyEvents').textContent = `${data.buy_event_count} 次股票级 BUY 记录`;
  $('winLoss').textContent = `${data.winning_stock_count} / ${data.losing_stock_count}`;
  $('priceCoverage').textContent = `${data.priced_buy_stock_count} / ${data.buy_stock_count} 只有效价格`;
}

function visibleRows() {
  if (!state.dashboard) return [];
  let rows = state.dashboard.stocks.filter((row) => {
    if (state.filter === 'buy' && !row.has_buy) return false;
    if (state.filter === 'other' && row.has_buy) return false;
    return `${row.stock_name} ${row.symbol}`.toLowerCase().includes(state.query);
  });
  rows = [...rows];
  const byReturn = (a, b, direction) => {
    if (a.return_pct == null) return b.return_pct == null ? a.symbol.localeCompare(b.symbol) : 1;
    if (b.return_pct == null) return -1;
    return direction * (a.return_pct - b.return_pct) || a.symbol.localeCompare(b.symbol);
  };
  if (state.sort === 'impression') rows.sort((a, b) => priceImpressionRank(a.price_impression) - priceImpressionRank(b.price_impression) || b.latest_analysis_date.localeCompare(a.latest_analysis_date) || a.symbol.localeCompare(b.symbol));
  else if (state.sort === 'return_desc') rows.sort((a, b) => byReturn(a, b, -1));
  else if (state.sort === 'return_asc') rows.sort((a, b) => byReturn(a, b, 1));
  else if (state.sort === 'analysis') rows.sort((a, b) => b.latest_analysis_date.localeCompare(a.latest_analysis_date) || a.symbol.localeCompare(b.symbol));
  else if (state.sort === 'name') rows.sort((a, b) => a.symbol.localeCompare(b.symbol));
  else rows.sort((a, b) => Number(b.has_buy) - Number(a.has_buy) || byReturn(a, b, -1));
  return rows;
}

function renderTable() {
  const body = $('stocksBody');
  body.replaceChildren();
  const rows = visibleRows();
  $('resultCount').textContent = `显示 ${rows.length} 只 · 历史决策 ${state.dashboard?.decision_count || 0} 条`;
  if (!rows.length) {
    const row = document.createElement('tr');
    const cell = text('td', '没有符合条件的股票', 'empty-cell');
    cell.colSpan = 11;
    row.append(cell); body.append(row); return;
  }
  for (const item of rows) {
    const row = document.createElement('tr');
    row.tabIndex = 0;
    row.setAttribute('aria-label', `查看${item.stock_name}的最新分析与辩论`);
    row.addEventListener('click', () => openDetail(item));
    row.addEventListener('keydown', (event) => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openDetail(item); } });
    const stock = document.createElement('td');
    stock.append(text('div', item.stock_name, 'stock-name'), text('div', item.symbol, 'stock-code'));
    row.append(stock);
    const action = document.createElement('td');
    action.append(text('span', actionLabel(item.latest_action), `badge ${(item.latest_action || 'flat').toLowerCase()}`));
    row.append(action);
    row.append(text('td', item.latest_analysis_date || '—'));
    row.append(text('td', item.price_impression || '—'));
    row.append(text('td', item.first_buy_date || '—'));
    row.append(text('td', item.has_buy ? item.buy_count : '—'));
    row.append(text('td', number(item.buy_close), 'number'));
    row.append(text('td', number(item.latest_close), 'number'));
    row.append(text('td', pct(item.return_pct), `number return-cell ${tone(item.return_pct)}`));
    const priceDate = document.createElement('td');
    if (item.price_date && !item.price_is_latest_for_market) {
      const dot = document.createElement('span'); dot.className = 'stale-dot'; dot.title = '该价格落后于同市场的最新缓存'; priceDate.append(dot);
    }
    priceDate.append(text('span', item.price_date || '无缓存', 'price-date'));
    row.append(priceDate);
    row.append(text('td', '查看 →', 'detail-link'));
    body.append(row);
  }
}

function holdingsMoney(value, currency) {
  if (value == null) return '—';
  const prefix = currency === 'HKD' ? 'HK$' : '¥';
  return `${value < 0 ? '-' : ''}${prefix}${number(Math.abs(value))}`;
}

function renderHoldings(data) {
  $('holdingsStatus').textContent = `人工持仓文件日期 ${data.source_as_of_date || '未注明'} · ${data.priced_count} / ${data.position_count} 只已定价`;
  $('cashCny').textContent = holdingsMoney(data.cash_cny, 'CNY');
  for (const [id, value, currency] of [
    ['cnMarketValue', data.totals.CNY.market_value, 'CNY'],
    ['cnPnl', data.totals.CNY.unrealized_pnl, 'CNY'],
    ['hkMarketValue', data.totals.HKD.market_value, 'HKD'],
    ['hkPnl', data.totals.HKD.unrealized_pnl, 'HKD'],
  ]) {
    $(id).textContent = holdingsMoney(value, currency);
    $(id).classList.toggle('positive', id.endsWith('Pnl') && value > 0);
    $(id).classList.toggle('negative', id.endsWith('Pnl') && value < 0);
  }
  $('holdingsCount').textContent = `持仓 ${data.position_count} 只 · 股数与成本来自人工持仓文件`;
  const body = $('holdingsBody'); body.replaceChildren();
  if (!data.positions.length) {
    const row = document.createElement('tr'); const cell = text('td', '当前没有持仓记录', 'empty-cell'); cell.colSpan = 9; row.append(cell); body.append(row); return;
  }
  for (const item of data.positions) {
    const row = document.createElement('tr');
    const research = state.dashboard?.stocks.find((stock) => stock.symbol === item.symbol);
    if (research) {
      row.tabIndex = 0;
      row.setAttribute('aria-label', `查看${item.stock_name}的最新分析与辩论`);
      row.addEventListener('click', () => openDetail(research));
      row.addEventListener('keydown', (event) => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openDetail(research); } });
    }
    const stock = document.createElement('td');
    stock.append(text('div', item.stock_name, 'stock-name'), text('div', item.symbol || '代码未匹配', 'stock-code'));
    row.append(stock);
    row.append(text('td', item.currency));
    row.append(text('td', Number(item.shares).toLocaleString('zh-CN'), 'number'));
    row.append(text('td', number(item.avg_cost, 3), 'number'));
    row.append(text('td', number(item.latest_close), 'number'));
    row.append(text('td', holdingsMoney(item.market_value, item.currency), 'number'));
    row.append(text('td', holdingsMoney(item.unrealized_pnl, item.currency), `number return-cell ${tone(item.unrealized_pnl)}`));
    row.append(text('td', pct(item.unrealized_pct), `number return-cell ${tone(item.unrealized_pct)}`));
    row.append(text('td', item.price_date || '无缓存', 'price-date'));
    body.append(row);
  }
}

async function loadHoldings() {
  $('refreshHoldingsBtn').disabled = true;
  $('refreshHoldingsBtn').textContent = '刷新中…';
  try {
    const response = await fetch('/api/holdings');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    state.holdings = await response.json();
    renderHoldings(state.holdings);
  } catch (error) {
    $('holdingsStatus').textContent = `持仓读取失败：${error.message}`;
  } finally {
    $('refreshHoldingsBtn').disabled = false;
    $('refreshHoldingsBtn').innerHTML = '刷新数据 <span aria-hidden="true">↗</span>';
  }
}

function switchView(view) {
  $('analysisView').hidden = view !== 'analysis';
  $('holdingsView').hidden = view !== 'holdings';
  for (const button of document.querySelectorAll('[data-view]')) button.classList.toggle('active', button.dataset.view === view);
  if (view === 'holdings' && !state.holdings) loadHoldings();
}

function section(title, parent, dateText = '') {
  const node = document.createElement('section');
  node.className = 'detail-section';
  node.append(text('h3', title));
  if (dateText) node.append(text('p', dateText, 'section-date'));
  parent.append(node);
  return node;
}

function renderContent(parent, value) {
  if (value == null || value === '') { parent.append(text('div', '暂无这部分记录', 'detail-empty')); return; }
  if (Array.isArray(value)) {
    if (!value.length) { parent.append(text('div', '暂无这部分记录', 'detail-empty')); return; }
    const list = document.createElement('ol'); list.className = 'detail-list';
    for (const item of value) { const li = document.createElement('li'); li.textContent = typeof item === 'object' ? JSON.stringify(item, null, 2) : String(item); list.append(li); }
    parent.append(list); return;
  }
  if (typeof value === 'object') {
    const pairs = document.createElement('dl'); pairs.className = 'detail-kv';
    for (const [key, item] of Object.entries(value)) {
      pairs.append(text('dt', ({ action_type: '动作', action_num: '数量', price_impression: '价格判断', reason: '裁判理由', confidence_score: '信心', recommended_action: '最终建议' })[key] || key));
      pairs.append(text('dd', typeof item === 'object' ? JSON.stringify(item, null, 2) : item));
    }
    parent.append(pairs); return;
  }
  parent.append(text('p', value, 'detail-paragraph'));
}

function stage(parent, title, data, field, dateText) {
  const node = section(title, parent, dateText);
  renderContent(node, field ? data?.[field] : data);
}

function renderDetail(detail, row) {
  const body = $('drawerBody'); body.replaceChildren();
  $('drawerTitle').textContent = `${detail.stock_name} · ${detail.symbol}`;
  $('drawerSubtitle').textContent = `最新决策 ${detail.latest_decision_date} · 最新完整辩论 ${detail.debate_date || '暂无'}`;

  const metrics = document.createElement('div'); metrics.className = 'detail-metrics';
  for (const [label, value, className] of [
    ['首次 BUY 至今', pct(row.return_pct), tone(row.return_pct)],
    ['最新收盘', `${number(row.latest_close)} · ${row.price_date || '无日期'}`, ''],
    ['最近判断', actionLabel(row.latest_action), ''],
  ]) {
    const metric = document.createElement('div'); metric.className = 'detail-metric';
    metric.append(text('div', label, 'detail-metric-label'), text('div', value, `detail-metric-value ${className}`)); metrics.append(metric);
  }
  body.append(metrics);
  const decision = detail.decision || {};
  stage(body, '最新决策结论', decision.recommended_action, null, `分析日 ${detail.latest_decision_date}`);
  if (decision.court?.verdict) stage(body, '最终裁决与分歧', decision.court.verdict);

  const events = section(`历史 BUY 记录 · ${row.buy_count} 次`, body, '按各次 BUY 当日收盘价与该股最新可用收盘价比较');
  if (row.buy_events.length) {
    const table = document.createElement('table'); table.className = 'event-table';
    const head = document.createElement('thead'); const headRow = document.createElement('tr');
    for (const label of ['BUY 日期', '建议数量', '当日收盘', '至今涨跌']) headRow.append(text('th', label));
    head.append(headRow); table.append(head);
    const tbody = document.createElement('tbody');
    for (const event of row.buy_events) {
      const tr = document.createElement('tr');
      tr.append(text('td', event.date), text('td', event.quantity), text('td', number(event.anchor_close)), text('td', pct(event.return_pct), tone(event.return_pct)));
      tbody.append(tr);
    }
    table.append(tbody); events.append(table);
  } else events.append(text('div', '这只股票没有 BUY 记录', 'detail-empty'));

  const stages = detail.stages || {};
  const debateDate = detail.debate_date ? `辩论日 ${detail.debate_date}` : '';
  stage(body, 'Bull · 正方论点', stages.bull, 'arguments', debateDate);
  stage(body, 'Bear · 反方论点', stages.bear, 'arguments', debateDate);
  stage(body, 'Bull · 对反方的反驳', stages.bull_rebuttal, 'rebuttals', debateDate);
  stage(body, 'Bear · 对正方的反驳', stages.bear_rebuttal, 'rebuttals', debateDate);
  for (const [key, label] of [['juror_01', '裁判 1'], ['juror_02', '裁判 2'], ['juror_03', '裁判 3']]) stage(body, label, stages[key], null, debateDate);
  stage(body, '投票汇总', stages.vote_summary, null, debateDate);
  stage(body, 'Final · 最终结果', stages.final || decision, null, debateDate);
}

async function openDetail(row) {
  state.selected = row.symbol;
  $('drawerBackdrop').hidden = false;
  $('drawer').hidden = false;
  document.body.style.overflow = 'hidden';
  $('drawerTitle').textContent = `${row.stock_name} · ${row.symbol}`;
  $('drawerSubtitle').textContent = '正在读取最近一轮辩论…';
  $('drawerBody').replaceChildren(text('div', '正在读取详情…', 'detail-empty'));
  try {
    const response = await fetch(`/api/stocks/${encodeURIComponent(row.symbol)}`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const detail = await response.json();
    if (state.selected === row.symbol) renderDetail(detail, row);
  } catch (error) {
    $('drawerBody').replaceChildren(text('div', `详情读取失败：${error.message}`, 'detail-empty'));
  }
}

function closeDetail() {
  state.selected = null;
  $('drawerBackdrop').hidden = true; $('drawer').hidden = true;
  document.body.style.overflow = '';
}

async function load() {
  $('refreshBtn').disabled = true;
  $('refreshBtn').textContent = '刷新中…';
  try {
    const response = await fetch('/api/dashboard');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    state.dashboard = await response.json();
    fillSummary(state.dashboard);
    renderTable();
  } catch (error) {
    $('stocksBody').replaceChildren();
    const row = document.createElement('tr'); const cell = text('td', `数据读取失败：${error.message}`, 'empty-cell'); cell.colSpan = 11; row.append(cell); $('stocksBody').append(row);
    $('resultCount').textContent = '无法读取本地数据';
  } finally {
    $('refreshBtn').disabled = false; $('refreshBtn').innerHTML = '刷新数据 <span aria-hidden="true">↗</span>';
  }
}

$('refreshBtn').addEventListener('click', load);
$('refreshHoldingsBtn').addEventListener('click', loadHoldings);
for (const button of document.querySelectorAll('[data-view]')) button.addEventListener('click', () => switchView(button.dataset.view));
$('searchInput').addEventListener('input', (event) => { state.query = event.target.value.trim().toLowerCase(); renderTable(); });
$('sortSelect').addEventListener('change', (event) => { state.sort = event.target.value; renderTable(); });
for (const button of document.querySelectorAll('[data-filter]')) button.addEventListener('click', () => {
  state.filter = button.dataset.filter;
  for (const item of document.querySelectorAll('[data-filter]')) item.classList.toggle('active', item === button);
  renderTable();
});
$('closeDrawer').addEventListener('click', closeDetail);
$('drawerBackdrop').addEventListener('click', closeDetail);
document.addEventListener('keydown', (event) => { if (event.key === 'Escape' && !$('drawer').hidden) closeDetail(); });
load();
