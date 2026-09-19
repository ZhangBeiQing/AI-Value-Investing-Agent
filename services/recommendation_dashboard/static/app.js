const state = { dashboard: null, holdings: null, jobs: [], openJobId: null, filter: 'all', query: '', sort: 'impression', selected: null, chatTimer: null };
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

function latestJob(symbol) { return state.jobs.find((job) => job.symbol === symbol); }
function isActiveJob(job) { return job && !['complete', 'failed', 'cancelled'].includes(job.status); }
function isCancellableJob(job) { return isActiveJob(job) && !['publishing', 'cancelling'].includes(job.status); }
function jobLabel(job) { return ({ queued: '排队中', preparing_data: '准备数据', financial_research: '财报深研中', building_research: '生成研究包', debating: 'AI 辩论中', validating: '校验结果', publishing: '写入正式决策', cancelling: '取消中', cancelled: '已取消', complete: '分析已完成', failed: '分析失败' })[job.status] || job.status; }

async function postJson(path, payload) {
  const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}

async function startResearch(symbol) {
  try {
    const job = await postJson('/api/jobs', { symbol });
    $('addStockMessage').textContent = `${symbol} 已提交研究任务：${jobLabel(job)}`;
    await loadJobs();
  } catch (error) { $('addStockMessage').textContent = `无法启动研究：${error.message}`; }
}

async function cancelResearch(job) {
  if (!isCancellableJob(job) || !window.confirm(`确定取消 ${job.symbol} 的本次研究任务吗？`)) return;
  try {
    const updated = await postJson(`/api/jobs/${encodeURIComponent(job.id)}/cancel`, {});
    const index = state.jobs.findIndex((item) => item.id === job.id);
    if (index >= 0) state.jobs[index] = updated;
    if (state.openJobId === job.id) await openJob(job.id, { refresh: true });
    else { renderJobs(); if (state.dashboard) renderTable(); }
  } catch (error) { $('addStockMessage').textContent = `取消任务失败：${error.message}`; }
}

function renderJobs() {
  const list = $('researchJobs'); list.replaceChildren();
  for (const job of state.jobs.slice(0, 12)) {
    const item = document.createElement('div'); item.className = 'job-item';
    const identity = document.createElement('span');
    identity.append(text('span', job.stock_name || job.symbol, 'stock-name'));
    identity.append(text('span', ` ${job.symbol} · ${job.date} · ${job.message || jobLabel(job)}`, 'job-identity-detail'));
    item.append(identity);
    const actions = document.createElement('div');
    actions.append(text('span', jobLabel(job), `job-status ${job.status} ${isActiveJob(job) ? 'active' : ''}`));
    const open = text('button', '查看进度 →', 'inline-action'); open.type = 'button';
    open.addEventListener('click', () => openJob(job.id)); actions.append(open);
    if (isCancellableJob(job)) {
      const cancel = text('button', '取消任务', 'inline-action cancel-action'); cancel.type = 'button';
      cancel.addEventListener('click', () => cancelResearch(job)); actions.append(cancel);
    }
    item.append(actions); list.append(item);
  }
}

async function loadJobs({ render = true } = {}) {
  try {
    const previous = new Map(state.jobs.map((job) => [job.id, job.status]));
    const response = await fetch('/api/jobs');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    state.jobs = (await response.json()).jobs || [];
    if (render) {
      renderJobs();
      if (state.dashboard) renderTable();
    }
    if (state.jobs.some((job) => job.status === 'complete' && previous.has(job.id) && previous.get(job.id) !== 'complete')) load();
  } catch (error) { $('addStockMessage').textContent = `任务状态读取失败：${error.message}`; }
}

async function openJob(jobId, { refresh = false } = {}) {
  state.openJobId = jobId;
  $('drawerBackdrop').hidden = false; $('drawer').hidden = false; document.body.style.overflow = 'hidden';
  const body = $('drawerBody');
  const savedScrollTop = body.scrollTop;
  const wasNearBottom = body.scrollHeight - body.clientHeight - body.scrollTop < 80;
  const oldLog = body.querySelector('.job-log');
  const oldLogSection = oldLog?.closest('.detail-section');
  const oldLogOffset = oldLogSection ? oldLogSection.getBoundingClientRect().top - body.getBoundingClientRect().top : null;
  const oldLogScrollTop = oldLog?.scrollTop || 0;
  const oldLogNearBottom = oldLog ? oldLog.scrollHeight - oldLog.clientHeight - oldLog.scrollTop < 40 : false;
  if (!refresh) {
    $('drawerTitle').textContent = '单股研究进度'; $('drawerSubtitle').textContent = '与正式交易账本隔离';
    body.replaceChildren(text('div', '正在读取任务…', 'detail-empty'));
  }
  try {
    const response = await fetch(`/api/jobs/${encodeURIComponent(jobId)}/result`);
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
    if (state.openJobId !== jobId || $('drawer').hidden) return;
    body.replaceChildren();
    $('drawerTitle').textContent = `${result.job.stock_name || result.job.symbol} · ${result.job.symbol} · ${jobLabel(result.job)}`;
    $('drawerSubtitle').textContent = `${result.job.date} · ${result.job.message}`;
    body.append(text('p', result.job.status === 'complete'
      ? '已并入正式决策与 Agent 虚拟账本；真实持仓仍只由你手动维护。'
      : result.job.status === 'cancelled'
        ? '任务已取消；中间产物没有发布，也没有写入 Agent 虚拟账本。'
        : '研究进行中；完成校验后才会合入正式决策与 Agent 虚拟账本。', 'job-note'));
    if (isCancellableJob(result.job)) {
      const cancel = text('button', '取消本次分析', 'secondary-button cancel-action'); cancel.type = 'button';
      cancel.addEventListener('click', () => cancelResearch(result.job)); body.append(cancel);
    }
    if (result.research) {
      const block = section('本次单股研究包', body);
      const button = text('button', '展开研究包', 'secondary-button'); button.type = 'button';
      const pre = text('pre', result.research, 'research-content'); pre.hidden = true;
      button.addEventListener('click', () => { pre.hidden = !pre.hidden; button.textContent = pre.hidden ? '展开研究包' : '收起研究包'; });
      block.append(button, pre);
    }
    const stages = result.stages || {};
    stage(body, 'Bull · 正方论点', stages.bull, 'arguments');
    stage(body, 'Bear · 反方论点', stages.bear, 'arguments');
    stage(body, 'Bull · 对反方的反驳', stages.bull_rebuttal, 'rebuttals');
    stage(body, 'Bear · 对正方的反驳', stages.bear_rebuttal, 'rebuttals');
    for (const [key, label] of [['juror_01', '裁判 1'], ['juror_02', '裁判 2'], ['juror_03', '裁判 3']]) stage(body, label, stages[key]);
    stage(body, '投票汇总', stages.vote_summary);
    if (result.verdict) stage(body, 'Final · 独立研究裁决', result.verdict);
    const log = section('任务日志', body);
    const logContent = text('pre', result.job.log_tail || '暂无日志', 'job-log');
    log.append(logContent);
    if (refresh) {
      if (wasNearBottom) body.scrollTop = body.scrollHeight;
      else if (oldLogOffset != null) {
        const newLogOffset = log.getBoundingClientRect().top - body.getBoundingClientRect().top;
        body.scrollTop = savedScrollTop + newLogOffset - oldLogOffset;
      } else body.scrollTop = savedScrollTop;
      logContent.scrollTop = oldLogNearBottom ? logContent.scrollHeight : oldLogScrollTop;
    }
  } catch (error) {
    if (!refresh) body.replaceChildren(text('div', `任务读取失败：${error.message}`, 'detail-empty'));
  }
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
    row.addEventListener('click', () => item.source === 'on_demand' ? openJob(item.job_id) : openDetail(item));
    row.addEventListener('keydown', (event) => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); item.source === 'on_demand' ? openJob(item.job_id) : openDetail(item); } });
    const stock = document.createElement('td');
    stock.append(text('div', item.stock_name, 'stock-name'), text('div', item.symbol, 'stock-code'));
    if (item.source === 'on_demand') stock.append(text('div', '独立研究历史记录', 'job-note'));
    const job = latestJob(item.symbol);
    if (job) stock.append(text('div', jobLabel(job), `job-status ${job.status} ${isActiveJob(job) ? 'active' : ''}`));
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
    const detailCell = document.createElement('td');
    detailCell.append(text('span', '查看 →', 'detail-link'));
    if (job) {
      const jobButton = text('button', isActiveJob(job) ? '查看进度' : '查看独立结果', 'inline-action');
      jobButton.type = 'button';
      jobButton.addEventListener('keydown', (event) => event.stopPropagation());
      jobButton.addEventListener('click', (event) => { event.stopPropagation(); openJob(job.id); });
      detailCell.append(document.createElement('br'), jobButton);
    }
    const analyzeButton = text('button', isActiveJob(job) ? '分析中' : '重新分析', 'inline-action');
    analyzeButton.type = 'button'; analyzeButton.disabled = Boolean(isActiveJob(job));
    analyzeButton.addEventListener('keydown', (event) => event.stopPropagation());
    analyzeButton.addEventListener('click', (event) => { event.stopPropagation(); startResearch(item.symbol); });
    detailCell.append(document.createElement('br'), analyzeButton);
    row.append(detailCell);
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

async function showResearch(symbol, day, output) {
  output.textContent = '正在读取研究包…';
  try {
    const response = await fetch(`/api/research/${encodeURIComponent(symbol)}?date=${encodeURIComponent(day)}`);
    const packageData = await response.json();
    if (!response.ok) throw new Error(packageData.error || `HTTP ${response.status}`);
    output.textContent = packageData.content;
  } catch (error) {
    output.textContent = `研究包读取失败：${error.message}`;
  }
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
  const recentJob = latestJob(detail.symbol);
  if (recentJob) {
    const independent = section('最新独立研究任务', body, `${recentJob.date} · ${jobLabel(recentJob)}`);
    independent.append(text('p', recentJob.message || '点击查看进度与独立研究结果', 'job-note'));
    const button = text('button', '查看任务进度与结果', 'secondary-button'); button.type = 'button';
    button.addEventListener('click', () => openJob(recentJob.id)); independent.append(button);
  }
  const research = section('逐股研究包', body, '研究包日期与最新交易决策日期可能不同');
  if (detail.research_packages?.length) {
    const controls = document.createElement('div'); controls.className = 'research-controls';
    const select = document.createElement('select'); select.setAttribute('aria-label', '选择研究包日期');
    for (const item of detail.research_packages) {
      const option = document.createElement('option'); option.value = item.date; option.textContent = item.date; select.append(option);
    }
    const button = text('button', '打开研究包', 'secondary-button'); button.type = 'button';
    const output = document.createElement('pre'); output.className = 'research-content'; output.hidden = true;
    button.addEventListener('click', () => { output.hidden = false; showResearch(detail.symbol, select.value, output); });
    controls.append(select, button); research.append(controls, output);
  } else research.append(text('div', '暂无该股票的研究包', 'detail-empty'));
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
  renderChat(detail.symbol, body);
}

async function renderChat(symbol, body) {
  const block = section('与 OpenCode 继续讨论', body, '自动带入最新决策、辩论、最近两份研究包及分析日组合输入；仅问答，不改动交易文件');
  const controls = document.createElement('div'); controls.className = 'chat-controls';
  const model = document.createElement('select'); model.setAttribute('aria-label', 'OpenCode 模型');
  const send = text('button', '发送', 'secondary-button'); send.type = 'button';
  const reset = text('button', '新对话', 'secondary-button'); reset.type = 'button'; reset.title = '重新读取最新资料并开始新会话';
  controls.append(model, reset, send);
  const messages = document.createElement('div'); messages.className = 'chat-messages';
  const input = document.createElement('textarea'); input.className = 'chat-input'; input.placeholder = '围绕这只股票的结论继续提问…'; input.rows = 3;
  const status = text('div', '正在读取对话…', 'job-note');
  block.append(controls, messages, input, status);
  let busy = false;
  async function refresh() {
    if (state.selected !== symbol || !block.isConnected) return;
    try {
      const response = await fetch(`/api/chat/${encodeURIComponent(symbol)}`);
      const chat = await response.json();
      if (!response.ok) throw new Error(chat.error || `HTTP ${response.status}`);
      messages.replaceChildren();
      for (const item of chat.messages || []) {
        const entry = document.createElement('div'); entry.className = `chat-message ${item.role}`;
        entry.append(text('div', item.role === 'user' ? '你' : 'OpenCode', 'chat-role'), text('div', item.content, 'chat-content'));
        messages.append(entry);
      }
      busy = chat.status === 'running'; send.disabled = busy; model.disabled = busy; reset.disabled = busy;
      status.textContent = busy ? 'OpenCode 正在回答…' : chat.error ? `上次回答失败：${chat.error}` : '会话保存在本机；切换模型会保留本股对话历史。';
    } catch (error) { status.textContent = `对话读取失败：${error.message}`; }
    if (busy) state.chatTimer = setTimeout(refresh, 2000);
  }
  send.addEventListener('click', async () => {
    const message = input.value.trim(); if (!message || busy) return;
    send.disabled = true; status.textContent = '正在提交…';
    try {
      await postJson(`/api/chat/${encodeURIComponent(symbol)}`, { message, model: model.value });
      input.value = ''; await refresh();
    } catch (error) { status.textContent = `发送失败：${error.message}`; send.disabled = false; }
  });
  reset.addEventListener('click', async () => {
    if (busy || !window.confirm('开始新对话？当前本股聊天记录将从页面移除，新问题会重新读取最新资料。')) return;
    try { await postJson(`/api/chat/${encodeURIComponent(symbol)}/reset`, {}); await refresh(); }
    catch (error) { status.textContent = `新建对话失败：${error.message}`; }
  });
  input.addEventListener('keydown', (event) => { if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) { event.preventDefault(); send.click(); } });
  try {
    const response = await fetch('/api/chat/models');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
    const chatResponse = await fetch(`/api/chat/${encodeURIComponent(symbol)}`);
    const chat = await chatResponse.json();
    for (const name of data.models || []) { const option = document.createElement('option'); option.value = name; option.textContent = name; model.append(option); }
    model.value = (data.models || []).includes(chat.model) ? chat.model : ((data.models || [])[0] || '');
    if (!model.value) { send.disabled = true; status.textContent = '未找到 OpenCode 可用模型'; return; }
    await refresh();
  } catch (error) { send.disabled = true; status.textContent = `模型读取失败：${error.message}`; }
}

async function openDetail(row) {
  state.selected = row.symbol;
  state.openJobId = null;
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
  if (state.chatTimer) clearTimeout(state.chatTimer);
  state.selected = null;
  state.openJobId = null;
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
$('addStockForm').addEventListener('submit', async (event) => {
  event.preventDefault();
  const name = $('newStockName').value.trim();
  const message = $('addStockMessage'); message.replaceChildren(text('span', '正在匹配股票名称…'));
  try {
    const response = await fetch(`/api/universe/resolve?name=${encodeURIComponent(name)}`);
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
    const candidates = data.candidates || [];
    if (!candidates.length) { message.textContent = '本地股票名称映射暂无匹配，请先补充准确的名称—代码映射。'; return; }
    const choose = async (candidate) => {
      message.textContent = `已匹配 ${candidate.name} ${candidate.symbol}，正在添加并启动…`;
      await postJson('/api/universe', { symbol: candidate.symbol, name: candidate.name });
      await startResearch(candidate.symbol);
    };
    if (candidates.length === 1) { await choose(candidates[0]); return; }
    message.textContent = '找到多个同名证券，请选择：';
    for (const candidate of candidates) {
      const button = text('button', `${candidate.name} · ${candidate.symbol}`, 'secondary-button'); button.type = 'button';
      button.addEventListener('click', () => choose(candidate).catch((error) => { message.textContent = error.message; }));
      message.append(button);
    }
  } catch (error) { message.textContent = `添加股票失败：${error.message}`; }
});
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
loadJobs();
setInterval(async () => {
  if (!state.jobs.some(isActiveJob)) return;
  const drawerOpen = !$('drawer').hidden;
  await loadJobs({ render: !drawerOpen });
  if (state.openJobId && drawerOpen) openJob(state.openJobId, { refresh: true });
}, 5000);
