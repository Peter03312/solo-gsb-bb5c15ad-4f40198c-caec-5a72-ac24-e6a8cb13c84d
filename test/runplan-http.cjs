'use strict';

/**
 * 定时走行计划 HTTP 集成测试：
 *   临时库启动真实服务 → 值班员开放进路并提交定时走行计划 →
 *   校验非法计划整体拒绝、提交返回（编号/状态/下一步/到期时刻）、到期自动按序
 *   注入走行事件并完成进路、人工事件抢先停止且不覆盖、重复触发不重发、
 *   本人终止仅停止注入不解除、非本人拒绝、同一实例唯一活动计划、
 *   与延时取消的两种相互作用、重启后继续并依序处理逾期步骤、副本不复制活动计划、
 *   重置清除。
 *
 * 运行：node test/runplan-http.cjs
 */
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const PORT = process.env.TEST_PORT || 3921;
const DB_PATH = process.env.SMOKE_DB_RUNPLAN || path.join(__dirname, '..', 'data', 'runplan-test.db');
const YARD_DB_PATH = path.join(path.dirname(DB_PATH),
  path.basename(DB_PATH, path.extname(DB_PATH)) + '.yards', 'yard-1.db');
const BASE = `http://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let passed = 0;
function ok(name, cond, extra) {
  if (!cond) throw new Error(name + (extra ? `（${JSON.stringify(extra)}）` : ''));
  console.log('  ✓', name);
  passed++;
}
function eq(name, actual, expected) {
  if (actual !== expected) {
    throw new Error(`${name}: 期望 ${JSON.stringify(expected)}，实际 ${JSON.stringify(actual)}`);
  }
  console.log('  ✓', name);
  passed++;
}

let server = null;
function startServer(keepDb = false) {
  return new Promise((resolve, reject) => {
    if (!keepDb) {
      for (const ext of ['', '-wal', '-shm']) {
        try { fs.unlinkSync(DB_PATH + ext); } catch (_) {}
      }
      fs.rmSync(path.join(path.dirname(DB_PATH),
        path.basename(DB_PATH, path.extname(DB_PATH)) + '.yards'), { recursive: true, force: true });
    }
    server = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'server.js')], {
      env: { ...process.env, PORT: String(PORT), DB_PATH, SEED_DEMO: '1' },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let out = '';
    const timer = setTimeout(() => reject(new Error('服务启动超时: ' + out)), 10000);
    server.stdout.on('data', (d) => {
      out += d.toString();
      if (out.includes('已启动')) { clearTimeout(timer); resolve(); }
    });
    server.stderr.on('data', (d) => { out += d.toString(); });
    server.on('exit', (code) => { if (code !== 0) reject(new Error('服务异常退出: ' + out)); });
  });
}
function stopServer() {
  return new Promise((resolve) => {
    if (!server) return resolve();
    server.on('exit', () => { server = null; resolve(); });
    server.kill('SIGTERM');
    setTimeout(() => { try { server.kill('SIGKILL'); } catch (_) {} }, 3000);
  });
}

async function api(pathName, { token, method = 'GET', body } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = 'Bearer ' + token;
  const resp = await fetch(BASE + pathName, {
    method, headers, body: body ? JSON.stringify(body) : undefined
  });
  let data = {};
  try { data = await resp.json(); } catch (_) {}
  return { status: resp.status, data };
}

async function login(role, nameOrPwd) {
  const body = role === 'admin' ? { password: nameOrPwd || 'admin123' } : { name: nameOrPwd };
  const { status, data } = await api(`/api/auth/login/${role}`, { method: 'POST', body });
  if (status !== 200) throw new Error('登录失败: ' + data.error);
  return data.token;
}

async function getState(token) {
  return (await api('/api/state', { token })).data.state;
}

async function openR1(token) {
  const r = await api('/api/routes/R1/open', {
    token, method: 'POST', body: { switchPositions: { SW7: 'N', SW8: 'N' } }
  });
  if (r.status !== 200) throw new Error('开放 R1 失败: ' + JSON.stringify(r.data));
  return r.data.instance.id;
}

/** R1 的完整定时步骤（3 段 → 6 步，交替占用/清空），每步等待 waitSec 秒 */
function r1Steps(waitSec) {
  const w = typeof waitSec === 'function' ? waitSec : () => waitSec;
  const steps = [];
  let n = 0;
  for (const sid of ['1', '3', '5']) {
    steps.push({ action: 'occupy', sectionId: sid, waitSeconds: w(++n) });
    steps.push({ action: 'clear', sectionId: sid, waitSeconds: w(++n) });
  }
  return steps;
}

async function submitPlan(token, iid, steps) {
  return api(`/api/instances/${iid}/run-plans`, { token, method: 'POST', body: { steps } });
}

async function waitPlanStatus(token, id, status, timeoutMs = 12000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const r = await api(`/api/run-plans?scope=all`, { token });
    const row = r.data.plans.find((x) => x.id === id);
    if (row && row.status === status) return row;
    if (row && status !== 'active' && row.status !== 'active') return row;
    if (Date.now() > deadline) throw new Error(`计划 #${id} 在 ${timeoutMs}ms 内未进入 ${status}（当前 ${row && row.status}）`);
    await sleep(150);
  }
}

async function waitInstanceGone(token, id, timeoutMs = 12000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const st = await getState(token);
    if (!st.openInstances.some((x) => x.id === id)) return st;
    if (Date.now() > deadline) throw new Error(`实例 #${id} 在 ${timeoutMs}ms 内未解除`);
    await sleep(150);
  }
}

/** 人工走完全程（与 cancellation 测试同一顺序） */
async function walkThrough(token, instanceId, sections) {
  let seq = 1;
  for (let i = 0; i < sections.length; i++) {
    await api(`/api/instances/${instanceId}/events`,
      { token, method: 'POST', body: { seq: seq++, sectionId: sections[i], action: 'occupy' } });
    if (i >= 1) {
      await api(`/api/instances/${instanceId}/events`,
        { token, method: 'POST', body: { seq: seq++, sectionId: sections[i - 1], action: 'clear' } });
    }
  }
  await api(`/api/instances/${instanceId}/events`,
    { token, method: 'POST', body: { seq: seq++, sectionId: sections[sections.length - 1], action: 'clear' } });
}

async function main() {
  await startServer();
  const admin = await login('admin');
  const jia = await login('dispatcher', '值班员甲');
  const yi = await login('dispatcher', '值班员乙');

  console.log('\n[1] 鉴权与前置校验');
  let r = await api('/api/instances/1/run-plans', { method: 'POST', body: { steps: [] } });
  eq('未登录 401', r.status, 401);
  r = await api('/api/instances/1/run-plans', { token: admin, method: 'POST', body: { steps: [] } });
  eq('管理员 403', r.status, 403);
  r = await submitPlan(jia, 999, r1Steps(1));
  eq('不存在实例 404', r.status, 404);
  eq('错误码 INSTANCE_NOT_FOUND', r.data.code, 'INSTANCE_NOT_FOUND');

  console.log('\n[2] 非法计划整体拒绝（不写入任何计划）');
  const iidV = await openR1(jia);
  r = await submitPlan(jia, iidV, []);
  eq('空步骤 400', r.status, 400);
  eq('错误码 RUNPLAN_STEPS_REQUIRED', r.data.code, 'RUNPLAN_STEPS_REQUIRED');
  r = await submitPlan(jia, iidV, r1Steps(1).slice(0, 5));
  eq('步数不足 400', r.status, 400);
  eq('错误码 RUNPLAN_STEPS_MISMATCH', r.data.code, 'RUNPLAN_STEPS_MISMATCH');
  const badOrder = r1Steps(1);
  badOrder[0] = { action: 'clear', sectionId: '1', waitSeconds: 1 }; // 首步应为占用
  r = await submitPlan(jia, iidV, badOrder);
  eq('首步清空 400', r.status, 400);
  eq('错误码 RUNPLAN_STEPS_MISMATCH(序)', r.data.code, 'RUNPLAN_STEPS_MISMATCH');
  const badSection = r1Steps(1);
  badSection[2] = { action: 'occupy', sectionId: '5', waitSeconds: 1 }; // 第二步占用应为区段3
  r = await submitPlan(jia, iidV, badSection);
  eq('区段顺序不符 400', r.status, 400);
  const badAction = r1Steps(1);
  badAction[1] = { action: 'occupy', sectionId: '1', waitSeconds: 1 }; // 第二步应为清空
  r = await submitPlan(jia, iidV, badAction);
  eq('动作未交替 400', r.status, 400);
  r = await submitPlan(jia, iidV, r1Steps(0));
  eq('等待 0 秒 400', r.status, 400);
  eq('错误码 RUNPLAN_BAD_WAIT', r.data.code, 'RUNPLAN_BAD_WAIT');
  r = await submitPlan(jia, iidV, r1Steps(-2));
  eq('等待负数 400', r.status, 400);
  r = await submitPlan(jia, iidV, r1Steps(1.5));
  eq('等待小数 400', r.status, 400);
  // 非法提交后不存在任何活动计划，实例仍开放、信号绿
  let st = await getState(jia);
  eq('非法计划未留下活动计划', Object.keys(st.activeRunPlansByInstance).length, 0);
  eq('实例仍开放', st.openInstances.some((x) => x.id === iidV), true);
  // 已有走行事件（不是尚未走行）→ 拒绝
  await api(`/api/instances/${iidV}/events`, {
    token: jia, method: 'POST', body: { seq: 1, sectionId: '1', action: 'occupy' }
  });
  r = await submitPlan(jia, iidV, r1Steps(1));
  eq('已有走行事件 409', r.status, 409);
  eq('错误码 RUNPLAN_ALREADY_MOVING', r.data.code, 'RUNPLAN_ALREADY_MOVING');
  await walkThrough(jia, iidV, ['1', '3', '5']);

  console.log('\n[3] 合法计划：返回编号/状态/下一步/到期时刻并自动走完全程');
  const iidA = await openR1(jia);
  const t0 = Date.now();
  r = await submitPlan(jia, iidA, r1Steps(1));
  eq('提交返回 201', r.status, 201);
  const p0 = r.data.plan;
  eq('计划状态 active', p0.status, 'active');
  eq('总步数 6', p0.totalSteps, 6);
  eq('已完成 0 步', p0.completedSteps, 0);
  eq('下一步为第 1 步', p0.nextStep.step, 1);
  eq('下一步动作 occupy', p0.nextStep.action, 'occupy');
  eq('下一步区段 1', p0.nextStep.sectionId, '1');
  ok('下一步到期约 1 秒后',
    p0.nextStep.dueAt - t0 >= 900 && p0.nextStep.dueAt - t0 <= 1200, { dueAt: p0.nextStep.dueAt - t0 });
  st = await getState(jia);
  ok('state 含活动计划索引', !!st.activeRunPlansByInstance[String(iidA)]);
  eq('state 下发全部计划', st.runPlans.some((x) => x.id === p0.id), true);
  // 到期自动执行，末步成功后完成并进路解除
  const done = await waitPlanStatus(jia, p0.id, 'completed');
  eq('计划 completed', done.status, 'completed');
  eq('完成时 6/6 步', done.completedSteps, 6);
  eq('完成后无下一步', done.nextStep, null);
  ok('完成说明', !!done.endReason && done.endReason.includes('完成'));
  await waitInstanceGone(jia, iidA);
  st = await getState(jia);
  const relInst = st.recentReleased.find((x) => x.id === iidA);
  ok('6 个走行事件齐全（序号 1~6）', [1, 2, 3, 4, 5, 6].every((n) => relInst.events.some((e) => e.seq === n)),
    relInst.events.map((e) => e.seq));
  ok('事件由系统代发（client 标记）', relInst.events.every((e) => String(e.client).includes('定时走行计划')),
    relInst.events.map((e) => e.client));
  eq('区段 1/3/5 全部恢复空闲',
    ['1', '3', '5'].map((id) => st.sections.find((s) => s.id === id).occ_state).join(','),
    'free,free,free');
  // 活动计划索引已清空
  eq('完成后无活动计划', Object.keys(st.activeRunPlansByInstance).length, 0);

  console.log('\n[4] 人工事件抢先：计划停止、人工结果保留、重复触发绝不重发');
  const iidB = await openR1(jia);
  r = await submitPlan(jia, iidB, r1Steps(3)); // 每步 3 秒
  const pB = r.data.plan.id;
  // 立即人工提交首段占用（抢先于计划第 1 步到期）
  r = await api(`/api/instances/${iidB}/events`, {
    token: jia, method: 'POST', body: { seq: 1, sectionId: '1', action: 'occupy' }
  });
  eq('人工占用受理', r.status, 200);
  const pBrow = await waitPlanStatus(jia, pB, 'stopped');
  eq('活动计划被停止', pBrow.status, 'stopped');
  ok('停止原因写明人工抢先', pBrow.endReason.includes('人工'), pBrow.endReason);
  eq('已执行步为 0（未注入）', pBrow.completedSteps, 0);
  // 等待超过计划首步到期时刻：系统不得补注入/重发任何事件
  await sleep(3600);
  st = await getState(jia);
  const instB = st.openInstances.find((x) => x.id === iidB);
  eq('抢先之后只有人工的 1 个事件', instB.events.length, 1);
  eq('唯一事件为人工序号 1 占用区段1',
    JSON.stringify([instB.events[0].seq, instB.events[0].action, instB.events[0].section_id]),
    JSON.stringify([1, 'occupy', '1']));
  eq('实例仍开放（计划停止不解除进路）', !!instB, true);
  // 人工继续走完全程，正常解除
  await walkThrough(jia, iidB, ['1', '3', '5']);
  st = await getState(jia);
  eq('人工走行后正常解除', st.openInstances.some((x) => x.id === iidB), false);

  console.log('\n[5] 同一实例唯一活动计划；本人终止仅停止注入不解除；非本人拒绝');
  const iidC = await openR1(jia);
  r = await submitPlan(jia, iidC, r1Steps(10));
  const pC = r.data.plan.id;
  r = await submitPlan(jia, iidC, r1Steps(10));
  eq('重复活动计划 409', r.status, 409);
  eq('错误码 RUNPLAN_ALREADY_ACTIVE', r.data.code, 'RUNPLAN_ALREADY_ACTIVE');
  // 非本人终止
  r = await api(`/api/run-plans/${pC}/terminate`, { token: yi, method: 'POST' });
  eq('非本人终止 403', r.status, 403);
  eq('错误码 NOT_OWNER', r.data.code, 'NOT_OWNER');
  // 查看：单张与列表
  r = await api(`/api/run-plans/${pC}`, { token: jia });
  eq('单张查询 200', r.status, 200);
  r = await api('/api/run-plans?status=active', { token: yi });
  eq('他人默认只见本人计划（不含甲的）', r.data.plans.some((x) => x.id === pC), false);
  r = await api('/api/run-plans?scope=all&status=active', { token: yi });
  eq('scope=all 可见全员计划', r.data.plans.some((x) => x.id === pC), true);
  // 本人终止
  r = await api(`/api/run-plans/${pC}/terminate`, { token: jia, method: 'POST' });
  eq('本人终止 200', r.status, 200);
  eq('终止后状态 stopped', r.data.plan.status, 'stopped');
  // 重复终止
  r = await api(`/api/run-plans/${pC}/terminate`, { token: jia, method: 'POST' });
  eq('已停止再终止 409', r.status, 409);
  eq('错误码 RUNPLAN_NOT_ACTIVE', r.data.code, 'RUNPLAN_NOT_ACTIVE');
  eq('查询不存在计划 404',
    (await api('/api/run-plans/99999', { token: jia })).status, 404);
  // 终止不解除进路、未注入任何事件，且到期后也不注入
  await sleep(1200);
  st = await getState(jia);
  const instC = st.openInstances.find((x) => x.id === iidC);
  ok('终止后实例仍开放', !!instC);
  eq('终止后无走行事件', instC.events.length, 0);
  eq('锁闭保留（3 段）', instC.lockedSections.length, 3);
  // 人工走完全程释放实例供后续用例
  await walkThrough(jia, iidC, ['1', '3', '5']);

  console.log('\n[6] 计划首步占用同一事务终止延时取消，并继续走完全程');
  r = await api('/api/admin/settings/cancel-protection', {
    token: admin, method: 'PUT', body: { protectionSeconds: 30 }
  });
  eq('保护时长设为 30 秒', r.status, 200);
  const iidD = await openR1(jia);
  r = await api(`/api/instances/${iidD}/cancel-delayed`, { token: jia, method: 'POST' });
  const cD = r.data.cancellation.id;
  r = await submitPlan(jia, iidD, r1Steps(1));
  const pD = r.data.plan.id;
  await waitPlanStatus(jia, pD, 'completed');
  await waitInstanceGone(jia, iidD);
  const cDrow = (await api('/api/cancellations?scope=all', { token: jia }))
    .data.cancellations.find((x) => x.id === cD);
  eq('延时取消被首步占用终止', cDrow.status, 'aborted');

  console.log('\n[7] 延时取消到期解除进路 → 计划前提失效停止（不覆盖取消结果）');
  r = await api('/api/admin/settings/cancel-protection', {
    token: admin, method: 'PUT', body: { protectionSeconds: 1 }
  });
  const iidE = await openR1(jia);
  r = await submitPlan(jia, iidE, r1Steps(5)); // 首步 5 秒，晚于取消到期
  const pE = r.data.plan.id;
  r = await api(`/api/instances/${iidE}/cancel-delayed`, { token: jia, method: 'POST' });
  eq('无走行记录时取消申请受理', r.status, 201);
  const pErow = await waitPlanStatus(jia, pE, 'stopped');
  ok('计划因实例解除停止', pErow.endReason.includes('解除'), pErow.endReason);
  st = await waitInstanceGone(jia, iidE);
  eq('进路已被延时取消解除', st.openInstances.some((x) => x.id === iidE), false);
  // 再等 5 秒确认计划不会复活注入
  const eventsE = (await getState(jia)).recentReleased.find((x) => x.id === iidE).events.length;
  await sleep(5200);
  const eventsE2 = (await getState(jia)).recentReleased.find((x) => x.id === iidE).events.length;
  eq('停止后不补注入任何事件', eventsE2, eventsE);

  console.log('\n[8] 重启：未完成计划继续，逾期步骤启动即依序处理');
  const iidF = await openR1(jia);
  r = await submitPlan(jia, iidF, r1Steps(300));
  const pF = r.data.plan.id;
  await stopServer();
  await startServer(true);
  st = await getState(jia);
  ok('重启后活动计划仍在', !!st.activeRunPlansByInstance[String(iidF)]);
  eq('重启后实例仍开放', st.openInstances.some((x) => x.id === iidF), true);
  // 把全部步骤到期时刻改到过去 → 再次重启，启动即依序处理完
  await stopServer();
  await new Promise((resolve) => {
    const Database = require('better-sqlite3');
    const d = new Database(YARD_DB_PATH);
    d.prepare(`UPDATE run_plan_steps SET due_at=? WHERE plan_id=?`)
      .run(Date.now() - 1000, pF);
    d.close();
    resolve();
  });
  await startServer(true);
  const pFrow = await waitPlanStatus(jia, pF, 'completed', 8000);
  eq('重启后逾期计划完成', pFrow.status, 'completed');
  eq('6 步全部执行', pFrow.completedSteps, 6);
  st = await getState(jia);
  eq('逾期步骤依序注入后进路解除', st.openInstances.some((x) => x.id === iidF), false);
  const relF = st.recentReleased.find((x) => x.id === iidF);
  eq('恰好 6 个事件（无重发）', relF.events.length, 6);

  console.log('\n[9] 创建副本不复制活动计划；原站场计划继续运行');
  const iidG = await openR1(jia);
  r = await submitPlan(jia, iidG, r1Steps(120));
  const pG = r.data.plan.id;
  r = await api('/api/admin/yards', {
    token: admin, method: 'POST', body: { name: '定时计划副本' }
  });
  eq('创建副本 201', r.status, 201);
  const yardId2 = r.data.yard.id;
  // 副本复制了开放实例，但没有复制活动计划
  r = await api('/api/auth/yard', { token: jia, method: 'PUT', body: { yardId: yardId2 } });
  eq('切换到副本', r.status, 200);
  st = await getState(jia);
  eq('副本含复制来的开放实例', st.openInstances.some((x) => x.id === iidG), true);
  eq('副本无活动计划', Object.keys(st.activeRunPlansByInstance).length, 0);
  r = await api('/api/run-plans?scope=all', { token: jia });
  eq('副本计划列表为空', r.data.plans.length, 0);
  // 副本可对复制来的实例独立提交计划（与原站场互不串用）
  r = await submitPlan(jia, iidG, r1Steps(60));
  eq('副本内可独立提交计划', r.status, 201);
  const pCopy = r.data.plan.id;
  // 切回原站场：原计划仍在
  await api('/api/auth/yard', { token: jia, method: 'PUT', body: { yardId: 1 } });
  st = await getState(jia);
  ok('原站场活动计划仍在（不串用）', !!st.activeRunPlansByInstance[String(iidG)]);

  console.log('\n[10] 管理员重置：计划与实例一并清除（原站场）');
  // 管理员会话当前站场默认原站场；先终止副本计划以免干扰（副本保留不重置）
  await api('/api/auth/yard', { token: jia, method: 'PUT', body: { yardId: yardId2 } });
  await api(`/api/run-plans/${pCopy}/terminate`, { token: jia, method: 'POST' });
  await api('/api/auth/yard', { token: jia, method: 'PUT', body: { yardId: 1 } });
  r = await api('/api/admin/reset/training', { token: admin, method: 'POST' });
  eq('重置 200', r.status, 200);
  eq('重置后无开放实例', r.data.state.openInstances.length, 0);
  eq('重置后无活动计划', Object.keys(r.data.state.activeRunPlansByInstance).length, 0);
  eq('重置后计划记录清空', r.data.state.runPlans.length, 0);

  console.log(`\n定时走行计划全部 ${passed} 项断言通过 ✅`);
  await stopServer();
}

main().catch(async (e) => {
  console.error('\n❌ 测试失败:', e);
  await stopServer();
  process.exit(1);
});
