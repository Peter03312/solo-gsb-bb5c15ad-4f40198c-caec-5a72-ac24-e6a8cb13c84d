'use strict';

/**
 * 定时走行计划。
 *
 * 语义（与需求一一对应）：
 *  - 值班员可对【当前站场已开放且尚未走行】的进路实例提交定时走行计划：
 *    按进路区段顺序交替列出 占用/清空 步骤（每段一对，共 2×区段数 步），
 *    每步填写正整数秒等待时长；首步从提交时刻计时，后续步的到期时刻在提交时
 *    按步依次累加确定——上一步成功的时刻即下一步计时的锚点，因此重启后
 *    "逾期步骤依序处理"只需把已到期的步骤在同一事务内连续执行完即可。
 *  - 提交时校验：步骤覆盖完整进路（动作/区段与进路顺序完全一致）、实例仍
 *    开放且无任何走行事件（尚未走行）、同一实例没有其他活动计划。任一不满足
 *    整体拒绝（不写入任何计划/步骤行）。
 *  - 到期处理：沿用现有事件序号与走行校验
 *    （interlocking.submitSectionEventInTxn），在同一事务内提交事件并推进
 *    计划；信号回红、逐段解锁、等待兑现等全部遵循现有规则。
 *  - 前提失效时计划停止（stopped）并写明原因、不覆盖人工结果：
 *      · 人工事件抢先改变进度（interlocking.submitSectionEvent 走人工路径，
 *        成功推进后同事务调用 onManualEventInTxn 停止计划）；
 *      · 延时取消申请到期解除（cancellation.processOneInTxn 内同事务停止）；
 *      · 人工解除/实例已不存在（releaseInstance 及兜底检测）。
 *    已经成功执行的步骤（executed_at 非空）绝不重复注入事件。
 *  - 末步成功后计划完成（completed；此时进路按现有规则同步自动解除）。
 *  - 本人终止（terminatePlan）仅停止后续注入，不解除进路、不改动已注入事件。
 *  - 重启后继续未完成计划，逾期步骤依序处理；副本创建不复制活动计划
 *    （yards.js 复制清单不含 run_plans/run_plan_steps）。
 *
 * 时间全部存 epoch 毫秒整数。
 */

const { getDb } = require('./db');
const { InterlockError } = require('./interlocking');

const MAX_WAIT_SECONDS = 24 * 3600; // 单步等待上限（正整数秒，1 天）

/** 系统代发走行事件在 section_events.client 中的标记（人工事件不会以此为名） */
function injectedClient(applicant) {
  return `定时走行计划（${applicant}）`;
}

let timer = null;

/* ------------------------------ 提交前校验 ------------------------------ */

/**
 * 校验并规范化步骤列表。
 * 要求：非空数组；每步 {action:'occupy'|'clear', sectionId, waitSeconds}；
 * 步骤数恰为 2×区段数，按区段顺序严格交替 占用→清空（…同一区段一对）；
 * 每个等待时长为 1~MAX_WAIT_SECONDS 的正整数。
 * @returns {Array<{action,sectionId,waitSeconds}>}
 */
function validateSteps(routeSteps, rawSteps) {
  if (!Array.isArray(rawSteps) || rawSteps.length === 0) {
    throw new InterlockError(400, 'RUNPLAN_STEPS_REQUIRED',
      '定时走行计划必须按进路区段顺序交替列出占用、清空步骤');
  }
  const expected = [];
  for (const s of routeSteps) {
    expected.push({ action: 'occupy', sectionId: s.section_id });
    expected.push({ action: 'clear', sectionId: s.section_id });
  }
  if (rawSteps.length !== expected.length) {
    throw new InterlockError(400, 'RUNPLAN_STEPS_MISMATCH',
      `步骤未覆盖完整进路：应为 ${expected.length} 步（${routeSteps.length} 个区段，` +
      `每段占用/清空各一步），收到 ${rawSteps.length} 步`,
      { expectedSteps: expected.length, receivedSteps: rawSteps.length });
  }

  const out = [];
  rawSteps.forEach((raw, idx) => {
    const want = expected[idx];
    const stepNo = idx + 1;
    const action = String((raw || {}).action || '');
    const sectionId = String((raw || {}).sectionId == null ? '' : raw.sectionId);
    if (!['occupy', 'clear'].includes(action)) {
      throw new InterlockError(400, 'RUNPLAN_BAD_ACTION',
        `第 ${stepNo} 步动作非法：只能是 occupy(占用) 或 clear(清空)（收到 ${action}）`,
        { step: stepNo });
    }
    if (!sectionId) {
      throw new InterlockError(400, 'RUNPLAN_BAD_SECTION',
        `第 ${stepNo} 步缺少区段编号`, { step: stepNo });
    }
    if (action !== want.action || sectionId !== want.sectionId) {
      throw new InterlockError(400, 'RUNPLAN_STEPS_MISMATCH',
        `第 ${stepNo} 步与进路顺序不符：应为 ${want.action === 'occupy' ? '占用' : '清空'} ` +
        `区段 ${want.sectionId}，收到 ${action === 'occupy' ? '占用' : '清空'} 区段 ${sectionId}`,
        { step: stepNo, expected: want, received: { action, sectionId } });
    }
    const wait = Number(raw.waitSeconds);
    if (!Number.isInteger(wait) || wait < 1 || wait > MAX_WAIT_SECONDS) {
      throw new InterlockError(400, 'RUNPLAN_BAD_WAIT',
        `第 ${stepNo} 步等待时长必须是 1~${MAX_WAIT_SECONDS} 之间的正整数秒` +
        `（收到 ${raw.waitSeconds}）`, { step: stepNo });
    }
    out.push({ action, sectionId, waitSeconds: wait });
  });
  return out;
}

/* -------------------------------- 提交 -------------------------------- */

/**
 * 提交定时走行计划。整体在单个事务内完成全部校验与入库，非法计划整体拒绝。
 * @param {number} instanceId 进路实例编号
 * @param {Array} rawSteps 步骤列表
 * @param {string} applicant 提交人（值班员姓名）
 * @returns {Object} 计划详情（含状态、下一步、到期时刻）
 */
function submitPlan(instanceId, rawSteps, applicant) {
  const db = getDb();
  const now = Date.now();

  const run = db.transaction(() => {
    const inst = db.prepare(`SELECT * FROM route_instances WHERE id=?`).get(instanceId);
    if (!inst || inst.status !== 'open') {
      throw new InterlockError(404, 'INSTANCE_NOT_FOUND',
        `进路实例 ${instanceId} 不存在或已解除，不能提交定时走行计划`);
    }

    // 同一实例没有其他活动计划（部分唯一索引兜底并发重复提交）
    const existing = db.prepare(
      `SELECT id FROM run_plans WHERE instance_id=? AND status='active'`
    ).get(instanceId);
    if (existing) {
      throw new InterlockError(409, 'RUNPLAN_ALREADY_ACTIVE',
        `进路实例 ${instanceId} 已有活动的定时走行计划 #${existing.id}，` +
        `同一实例不能重复提交；可由本人终止后再提交`,
        { planId: existing.id });
    }

    // 尚未走行：无任何走行事件（占用/清空都算）
    const ev = db.prepare(
      `SELECT seq, action, section_id FROM section_events WHERE instance_id=? ORDER BY seq LIMIT 1`
    ).get(instanceId);
    if (ev) {
      throw new InterlockError(409, 'RUNPLAN_ALREADY_MOVING',
        `进路实例 ${instanceId} 已有走行事件（#${ev.seq} ` +
        `${ev.action === 'occupy' ? '占用' : '清空'} 区段 ${ev.section_id}），不是尚未走行的进路，` +
        `不能再提交定时走行计划`,
        { instanceId });
    }

    const routeSteps = db.prepare(
      `SELECT step_order, section_id FROM route_steps WHERE route_id=? ORDER BY step_order`
    ).all(inst.route_id);
    const steps = validateSteps(routeSteps, rawSteps);

    const info = db.prepare(
      `INSERT INTO run_plans(instance_id, applicant, status, submitted_at)
       VALUES(?, ?, 'active', ?)`
    ).run(instanceId, applicant, now);
    const planId = info.lastInsertRowid;

    const insStep = db.prepare(
      `INSERT INTO run_plan_steps(plan_id, step_order, action, section_id, wait_seconds, due_at)
       VALUES(?, ?, ?, ?, ?, ?)`
    );
    let due = now;
    steps.forEach((s, idx) => {
      due += s.waitSeconds * 1000;
      insStep.run(planId, idx + 1, s.action, s.sectionId, s.waitSeconds, due);
    });

    return planId;
  });

  const planId = run();
  return getPlan(planId);
}

/* ------------------------ 前提失效：同事务停止计划 ------------------------ */

/**
 * 停止某实例上的活动计划（调用方必须已在事务内）。
 * @returns {number|null} 被停止的计划编号（无活动计划时 null）
 */
function stopActiveForInstanceInTxn(instanceId, reason) {
  const db = getDb();
  const row = db.prepare(
    `SELECT id FROM run_plans WHERE instance_id=? AND status='active'`
  ).get(instanceId);
  if (!row) return null;
  db.prepare(
    `UPDATE run_plans SET status='stopped', finished_at=?, end_reason=? WHERE id=?`
  ).run(Date.now(), reason, row.id);
  return row.id;
}

/**
 * 人工走行事件抢先改变进度（事件已在同一事务内成功写入后调用）：停止计划。
 * 不覆盖人工结果——本函数只改计划自身状态，不回滚/改写事件。
 */
function onManualEventInTxn(instanceId) {
  return stopActiveForInstanceInTxn(instanceId,
    '值班员人工提交了走行事件，抢先改变走行进度，定时走行计划停止；已注入结果以人工事件为准，后续步骤不再自动注入');
}

/** 延时取消到期解除 / 人工解除等使实例解除时：停止计划（同事务）。 */
function onInstanceReleasedInTxn(instanceId) {
  return stopActiveForInstanceInTxn(instanceId,
    '进路实例已解除（延时取消到期解除或人工解除），定时走行计划前提失效，计划停止');
}

/* ------------------------------ 到期处理 ------------------------------ */

/**
 * 处理单个活动计划的到期步骤（事务体）：
 * 逾期步骤依序在同一事务内连续注入；每步注入前重新确认前提仍成立。
 * 任一步前提失效（人工抢先/实例解除/走行校验不通过）即停止计划，不再继续。
 * @returns {{planId:number, executed:Array<number>, completed:boolean, stopped:?string}}
 */
function processOneInTxn(row, now) {
  const db = getDb();
  const planId = row.id;
  const executed = [];

  const inst = db.prepare(
    `SELECT * FROM route_instances WHERE id=?`
  ).get(row.instance_id);

  // 1) 实例已不存在/已解除：停止（重置删实例走外键级联，正常解除走此兜底）
  if (!inst || inst.status !== 'open') {
    db.prepare(
      `UPDATE run_plans SET status='stopped', finished_at=?,
              end_reason='进路实例已不存在或已解除，定时走行计划前提失效，计划停止'
        WHERE id=?`
    ).run(now, planId);
    return { planId, executed, stopped: 'instance_gone' };
  }

  const pending = db.prepare(
    `SELECT * FROM run_plan_steps WHERE plan_id=? AND executed_at IS NULL ORDER BY step_order`
  ).all(planId);

  // 2) 防御：没有待执行步骤的活动计划直接收尾为完成
  if (pending.length === 0) {
    db.prepare(
      `UPDATE run_plans SET status='completed', finished_at=?,
              end_reason='全部步骤已成功执行，定时走行计划完成' WHERE id=?`
    ).run(now, planId);
    return { planId, executed, completed: true };
  }

  const savepoint = db.transaction((step) => {
    // 沿用现有事件序号与走行校验；手动=否，避免反向再触发计划停止
    const res = require('./interlocking').submitSectionEventInTxn(
      row.instance_id, step.step_order, step.section_id, step.action,
      injectedClient(row.applicant), { manual: false }
    );
    if (res.idempotent) {
      // 理论不可达（executed_at 非空才会被跳过；同序号事件人工抢先会先停计划）。
      // 视为该步已存在，不重复注入。
      return { idempotent: true };
    }
    db.prepare(
      `UPDATE run_plan_steps SET executed_at=?, event_seq=? WHERE id=?`
    ).run(now, step.step_order, step.id);
    executed.push(step.step_order);
    return { idempotent: false };
  });

  for (const step of pending) {
    if (step.due_at > now) break; // 后续步未到期，等待下一轮扫描

    try {
      const r = savepoint(step);
      if (r.idempotent) continue;
      // 最后一个待执行步成功即计划完成（末步走行校验通常已令进路解除）
      if (step.step_order === pending[pending.length - 1].step_order) {
        db.prepare(
          `UPDATE run_plans SET status='completed', finished_at=?,
                  end_reason='末步已成功执行，定时走行计划完成；进路按现有走行规则自动解除'
            WHERE id=?`
        ).run(now, planId);
        return { planId, executed, completed: true };
      }
    } catch (e) {
      // 走行校验不通过（人工抢先改了进度、区段状态变化致 NOT_OCCUPIED 等）：
      // 回滚本步尝试，计划停止，已注入的人工/先前结果不被覆盖。
      const reason = `定时走行计划第 ${step.step_order} 步（` +
        `${step.action === 'occupy' ? '占用' : '清空'} 区段 ${step.section_id}）到期执行时` +
        `走行校验未通过：${e && e.message ? e.message : e}；计划停止，已提交结果保持不变，后续步骤不再自动注入`;
      db.prepare(
        `UPDATE run_plans SET status='stopped', finished_at=?, end_reason=? WHERE id=?`
      ).run(now, reason, planId);
      return { planId, executed, stopped: reason };
    }
  }

  return { planId, executed };
}

/**
 * 扫描当前站场全部到期活动计划并处理；每个计划一个独立顶层事务，
 * 单计划异常不拖垮扫描（下轮再试）。计划步骤注入可能令进路解除/逐段解锁，
 * 事务提交后统一触发一次等待兑现扫描。
 * @returns {{completed:Array<number>, stopped:Array<number>}}
 */
function processDue(trigger = 'timer') {
  const db = getDb();
  const now = Date.now();
  const duePlans = db.prepare(
    `SELECT * FROM run_plans WHERE status='active' AND EXISTS (
       SELECT 1 FROM run_plan_steps s
        WHERE s.plan_id=run_plans.id AND s.executed_at IS NULL AND s.due_at<=?
     ) ORDER BY id`
  ).all(now);

  const completed = [];
  const stopped = [];
  let anyExecuted = false;

  for (const row of duePlans) {
    const txn = db.transaction((r) => processOneInTxn(r, now));
    let res;
    try {
      res = txn(row);
    } catch (e) {
      console.error(`[runplan] 计划 #${row.id} 到期处理失败，下轮重试:`, e);
      continue;
    }
    if (res.executed && res.executed.length) anyExecuted = true;
    if (res.completed) {
      completed.push(row.id);
      anyExecuted = true;
    }
    if (res.stopped) stopped.push(row.id);
  }

  if (completed.length) {
    console.log(`[runplan] 定时走行计划完成（${trigger}）：#${completed.join('、#')}`);
  }
  if (stopped.length) {
    console.log(`[runplan] 定时走行计划停止（${trigger}）：#${stopped.join('、#')}`);
  }
  if (anyExecuted) {
    // 信号回红/逐段解锁/整条解除后的等待兑现沿用现有规则
    try {
      require('./interlocking').triggerWaitingSweep('runplan_due');
    } catch (e) {
      console.error('[runplan] 到期注入后等待兑现扫描失败:', e);
    }
  }
  return { completed, stopped };
}

/* ------------------------------ 定时与重启 ------------------------------ */

/** 逐站场扫描：各站场在自己的上下文/事务内按 due_at 处理，互不串用。 */
function processDueAllYards(trigger) {
  const { withYard } = require('./db');
  let yardsList;
  try {
    yardsList = require('./yards').listYards();
  } catch (_) {
    yardsList = null;
  }
  if (!yardsList) return processDue(trigger);
  const completed = [];
  const stopped = [];
  for (const y of yardsList) {
    try {
      const r = withYard(y.id, () => processDue(trigger + `:yard${y.id}`));
      for (const id of r.completed) completed.push({ yard: y.id, id });
      for (const id of r.stopped) stopped.push({ yard: y.id, id });
    } catch (e) {
      console.error(`[runplan] 站场 ${y.id} 定时扫描失败:`, e);
    }
  }
  return { completed, stopped };
}

/**
 * 启动定时扫描（服务启动时调用一次）：立即扫描一次处理重启期间已到期的步骤
 * （逾期步骤依序处理），之后每 0.5 秒全站场扫描。
 */
function startScheduler() {
  stopScheduler();
  try {
    const r = processDueAllYards('server_start');
    const comp = r.completed.map((x) => `#${x.id}(站场${x.yard})`);
    const stop = r.stopped.map((x) => `#${x.id}(站场${x.yard})`);
    if (comp.length || stop.length) {
      console.log(`[startup] 重启后处理定时走行计划：完成 ${comp.join('、') || '无'}，` +
        `停止 ${stop.join('、') || '无'}`);
    }
  } catch (e) {
    console.error('[startup] 定时走行计划到期扫描失败:', e);
  }
  timer = setInterval(() => {
    try { processDueAllYards('timer'); } catch (e) { console.error('[runplan] 定时扫描失败:', e); }
  }, 500);
}

function stopScheduler() {
  if (timer) { clearInterval(timer); timer = null; }
}

/* -------------------------------- 终止 -------------------------------- */

/**
 * 本人终止活动计划：仅停止后续注入，不解除进路、不改动已注入事件。
 * 非本人 403 NOT_OWNER；非活动 409 RUNPLAN_NOT_ACTIVE；不存在 404。
 */
function terminatePlan(planId, clientName) {
  const db = getDb();
  const row = db.prepare(`SELECT * FROM run_plans WHERE id=?`).get(planId);
  if (!row) throw new InterlockError(404, 'RUNPLAN_NOT_FOUND', `定时走行计划 ${planId} 不存在`);
  if (row.applicant !== clientName) {
    throw new InterlockError(403, 'NOT_OWNER',
      `不能终止：定时走行计划 #${planId} 的提交人为 ${row.applicant}，只能由本人终止`);
  }
  if (row.status !== 'active') {
    const map = { completed: '已完成', stopped: '已停止' };
    throw new InterlockError(409, 'RUNPLAN_NOT_ACTIVE',
      `不能终止：定时走行计划 #${planId} ${map[row.status] || row.status}，无需再终止`);
  }
  db.transaction(() => {
    db.prepare(
      `UPDATE run_plans SET status='stopped', finished_at=?,
              end_reason='提交人主动终止：仅停止后续自动注入，已注入的走行事件保持不变，进路不解除'
        WHERE id=? AND status='active'`
    ).run(Date.now(), planId);
  })();
  return getPlan(planId);
}

/* --------------------------- 管理员重置：一并清除 --------------------------- */

/**
 * 管理员重置推演：清除全部定时走行计划（进路实例随后被删除，外键级联也会删，
 * 这里在同一重置事务内显式清理，语义清晰）。应在重置事务内调用。
 */
function clearAllByReset() {
  getDb().prepare(`DELETE FROM run_plans`).run();
  return true;
}

/* ------------------------------ 查询 / 序列化 ------------------------------ */

function rowToPlan(row) {
  if (!row) return null;
  const db = getDb();
  const now = Date.now();
  const stepRows = db.prepare(
    `SELECT * FROM run_plan_steps WHERE plan_id=? ORDER BY step_order`
  ).all(row.id);
  const steps = stepRows.map((s) => ({
    step: s.step_order,
    action: s.action,                    // occupy / clear
    sectionId: s.section_id,
    waitSeconds: s.wait_seconds,
    dueAt: s.due_at,
    executedAt: s.executed_at,
    eventSeq: s.event_seq
  }));

  const nextStepRow = stepRows.find((s) => s.executed_at === null) || null;
  const total = steps.length;
  const doneCount = total - stepRows.filter((s) => s.executed_at === null).length;

  return {
    id: row.id,
    instanceId: row.instance_id,
    applicant: row.applicant,
    status: row.status,                  // active / completed / stopped
    submittedAt: row.submitted_at,
    finishedAt: row.finished_at,
    endReason: row.end_reason,
    totalSteps: total,
    completedSteps: doneCount,
    nextStep: nextStepRow ? {
      step: nextStepRow.step_order,
      action: nextStepRow.action,
      sectionId: nextStepRow.section_id,
      dueAt: nextStepRow.due_at,
      remainingMs: Math.max(0, nextStepRow.due_at - now),
      overDue: nextStepRow.due_at <= now
    } : null,
    steps
  };
}

function getPlan(id) {
  return rowToPlan(getDb().prepare(`SELECT * FROM run_plans WHERE id=?`).get(id));
}

/**
 * 计划列表：活动计划在前（按编号升序），终态在后（按结束时刻倒序）。
 * @param {{applicant?:string, status?:string}} filter
 */
function listPlans(filter = {}) {
  const db = getDb();
  let rows;
  if (filter.applicant) {
    rows = db.prepare(
      `SELECT * FROM run_plans WHERE applicant=?
        ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END,
                 CASE status WHEN 'active' THEN id ELSE -COALESCE(finished_at,0) END`
    ).all(filter.applicant);
  } else {
    rows = db.prepare(
      `SELECT * FROM run_plans
        ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END,
                 CASE status WHEN 'active' THEN id ELSE -COALESCE(finished_at,0) END`
    ).all();
  }
  if (filter.status) rows = rows.filter((r) => r.status === filter.status);
  return rows.map(rowToPlan);
}

/** 全部活动计划按实例索引（随 /api/state 下发，值班台据此显示与终止） */
function activeByInstance() {
  const rows = getDb().prepare(
    `SELECT * FROM run_plans WHERE status='active' ORDER BY id`
  ).all();
  const map = {};
  for (const r of rows) map[r.instance_id] = rowToPlan(r);
  return map;
}

module.exports = {
  MAX_WAIT_SECONDS,
  submitPlan,
  validateSteps,
  onManualEventInTxn,
  onInstanceReleasedInTxn,
  processDue,
  startScheduler,
  stopScheduler,
  terminatePlan,
  clearAllByReset,
  getPlan,
  listPlans,
  activeByInstance
};
