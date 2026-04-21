const express = require('express');
const cors = require('cors');
const axios = require('axios');
const mongoose = require('mongoose');

const app = express();
const PORT = process.env.PORT || 5000;
 
app.use(cors());
app.use(express.json());

/* =========================
   🔥 MONGODB CONNECTION
========================= */
const MONGODB_URI = process.env.MONGODB_URI ||
  'mongodb+srv://harshaffiliate16_db_user:5xJ3LN2LqA6qF6nt@cluster0.szkskqk.mongodb.net/?appName=Cluster0';

/* =========================
   🔥 MONGODB SCHEMAS
========================= */
const RunSchema = new mongoose.Schema({
  id:              { type: Number,  required: true, index: true },
  schedulerOrderId:{ type: String,  required: true, index: true },
  label:           { type: String,  required: true },
  apiUrl:          { type: String,  required: true },
  apiKey:          { type: String,  required: true },
  service:         { type: String,  required: true },
  link:            { type: String,  required: true },
  quantity:        { type: Number,  required: true },
  time:            { type: Date,    required: true },
  done:            { type: Boolean, default: false },
  status:          { type: String,  default: 'pending', index: true },
  smmOrderId:      { type: Number,  default: null },
  createdAt:       { type: Date,    default: Date.now },
  executedAt:      { type: Date,    default: null },
  error:           { type: String,  default: null },
  comments:        { type: String,  default: null },
});

// 🔥 Compound index for scheduler query performance
RunSchema.index({ done: 1, status: 1 });
RunSchema.index({ schedulerOrderId: 1, status: 1 });
RunSchema.index({ link: 1, label: 1, status: 1 });

const OrderSchema = new mongoose.Schema({
  schedulerOrderId: { type: String, required: true, unique: true, index: true },
  name:             { type: String, required: true },
  link:             { type: String, required: true },
  status:           { type: String, default: 'pending' },
  totalRuns:        { type: Number, required: true },
  completedRuns:    { type: Number, default: 0 },
  runStatuses:      [{ type: String }],
  createdAt:        { type: Date,   default: Date.now },
  lastUpdatedAt:    { type: Date,   default: Date.now },
});

const Run   = mongoose.model('Run',   RunSchema);
const Order = mongoose.model('Order', OrderSchema);

/* =========================
   🔥 GLOBAL SETTINGS
========================= */
let MIN_VIEWS_PER_RUN = 100;

/* =========================
   🔥 5 SEPARATE QUEUES + FLAGS
========================= */
let viewsQueue    = [];
let likesQueue    = [];
let sharesQueue   = [];
let savesQueue    = [];
let commentsQueue = [];

// 🔥 Single object for executing flags - easier to manage
const isExecuting = {
  VIEWS:    false,
  LIKES:    false,
  SHARES:   false,
  SAVES:    false,
  COMMENTS: false,
};

// 🔥 Cooldown tracker - prevents sending same link+label too fast
const lastExecutionTime = new Map();
const MIN_COOLDOWN_MS   = 10 * 60 * 1000; // 10 minutes

// 🔥 Max queue size safety guard
const MAX_QUEUE_SIZE = 500;

/* =========================
   🔥 START SERVER FIRST
   So Render always detects port
========================= */
app.listen(PORT, '0.0.0.0', () => {
  console.log(`========================================`);
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`Minimum views per run: ${MIN_VIEWS_PER_RUN}`);
  console.log(`Scheduler runs every 10 seconds`);
  console.log(`========================================`);
});

/* =========================
   🔥 THEN CONNECT MONGODB
========================= */
mongoose.connect(MONGODB_URI, {
  serverSelectionTimeoutMS: 30000,
  maxPoolSize: 10,
})
.then(async () => {
  console.log('✅ MongoDB Connected Successfully');

  // 🔥 Clean truly stuck runs on startup (older than 15 min, never got executedAt)
  try {
    const fifteenMinAgo = new Date(Date.now() - 15 * 60 * 1000);
    const cleanResult = await Run.updateMany(
      {
        status: 'processing',
        executedAt: null,
        createdAt: { $lt: fifteenMinAgo },
      },
      { $set: { status: 'pending', error: null } }
    );
    if (cleanResult.modifiedCount > 0) {
      console.log(`✅ Cleaned ${cleanResult.modifiedCount} stuck runs on startup`);
    }

    // 🔥 Also reset any orphaned 'queued' runs back to pending
    // (happens when server restarts mid-queue)
    const queuedClean = await Run.updateMany(
      { status: 'queued' },
      { $set: { status: 'pending' } }
    );
    if (queuedClean.modifiedCount > 0) {
      console.log(`✅ Reset ${queuedClean.modifiedCount} orphaned queued runs to pending`);
    }
  } catch (err) {
    console.error('Warning: Could not clean stuck runs:', err.message);
  }
})
.catch(err => {
  console.error('❌ MongoDB Connection Error:', err);
  console.log('⚠️ Server running but database not connected');
});

/* =========================
   🔥 HELPER: QUEUE MAP
   Maps label → queue array
========================= */
function getQueueForLabel(label) {
  switch (label) {
    case 'VIEWS':    return viewsQueue;
    case 'LIKES':    return likesQueue;
    case 'SHARES':   return sharesQueue;
    case 'SAVES':    return savesQueue;
    case 'COMMENTS': return commentsQueue;
    default:         return null;
  }
}

function pushToQueue(run) {
  const queue = getQueueForLabel(run.label);
  if (!queue) return false;
  if (queue.length >= MAX_QUEUE_SIZE) {
    console.warn(`[QUEUE] ${run.label} queue full (${MAX_QUEUE_SIZE}), dropping run #${run.id}`);
    return false;
  }
  queue.push(run);
  return true;
}

/* =========================
   🔥 PLACE ORDER WITH SMM API
========================= */
async function placeOrder({ apiUrl, apiKey, service, link, quantity, comments }) {
  // 🔥 FIXED: Build params correctly based on type
  const params = new URLSearchParams({
    key:    apiKey,
    action: 'add',
    service: String(service),
    link:   String(link),
  });

  if (comments) {
    // 🔥 FIXED: Comments orders - send comments, NOT quantity
    // Quantity for SMM API = number of comment lines (handled by comments string)
    params.append('comments', comments);
  } else {
    // Normal orders - send quantity
    params.append('quantity', String(quantity));
  }

  const response = await axios.post(apiUrl, params.toString(), {
    headers:  { 'Content-Type': 'application/x-www-form-urlencoded' },
    timeout:  30000, // 🔥 FIXED: 30s timeout - prevent indefinite hang
  });

  return response.data;
}

/* =========================
   🔥 ADD RUNS TO DATABASE
========================= */
async function addRuns(services, baseConfig, schedulerOrderId) {
  const runsToInsert = [];

  for (const [key, serviceConfig] of Object.entries(services)) {
    if (!serviceConfig) continue;

    const label = key.toUpperCase();

    for (const run of serviceConfig.runs) {
      let quantity;

      // VIEWS
      if (label === 'VIEWS') {
        // 🔥 FIXED: Use live MIN_VIEWS_PER_RUN variable, not hardcoded 100
        if (!run.quantity || run.quantity < MIN_VIEWS_PER_RUN) continue;
        quantity = run.quantity;
      }

      // COMMENTS
      else if (label === 'COMMENTS') {
        if (!run.comments) continue;

        let lines = run.comments
          .split('\n')
          .map(c => c.trim())
          .filter(c => c.length > 0);

        if (lines.length < 1) continue;

        // Limit max to 10 comments per run
        if (lines.length > 10) {
          lines = lines.sort(() => Math.random() - 0.5).slice(0, 10);
        }

        run.comments = lines.join('\n');
        quantity     = lines.length;
      }

      // OTHERS (likes, shares, saves)
      else {
        if (!run.quantity || run.quantity <= 0) continue;
        quantity = run.quantity;
      }

      runsToInsert.push({
        id:               Date.now() + Math.random(),
        schedulerOrderId,
        label,
        apiUrl:           baseConfig.apiUrl,
        apiKey:           baseConfig.apiKey,
        service:          serviceConfig.serviceId,
        link:             baseConfig.link,
        quantity,
        time:             new Date(run.time),
        done:             false,
        status:           'pending',
        smmOrderId:       null,
        createdAt:        new Date(),
        executedAt:       null,
        error:            null,
        comments:         run.comments || null,
      });
    }
  }

  // 🔥 FIXED: Batch insert all runs in one DB call instead of N individual saves
  if (runsToInsert.length > 0) {
    await Run.insertMany(runsToInsert);
  }

  return runsToInsert;
}

/* =========================
   🔥 UPDATE ORDER STATUS
========================= */
async function updateOrderStatus(schedulerOrderId) {
  if (!schedulerOrderId) return;

  // 🔥 Use single aggregation instead of two separate queries
  const orderRuns = await Run.find(
    { schedulerOrderId },
    { status: 1 } // Only fetch status field - faster
  ).lean(); // .lean() returns plain JS objects - 2x faster than full Mongoose docs

  const order = await Order.findOne({ schedulerOrderId }).lean();
  if (!order) return;

  // 🔥 Don't overwrite cancelled orders
  if (order.status === 'cancelled') return;

  const totalRuns      = orderRuns.length;
  const completedRuns  = orderRuns.filter(r => r.status === 'completed').length;
  const failedRuns     = orderRuns.filter(r => r.status === 'failed').length;
  const cancelledRuns  = orderRuns.filter(r => r.status === 'cancelled').length;
  const processingRuns = orderRuns.filter(r => r.status === 'processing').length;
  const queuedRuns     = orderRuns.filter(r => r.status === 'queued').length;
  const pausedRuns     = orderRuns.filter(r => r.status === 'paused').length;
  const pendingRuns    = orderRuns.filter(r => r.status === 'pending').length;
  const activeRuns     = totalRuns - cancelledRuns;

  let newStatus;

  if (activeRuns === 0) {
    newStatus = 'cancelled';
  } else if (completedRuns === activeRuns) {
    newStatus = 'completed';
  } else if (failedRuns === activeRuns) {
    newStatus = 'failed';
  } else if (pausedRuns > 0 && processingRuns === 0 && queuedRuns === 0) {
    newStatus = 'paused';
  } else if (processingRuns > 0 || completedRuns > 0 || queuedRuns > 0) {
    newStatus = 'running';
  } else if (pendingRuns > 0) {
    newStatus = 'pending';
  } else {
    newStatus = order.status;
  }

  await Order.updateOne(
    { schedulerOrderId },
    {
      $set: {
        status:       newStatus,
        completedRuns,
        totalRuns,
        lastUpdatedAt: new Date(),
        runStatuses:  orderRuns.map(r => r.status),
      },
    }
  );
}

/* =========================
   🔥 EXECUTE A SINGLE RUN
========================= */
async function executeRun(run) {
  // 🔥 Guard: validate run object
  if (!run || !run._id) {
    console.warn(`[executeRun] Invalid run object, skipping`);
    return;
  }

  // 🔥 Guard: skip if run was cancelled before we got here
  if (run.status === 'cancelled') {
    console.log(`[${run.label}] Run #${run.id} already cancelled, skipping`);
    return;
  }

  // 🔥 Guard: skip if quantity is invalid
  if (!run.quantity || run.quantity <= 0) {
    console.warn(`[${run.label}] Run #${run.id} has invalid quantity, marking failed`);
    await Run.updateOne({ _id: run._id }, { $set: { status: 'failed', error: 'Invalid quantity' } });
    return;
  }

  // 🔥 Check if parent order is still active
  const order = await Order.findOne(
    { schedulerOrderId: run.schedulerOrderId },
    { status: 1 }
  ).lean();

  if (!order || order.status === 'cancelled') {
    console.log(`[${run.label}] Order cancelled → skipping run #${run.id}`);
    await Run.updateOne({ _id: run._id }, { $set: { status: 'cancelled', done: true } });
    return;
  }

  if (order.status === 'paused') {
    console.log(`[${run.label}] Order paused → skipping run #${run.id}`);
    await Run.updateOne({ _id: run._id }, { $set: { status: 'paused' } });
    return;
  }

  try {
    // 🔥 FIXED: Cross-order link+label protection
    // Check if same link + same label is already being processed anywhere
    const activeSameType = await Run.findOne({
      link:   run.link,
      label:  run.label,
      status: 'processing',
      _id:    { $ne: run._id },
    }).lean();

    if (activeSameType) {
      console.log(`[${run.label}] Same type already processing for link, re-queuing run #${run.id}`);
      // 🔥 FIXED: Reset to pending so scheduler picks it up properly later
      // Do NOT push back to queue directly - scheduler will re-add when processing clears
      await Run.updateOne(
        { _id: run._id },
        { $set: { status: 'pending', time: new Date(Date.now() + 2 * 60 * 1000) } }
      );
      return;
    }

    console.log(`[${run.label}] Executing run #${run.id} | qty: ${run.quantity} | link: ${run.link}`);

    // Mark as processing
    await Run.updateOne(
      { _id: run._id },
      { $set: { status: 'processing', executedAt: new Date() } }
    );

    // Build payload
    const payload = {
      apiUrl:   run.apiUrl,
      apiKey:   run.apiKey,
      service:  run.service,
      link:     run.link,
      quantity: run.quantity,
      comments: run.label === 'COMMENTS' ? run.comments : null,
    };

    const result = await placeOrder(payload);

    if (result?.order) {
      console.log(`[${run.label}] ✅ SUCCESS - SMM Order ID: ${result.order}`);
      await Run.updateOne(
        { _id: run._id },
        {
          $set: {
            done:      true,
            status:    'completed',
            smmOrderId: result.order,
            error:     null,
          },
        }
      );
    } else {
      const errorMsg = result?.error || 'Unknown provider error';
      console.error(`[${run.label}] ❌ FAILED - Provider response:`, result);

      // 🔥 Provider busy → retry in 5 min
      if (
        errorMsg.toLowerCase().includes('active order') ||
        errorMsg.toLowerCase().includes('wait until') ||
        errorMsg.toLowerCase().includes('in progress')
      ) {
        console.log(`[${run.label}] Provider busy → retry in 5 min`);
        await Run.updateOne(
          { _id: run._id },
          {
            $set: {
              status: 'pending',
              error:  null,
              time:   new Date(Date.now() + 5 * 60 * 1000),
            },
          }
        );
      } else {
        await Run.updateOne(
          { _id: run._id },
          { $set: { status: 'failed', error: errorMsg } }
        );
      }
    }
  } catch (err) {
    const errorMsg = err.response?.data?.error || err.message || 'Unknown error';
    console.error(`[${run.label}] ❌ ERROR run #${run.id}:`, errorMsg);

    if (!run._id) return;

    // 🔥 Provider busy in catch block too
    if (
      errorMsg.toLowerCase().includes('active order') ||
      errorMsg.toLowerCase().includes('wait until') ||
      errorMsg.toLowerCase().includes('in progress')
    ) {
      console.log(`[${run.label}] Provider busy (catch) → retry in 5 min`);
      await Run.updateOne(
        { _id: run._id },
        {
          $set: {
            status: 'pending',
            error:  null,
            time:   new Date(Date.now() + 5 * 60 * 1000),
          },
        }
      );
    } else {
      await Run.updateOne(
        { _id: run._id },
        { $set: { status: 'failed', error: errorMsg } }
      );
    }
  }

  // 🔥 FIXED: Only one updateOrderStatus call - at the very end
  await updateOrderStatus(run.schedulerOrderId);
}

/* =========================
   🔥 UNIFIED QUEUE PROCESSOR
   Single factory function replaces
   5 identical duplicate processors
========================= */
function createQueueProcessor(label, getQueue, getFlag, setFlag) {
  return async function processQueue() {
    const queue = getQueue();

    if (getFlag() || queue.length === 0) return;

    setFlag(true);
    const run = queue.shift();

    console.log(`[${label} QUEUE] Processing run #${run.id} | Remaining: ${queue.length}`);

    try {
      const cooldownKey   = `${run.link}-${label}`;
      const lastExec      = lastExecutionTime.get(cooldownKey) || 0;
      const timeSinceLast = Date.now() - lastExec;

      if (timeSinceLast < MIN_COOLDOWN_MS) {
        const waitMs = MIN_COOLDOWN_MS - timeSinceLast;
        console.log(`[${label} QUEUE] Cooldown active, ${Math.round(waitMs / 1000)}s remaining`);

        // 🔥 FIXED: Put run back, release flag, schedule retry
        // Do NOT sleep here - just schedule a future check
        queue.unshift(run);
        setFlag(false);

        // Schedule next attempt after cooldown
        const retryAfter = Math.min(waitMs, 5 * 60 * 1000); // max 5 min wait
        setTimeout(() => {
          if (queue.length > 0 && !getFlag()) {
            processQueue();
          }
        }, retryAfter);
        return;
      }

      // 🔥 Fetch fresh run from DB to get latest status
      const freshRun = await Run.findById(run._id).lean();

      if (!freshRun) {
        console.log(`[${label} QUEUE] Run not found in DB, skipping`);
      } else if (freshRun.status === 'cancelled' || freshRun.status === 'completed' || freshRun.status === 'failed') {
        console.log(`[${label} QUEUE] Run #${run.id} is ${freshRun.status}, skipping`);
      } else {
        lastExecutionTime.set(cooldownKey, Date.now());
        // 🔥 Pass freshRun to executeRun for latest state
        await executeRun({ ...freshRun, _id: freshRun._id });
      }
    } catch (err) {
      console.error(`[${label} QUEUE] Unexpected error:`, err.message);
    }

    setFlag(false);

    // 🔥 FIXED: Small delay between runs (2s) then continue queue
    // Much shorter than 8s, prevents double-start issue
    if (queue.length > 0) {
      setTimeout(() => {
        if (!getFlag()) processQueue();
      }, 2000);
    }
  };
}

// 🔥 Create the 5 processors using factory
const processViewsQueue = createQueueProcessor(
  'VIEWS',
  () => viewsQueue,
  () => isExecuting.VIEWS,
  (v) => { isExecuting.VIEWS = v; }
);

const processLikesQueue = createQueueProcessor(
  'LIKES',
  () => likesQueue,
  () => isExecuting.LIKES,
  (v) => { isExecuting.LIKES = v; }
);

const processSharesQueue = createQueueProcessor(
  'SHARES',
  () => sharesQueue,
  () => isExecuting.SHARES,
  (v) => { isExecuting.SHARES = v; }
);

const processSavesQueue = createQueueProcessor(
  'SAVES',
  () => savesQueue,
  () => isExecuting.SAVES,
  (v) => { isExecuting.SAVES = v; }
);

const processCommentsQueue = createQueueProcessor(
  'COMMENTS',
  () => commentsQueue,
  () => isExecuting.COMMENTS,
  (v) => { isExecuting.COMMENTS = v; }
);

/* =========================
   🔥 CHECK IF RUN IN QUEUE
========================= */
function isRunInQueue(runId) {
  // 🔥 FIXED: Compare as same type (Number)
  const id = Number(runId);
  return viewsQueue.some(r    => Number(r.id) === id) ||
         likesQueue.some(r    => Number(r.id) === id) ||
         sharesQueue.some(r   => Number(r.id) === id) ||
         savesQueue.some(r    => Number(r.id) === id) ||
         commentsQueue.some(r => Number(r.id) === id);
}

/* =========================
   🔥 REMOVE RUN FROM ALL QUEUES
========================= */
function removeFromAllQueues(runId) {
  const id     = Number(runId);
  const filter = r => Number(r.id) !== id;
  viewsQueue    = viewsQueue.filter(filter);
  likesQueue    = likesQueue.filter(filter);
  sharesQueue   = sharesQueue.filter(filter);
  savesQueue    = savesQueue.filter(filter);
  commentsQueue = commentsQueue.filter(filter);
}

/* =========================
   🔥 REMOVE ORDER FROM ALL QUEUES
========================= */
function removeOrderFromAllQueues(schedulerOrderId) {
  const filter  = r => r.schedulerOrderId !== schedulerOrderId;
  viewsQueue    = viewsQueue.filter(filter);
  likesQueue    = likesQueue.filter(filter);
  sharesQueue   = sharesQueue.filter(filter);
  savesQueue    = savesQueue.filter(filter);
  commentsQueue = commentsQueue.filter(filter);
}

/* =========================
   🔥 MAIN SCHEDULER (every 10s)
========================= */
mongoose.connection.once('open', () => {
  console.log('🚀 Scheduler started after DB connected');

  // 🔥 Periodic stuck run cleanup (every 15 min)
  // FIXED: Moved OUT of executeRun - was firing on every single run execution
  setInterval(async () => {
    try {
      const fifteenMinAgo = new Date(Date.now() - 15 * 60 * 1000);
      const result = await Run.updateMany(
        {
          status:    'processing',
          executedAt: null,
          createdAt: { $lt: fifteenMinAgo },
        },
        { $set: { status: 'pending', error: null } }
      );
      if (result.modifiedCount > 0) {
        console.log(`[CLEANUP] Reset ${result.modifiedCount} stuck processing runs`);
      }
    } catch (err) {
      console.error('[CLEANUP] Error:', err.message);
    }
  }, 15 * 60 * 1000); // Every 15 minutes

  // 🔥 Main scheduler tick
  setInterval(async () => {
    try {
      const now = new Date();
      let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0 };

      // 🔥 FIXED: Only fetch pending runs that are due NOW
      // No longer fetches ALL runs then filters - much more efficient
      const pendingRuns = await Run.find({
        done:   false,
        status: 'pending',
        time:   { $lte: now },
      })
      .limit(50) // 🔥 Safety limit - process max 50 per tick
      .lean();

      if (pendingRuns.length === 0) return;

      // 🔥 FIXED: Batch fetch all related orders in ONE query
      // Instead of N individual Order.findOne calls inside loop
      const uniqueOrderIds = [...new Set(pendingRuns.map(r => r.schedulerOrderId))];
      const activeOrders   = await Order.find(
        { schedulerOrderId: { $in: uniqueOrderIds } },
        { schedulerOrderId: 1, status: 1 }
      ).lean();

      // Build a lookup map for O(1) access
      const orderStatusMap = new Map(
        activeOrders.map(o => [o.schedulerOrderId, o.status])
      );

      const runsToMarkQueued = [];

      for (const run of pendingRuns) {
        // 🔥 Skip if already in memory queue
        if (isRunInQueue(run.id)) continue;

        const orderStatus = orderStatusMap.get(run.schedulerOrderId);

        // 🔥 Skip if order doesn't exist or is inactive
        if (!orderStatus || orderStatus === 'cancelled' || orderStatus === 'paused') {
          if (orderStatus === 'cancelled') {
            // Mark the run cancelled too
            runsToMarkQueued.push({ id: run._id, newStatus: 'cancelled' });
          }
          continue;
        }

        // 🔥 Add to appropriate queue
        const pushed = pushToQueue(run);
        if (!pushed) continue;

        runsToMarkQueued.push({ id: run._id, newStatus: 'queued' });

        if (run.label === 'VIEWS')    { addedToQueue.views++;    console.log(`[SCHEDULER] VIEWS run #${run.id} → queue (qty: ${run.quantity})`); }
        if (run.label === 'LIKES')    { addedToQueue.likes++;    console.log(`[SCHEDULER] LIKES run #${run.id} → queue (qty: ${run.quantity})`); }
        if (run.label === 'SHARES')   { addedToQueue.shares++;   console.log(`[SCHEDULER] SHARES run #${run.id} → queue (qty: ${run.quantity})`); }
        if (run.label === 'SAVES')    { addedToQueue.saves++;    console.log(`[SCHEDULER] SAVES run #${run.id} → queue (qty: ${run.quantity})`); }
        if (run.label === 'COMMENTS') { addedToQueue.comments++; console.log(`[SCHEDULER] COMMENTS run #${run.id} → queue (qty: ${run.quantity})`); }
      }

      // 🔥 FIXED: Batch update all status changes in bulk ops instead of N individual saves
      if (runsToMarkQueued.length > 0) {
        const bulkOps = runsToMarkQueued.map(item => ({
          updateOne: {
            filter: { _id: item.id },
            update: { $set: { status: item.newStatus } },
          },
        }));
        await Run.bulkWrite(bulkOps);
      }

      const totalAdded = Object.values(addedToQueue).reduce((a, b) => a + b, 0);
      if (totalAdded > 0) {
        console.log(`[SCHEDULER] Queued → Views:${addedToQueue.views} Likes:${addedToQueue.likes} Shares:${addedToQueue.shares} Saves:${addedToQueue.saves} Comments:${addedToQueue.comments}`);
      }

      // 🔥 Trigger queue processors
      if (viewsQueue.length > 0    && !isExecuting.VIEWS)    processViewsQueue();
      if (likesQueue.length > 0    && !isExecuting.LIKES)    processLikesQueue();
      if (sharesQueue.length > 0   && !isExecuting.SHARES)   processSharesQueue();
      if (savesQueue.length > 0    && !isExecuting.SAVES)    processSavesQueue();
      if (commentsQueue.length > 0 && !isExecuting.COMMENTS) processCommentsQueue();

    } catch (error) {
      console.error('[SCHEDULER] Error:', error.message);
    }
  }, 10000);
});

/* =========================
   🔥 API: CREATE ORDER
========================= */
app.post('/api/order', async (req, res) => {
  try {
    const { apiUrl, apiKey, link, services, name } = req.body;

    if (!apiUrl || !apiKey || !link || !services) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    console.log('[CREATE ORDER] Services received:', JSON.stringify(services, null, 2));

    const schedulerOrderId = `sched-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const runsForOrder     = await addRuns(services, { apiUrl, apiKey, link }, schedulerOrderId);

    if (runsForOrder.length === 0) {
      return res.status(400).json({ error: 'No valid runs could be created from provided services' });
    }

    const orderData = new Order({
      schedulerOrderId,
      name:         name || `Order ${schedulerOrderId}`,
      link,
      status:       'pending',
      totalRuns:    runsForOrder.length,
      completedRuns: 0,
      runStatuses:  runsForOrder.map(() => 'pending'),
      createdAt:    new Date(),
      lastUpdatedAt: new Date(),
    });

    await orderData.save();

    console.log(`[CREATE ORDER] ✅ Created ${schedulerOrderId} with ${runsForOrder.length} runs`);

    return res.json({
      success:          true,
      message:          'Order scheduled',
      schedulerOrderId,
      status:           'pending',
      completedRuns:    0,
      totalRuns:        runsForOrder.length,
    });
  } catch (error) {
    console.error('[CREATE ORDER] Error:', error);
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 API: FETCH SMM SERVICES
========================= */
app.post('/api/services', async (req, res) => {
  const { apiUrl, apiKey } = req.body;
  if (!apiUrl || !apiKey) {
    return res.status(400).json({ error: 'Missing API URL or key' });
  }
  try {
    const params   = new URLSearchParams({ key: apiKey, action: 'services' });
    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 30000,
    });
    return res.json(response.data);
  } catch (error) {
    return res.status(500).json({ error: error.response?.data || error.message });
  }
});

/* =========================
   🔥 API: GET SINGLE ORDER STATUS
========================= */
app.get('/api/order/status/:schedulerOrderId', async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;

    // 🔥 Parallel fetch for speed
    const [order, orderRuns] = await Promise.all([
      Order.findOne({ schedulerOrderId }).lean(),
      Run.find({ schedulerOrderId }).lean(),
    ]);

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    return res.json({
      schedulerOrderId: order.schedulerOrderId,
      name:             order.name,
      link:             order.link,
      status:           order.status,
      totalRuns:        order.totalRuns,
      completedRuns:    order.completedRuns,
      runStatuses:      order.runStatuses,
      createdAt:        order.createdAt,
      lastUpdatedAt:    order.lastUpdatedAt,
      runs: orderRuns.map(r => ({
        id:          r.id,
        label:       r.label,
        quantity:    r.quantity,
        time:        r.time,
        status:      r.status,
        smmOrderId:  r.smmOrderId,
        executedAt:  r.executedAt,
        error:       r.error,
      })),
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 API: GET ALL ORDERS STATUS
========================= */
app.get('/api/orders/status', async (req, res) => {
  try {
    // 🔥 FIXED: Use aggregation instead of N+1 queries
    // Fetch all orders first
    const allOrders = await Order.find()
      .sort({ createdAt: -1 })
      .lean();

    if (allOrders.length === 0) {
      return res.json({ total: 0, orders: [] });
    }

    // 🔥 Fetch ALL runs for ALL orders in ONE query
    const allOrderIds = allOrders.map(o => o.schedulerOrderId);
    const allRuns     = await Run.find(
      { schedulerOrderId: { $in: allOrderIds } },
      { id: 1, label: 1, quantity: 1, time: 1, status: 1, smmOrderId: 1, schedulerOrderId: 1 }
    ).lean();

    // 🔥 Group runs by schedulerOrderId using Map for O(1) lookup
    const runsByOrder = new Map();
    for (const run of allRuns) {
      if (!runsByOrder.has(run.schedulerOrderId)) {
        runsByOrder.set(run.schedulerOrderId, []);
      }
      runsByOrder.get(run.schedulerOrderId).push(run);
    }

    const ordersWithRuns = allOrders.map(order => ({
      schedulerOrderId: order.schedulerOrderId,
      name:             order.name,
      link:             order.link,
      status:           order.status,
      totalRuns:        order.totalRuns,
      completedRuns:    order.completedRuns,
      runStatuses:      order.runStatuses,
      createdAt:        order.createdAt,
      lastUpdatedAt:    order.lastUpdatedAt,
      runs: (runsByOrder.get(order.schedulerOrderId) || []).map(r => ({
        id:         r.id,
        label:      r.label,
        quantity:   r.quantity,
        time:       r.time,
        status:     r.status,
        smmOrderId: r.smmOrderId,
      })),
    }));

    return res.json({ total: allOrders.length, orders: ordersWithRuns });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 API: ORDER CONTROL
   (cancel / pause / resume)
========================= */
app.post('/api/order/control', async (req, res) => {
  try {
    const { schedulerOrderId, action } = req.body;

    if (!schedulerOrderId || !action) {
      return res.status(400).json({ error: 'Missing schedulerOrderId or action' });
    }

    const order = await Order.findOne({ schedulerOrderId });
    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    if (action === 'cancel') {
      // 🔥 FIXED: Single bulkWrite instead of N individual saves
      await Run.updateMany(
        {
          schedulerOrderId,
          status: { $in: ['pending', 'processing', 'queued', 'paused'] },
        },
        { $set: { status: 'cancelled', done: true } }
      );

      // 🔥 FIXED: Remove entire order from all queues in one shot
      removeOrderFromAllQueues(schedulerOrderId);

      order.status = 'cancelled';
      await order.save();

      // Fetch updated runs for response
      const updatedRuns = await Run.find({ schedulerOrderId }, { status: 1 }).lean();

      return res.json({
        success:      true,
        status:       'cancelled',
        completedRuns: updatedRuns.filter(r => r.status === 'completed').length,
        runStatuses:  updatedRuns.map(r => r.status),
      });
    }

    if (action === 'pause') {
      // 🔥 FIXED: Single DB call
      await Run.updateMany(
        {
          schedulerOrderId,
          status: { $in: ['pending', 'queued'] },
        },
        { $set: { status: 'paused' } }
      );

      // 🔥 Remove from all queues
      removeOrderFromAllQueues(schedulerOrderId);

      order.status = 'paused';
      await order.save();

      const updatedRuns = await Run.find({ schedulerOrderId }, { status: 1 }).lean();

      return res.json({
        success:      true,
        status:       'paused',
        completedRuns: updatedRuns.filter(r => r.status === 'completed').length,
        runStatuses:  updatedRuns.map(r => r.status),
      });
    }

    if (action === 'resume') {
      // 🔥 Reset all paused runs back to pending
      await Run.updateMany(
        { schedulerOrderId, status: 'paused' },
        { $set: { status: 'pending' } }
      );

      order.status = 'running';
      await order.save();

      const updatedRuns = await Run.find({ schedulerOrderId }, { status: 1 }).lean();

      return res.json({
        success:      true,
        status:       'running',
        completedRuns: updatedRuns.filter(r => r.status === 'completed').length,
        runStatuses:  updatedRuns.map(r => r.status),
      });
    }

    return res.status(400).json({ error: 'Invalid action. Use: cancel, pause, resume' });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 API: GET ORDER RUNS
========================= */
app.get('/api/order/runs/:schedulerOrderId', async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    const orderRuns = await Run.find({ schedulerOrderId }).lean();

    return res.json({
      schedulerOrderId,
      runs: orderRuns.map(r => ({
        id:         r.id,
        label:      r.label,
        quantity:   r.quantity,
        time:       r.time,
        status:     r.status,
        smmOrderId: r.smmOrderId,
        executedAt: r.executedAt,
        error:      r.error,
      })),
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 API: MIN VIEWS SETTINGS
========================= */
app.get('/api/settings/min-views', (req, res) => {
  return res.json({ minViewsPerRun: MIN_VIEWS_PER_RUN });
});

app.post('/api/settings/min-views', (req, res) => {
  const { minViewsPerRun } = req.body;
  if (typeof minViewsPerRun !== 'number' || minViewsPerRun < 1) {
    return res.status(400).json({ error: 'Invalid minViewsPerRun value' });
  }
  MIN_VIEWS_PER_RUN = Math.floor(minViewsPerRun);
  console.log(`[SETTINGS] Min views per run updated to: ${MIN_VIEWS_PER_RUN}`);
  return res.json({ success: true, minViewsPerRun: MIN_VIEWS_PER_RUN });
});

/* =========================
   🔥 API: QUEUE STATUS
========================= */
app.get('/api/queues/status', (req, res) => {
  return res.json({
    views: {
      queueLength: viewsQueue.length,
      isExecuting: isExecuting.VIEWS,
      pending:     viewsQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time })),
    },
    likes: {
      queueLength: likesQueue.length,
      isExecuting: isExecuting.LIKES,
      pending:     likesQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time })),
    },
    shares: {
      queueLength: sharesQueue.length,
      isExecuting: isExecuting.SHARES,
      pending:     sharesQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time })),
    },
    saves: {
      queueLength: savesQueue.length,
      isExecuting: isExecuting.SAVES,
      pending:     savesQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time })),
    },
    comments: {
      queueLength: commentsQueue.length,
      isExecuting: isExecuting.COMMENTS,
      pending:     commentsQueue.map(r => ({ id: r.id, quantity: r.quantity, time: r.time })),
    },
  });
});

/* =========================
   🔥 API: RETRY STUCK RUNS
========================= */
app.post('/api/runs/retry-stuck', async (req, res) => {
  try {
    let resetCount = 0;

    // 🔥 Reset queued runs that are not in memory queue (orphaned after restart)
    const orphanedQueued = await Run.find({ status: 'queued' }).lean();
    const orphanedIds    = orphanedQueued
      .filter(r => !isRunInQueue(r.id))
      .map(r => r._id);

    if (orphanedIds.length > 0) {
      await Run.updateMany(
        { _id: { $in: orphanedIds } },
        { $set: { status: 'pending' } }
      );
      resetCount += orphanedIds.length;
    }

    // 🔥 Reset stuck processing runs (no executedAt, older than 15 min)
    const fifteenMinAgo = new Date(Date.now() - 15 * 60 * 1000);
    const stuckResult   = await Run.updateMany(
      {
        status:    'processing',
        executedAt: null,
        createdAt: { $lt: fifteenMinAgo },
      },
      { $set: { status: 'pending', error: null } }
    );
    resetCount += stuckResult.modifiedCount;

    return res.json({
      success:    true,
      resetCount,
      message:    `Reset ${resetCount} stuck runs`,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 API: MANUAL SCHEDULER TRIGGER
========================= */
app.post('/api/scheduler/trigger', async (req, res) => {
  try {
    const now = new Date();
    let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0 };

    const pendingRuns = await Run.find({
      done:   false,
      status: 'pending',
      time:   { $lte: now },
    })
    .limit(50)
    .lean();

    // Batch fetch orders
    const uniqueOrderIds = [...new Set(pendingRuns.map(r => r.schedulerOrderId))];
    const activeOrders   = await Order.find(
      { schedulerOrderId: { $in: uniqueOrderIds } },
      { schedulerOrderId: 1, status: 1 }
    ).lean();
    const orderStatusMap = new Map(activeOrders.map(o => [o.schedulerOrderId, o.status]));

    const bulkOps = [];

    for (const run of pendingRuns) {
      if (isRunInQueue(run.id)) continue;

      const orderStatus = orderStatusMap.get(run.schedulerOrderId);
      if (!orderStatus || orderStatus === 'cancelled' || orderStatus === 'paused') continue;

      const pushed = pushToQueue(run);
      if (!pushed) continue;

      bulkOps.push({
        updateOne: {
          filter: { _id: run._id },
          update: { $set: { status: 'queued' } },
        },
      });

      if (run.label === 'VIEWS')    addedToQueue.views++;
      if (run.label === 'LIKES')    addedToQueue.likes++;
      if (run.label === 'SHARES')   addedToQueue.shares++;
      if (run.label === 'SAVES')    addedToQueue.saves++;
      if (run.label === 'COMMENTS') addedToQueue.comments++;
    }

    if (bulkOps.length > 0) await Run.bulkWrite(bulkOps);

    if (viewsQueue.length > 0    && !isExecuting.VIEWS)    processViewsQueue();
    if (likesQueue.length > 0    && !isExecuting.LIKES)    processLikesQueue();
    if (sharesQueue.length > 0   && !isExecuting.SHARES)   processSharesQueue();
    if (savesQueue.length > 0    && !isExecuting.SAVES)    processSavesQueue();
    if (commentsQueue.length > 0 && !isExecuting.COMMENTS) processCommentsQueue();

    return res.json({
      success:      true,
      addedToQueue,
      currentQueues: {
        views:    viewsQueue.length,
        likes:    likesQueue.length,
        shares:   sharesQueue.length,
        saves:    savesQueue.length,
        comments: commentsQueue.length,
      },
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

/* =========================
   🔥 API: HEALTH CHECK
========================= */
app.get('/api/health', (req, res) => {
  return res.json({
    status:        'ok',
    mongoConnected: mongoose.connection.readyState === 1,
    uptime:        process.uptime(),
    minViewsPerRun: MIN_VIEWS_PER_RUN,
    queues: {
      views:    viewsQueue.length,
      likes:    likesQueue.length,
      shares:   sharesQueue.length,
      saves:    savesQueue.length,
      comments: commentsQueue.length,
    },
    executing: {
      views:    isExecuting.VIEWS,
      likes:    isExecuting.LIKES,
      shares:   isExecuting.SHARES,
      saves:    isExecuting.SAVES,
      comments: isExecuting.COMMENTS,
    },
  });
});

/* =========================
   🔥 KEEP ALIVE PING
   Prevents Render free tier sleep
========================= */
const BACKEND_URL = process.env.BACKEND_URL || 'https://iamsuperman-backend.onrender.com';
setInterval(async () => {
  try {
    await axios.get(`${BACKEND_URL}/api/health`, { timeout: 10000 });
    console.log('[PING] ✅ Keep-alive successful');
  } catch (e) {
    console.warn('[PING] ⚠️ Keep-alive failed:', e.message);
  }
}, 5 * 60 * 1000);
