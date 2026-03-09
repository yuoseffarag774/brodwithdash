/**
 * Broadcast Bot — Dashboard + Full Bot Commands
 * One file: Express dashboard + Discord OAuth2 + Bot commands + WebSocket broadcast
 */

require("dotenv").config();

const express      = require("express");
const session      = require("express-session");
const axios        = require("axios");
const { WebSocketServer } = require("ws");
const {
  REST, Routes, Client, GatewayIntentBits,
  ButtonStyle, ActionRowBuilder, ModalBuilder,
  TextInputBuilder, TextInputStyle, PermissionFlagsBits,
} = require("discord.js");
const fs   = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");
const { randomBytes } = require("crypto");

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

// ═══════════════════════════════════════════════════════════════════════════════
//  CONFIG
// ═══════════════════════════════════════════════════════════════════════════════
const PORT           = process.env.DASH_PORT || 3000;
const CLIENT_ID      = process.env.DISCORD_CLIENT_ID;
const CLIENT_SECRET  = process.env.DISCORD_CLIENT_SECRET;
// Also accept /dashboard as redirect (matches Discord Portal setting)
const REDIRECT_URI = process.env.DISCORD_REDIRECT_URI || `http://localhost:${PORT}/auth/callback`;
// Auto-detect: if Discord Portal sends to /dashboard, use that in token exchange
function getRedirectUri(req) {
  const host = `${req.protocol}://${req.get('host')}`;
  if (req.path === '/dashboard') return `${host}/dashboard`;
  return REDIRECT_URI;
}
const SESSION_SECRET = process.env.SESSION_SECRET || randomBytes(32).toString("hex");

// ═══════════════════════════════════════════════════════════════════════════════
//  DATA HELPERS
// ═══════════════════════════════════════════════════════════════════════════════
const DATA_FILE = path.join(__dirname, "data.json");

function loadData() {
  if (!fs.existsSync(DATA_FILE))
    return { tokens: [], groups: [], owners: [], botOwnerId: null, roles: {}, twofa: {} };
  const d = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  if (!d.owners)            d.owners    = [];
  if (!("botOwnerId" in d)) d.botOwnerId = null;
  if (!d.roles)             d.roles     = {};
  if (!d.twofa)             d.twofa     = {};
  return d;
}
function saveData(d) { fs.writeFileSync(DATA_FILE, JSON.stringify(d, null, 2)); }

// ─── Role helpers ──────────────────────────────────────────────────────────────
function getUserRole(uid) {
  const d = loadData();
  if (d.botOwnerId === uid) return "owner";
  return d.roles[uid] || (d.owners.includes(uid) ? "admin" : null);
}
function requireRole(minRole) {
  const levels = { owner:4, admin:3, mod:2, viewer:1 };
  return (req, res, next) => {
    if (!req.session.user) return res.status(401).json({ error:"Not authenticated" });
    const role = getUserRole(req.session.user.id);
    if (!role) return res.status(403).json({ error:"No permission" });
    if ((levels[role]||0) < (levels[minRole]||0)) return res.status(403).json({ error:`Requires ${minRole} role` });
    req.userRole = role;
    next();
  };
}
function generateGroupId(groups) {
  let id;
  do { id = randomBytes(4).toString("hex"); } while (groups.some(g => g.id === id));
  return id;
}

// ═══════════════════════════════════════════════════════════════════════════════
//  BOT CONSTANTS & CV2 BUILDERS
// ═══════════════════════════════════════════════════════════════════════════════
const CV2_FLAG = 1 << 15;
const T = { ACTION_ROW:1, BUTTON:2, SELECT:3, CONTAINER:17, TEXT:10, SEPARATOR:14, THUMBNAIL:11, SECTION:9 };
const COLORS = { blurple:0x5865F2, green:0x57F287, yellow:0xFEE75C, red:0xED4245, orange:0xFF9500, grey:0x4F545C };

const cvText     = (c)          => ({ type: T.TEXT,      content: c });
const cvSep      = (d=true,s=2) => ({ type: T.SEPARATOR, divider: d, spacing: s });
const cvBtn      = (id,label,style=ButtonStyle.Primary,disabled=false) => ({ type:T.BUTTON, custom_id:id, label, style, disabled });
const cvLinkBtn  = (url,label)  => ({ type: T.BUTTON, style:ButtonStyle.Link, label, url });
const cvRow      = (...c)       => ({ type: T.ACTION_ROW, components: c });
const cvThumb    = (url)        => ({ type: T.THUMBNAIL,  media: { url } });
const cvSection  = (content,thumbUrl) => ({ type:T.SECTION, components:[{type:T.TEXT,content}], accessory:cvThumb(thumbUrl) });
const cvContainer= (color,...c) => ({ type: T.CONTAINER, accent_color:color, components:c });

function botAvatarUrl() {
  const u = discordClient.user;
  return u ? u.displayAvatarURL({ size:64, extension:"png" }) : null;
}
function footerLine(label, mentionId) { return cvText(`-# ${label}  •  <@${mentionId}>`); }
function progressBar(done, total, width=12) {
  if (total===0) return `\`${"░".repeat(width)}\` 0%`;
  const pct=done/total, filled=Math.round(pct*width);
  return `\`${"█".repeat(filled)}${"░".repeat(width-filled)}\` ${(pct*100).toFixed(0)}%`;
}
function cvNotice(color, title, lines=[], withBack=true) {
  const body  = lines.map(([k,v]) => `**${k}:** ${v}`).join("\n");
  const parts = [
    cvText(`### ${title}`), cvSep(false,1),
    ...(body ? [cvText(body)] : []),
    ...(withBack ? [cvSep(false,1), cvRow(cvBtn("btn_refresh_panel","← Back to Panel",ButtonStyle.Secondary))] : []),
  ];
  return { flags:CV2_FLAG, components:[cvContainer(color,...parts)] };
}

// Bot session store
const botSessions = new Map();

// ═══════════════════════════════════════════════════════════════════════════════
//  DISCORD CLIENT
// ═══════════════════════════════════════════════════════════════════════════════
const discordClient = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildPresences,
  ],
});

discordClient.once("ready", () => {
  console.log(`[Bot] ✅ Logged in as ${discordClient.user.tag}`);
  console.log(`[Bot] Commands: $help | $panel | $bc | $obc | $rbc | $groups | $tokens | $owners`);
  // Load scheduled broadcasts on bot ready
  scheduleAllOnStartup();
  console.log(`[SCHED] ✅ Scheduled broadcasts loaded`);
});

discordClient.login(process.env.DISCORD_TOKEN).catch(e => {
  console.error("[Bot] Login failed:", e.message);
});

// ═══════════════════════════════════════════════════════════════════════════════
//  SCREEN BUILDERS
// ═══════════════════════════════════════════════════════════════════════════════
function buildPanel(guild, data, callerId) {
  const memberCount = guild?.memberCount ?? 0;
  const tokenCount  = data.tokens.length;
  const ratio       = tokenCount > 0 ? (memberCount/tokenCount).toFixed(2) : "∞";
  const groupLines  = data.groups.length > 0
    ? data.groups.map(g => `> 📦 **${g.name}** — ${g.tokenCount} tokens`).join("\n")
    : "> _لا توجد مجموعات_";
  const av     = botAvatarUrl();
  const header = "# 🚀 Broadcast Control Panel\nManage broadcast bots, groups, and send mass messages.";
  return { flags:CV2_FLAG, components:[cvContainer(COLORS.blurple,
    ...(av ? [cvSection(header,av)] : [cvText(header)]),
    cvSep(),
    cvText(["### 📊 Server Stats",`👥 **Members:** \`${memberCount.toLocaleString()}\``,`🤖 **Tokens:** \`${tokenCount}\``,`📈 **Ratio:** \`${ratio}\``].join("\n")),
    cvSep(),
    cvText(`### 📦 Groups\n${groupLines}`),
    cvSep(),
    cvRow(cvBtn("btn_add_tokens","➕ Add Tokens",ButtonStyle.Primary),cvBtn("btn_add_bots","🔗 Add Bots",ButtonStyle.Secondary),cvBtn("btn_select_group","📂 Select Group",ButtonStyle.Success)),
    cvRow(cvBtn("btn_create_group","🆕 Create Group",ButtonStyle.Secondary),cvBtn("btn_refresh_panel","🔄 Refresh",ButtonStyle.Secondary)),
    cvSep(false,1), footerLine("Broadcast Bot • Use buttons above to manage", callerId),
  )]};
}

function buildSelectGroupScreen(groups, mode="normal", callerId="0") {
  const customId  = mode==="obc" ? "obc_select_group" : mode==="bc" ? "bc_select_group" : "select_group";
  const modeLabel = mode==="obc" ? "🟢 Online-Only" : mode==="bc" ? "📢 All-Members" : "Broadcast";
  const select = { type:T.SELECT, custom_id:customId, placeholder:"Choose a group...",
    options:groups.map(g => ({ label:g.name, description:`${g.tokenCount} tokens`, value:g.name })) };
  const av     = botAvatarUrl();
  const header = `# 📂 Select Group — ${modeLabel}\nPick which group of bots to use.`;
  return { flags:CV2_FLAG, components:[cvContainer(COLORS.blurple,
    ...(av ? [cvSection(header,av)] : [cvText(header)]),
    cvSep(), cvText(groups.map(g => `> 📦 **${g.name}** — ${g.tokenCount} tokens`).join("\n")), cvSep(),
    { type:T.ACTION_ROW, components:[select] },
    cvRow(cvBtn("btn_refresh_panel","← Back",ButtonStyle.Secondary)),
    cvSep(false,1), footerLine("Broadcast Bot • Group Selection", callerId),
  )]};
}

function buildSpeedScreen(groupName, messagePreview, mode="normal", callerId="0") {
  const preview   = messagePreview.length > 180 ? messagePreview.slice(0,180)+"…" : messagePreview;
  const modeLabel = mode==="obc" ? "🟢 Online Only" : mode==="bc" ? "📢 All Members (BC)" : mode==="rbc" ? "🏷️ By Role" : "📢 All Members";
  const prefix    = mode==="obc" ? "obc_" : mode==="bc" ? "bc_" : "";
  const av        = botAvatarUrl();
  const header    = `# ⚡ Select Broadcast Speed\n**Group:** ${groupName}  •  **Target:** ${modeLabel}`;
  return { flags:CV2_FLAG, components:[cvContainer(COLORS.yellow,
    ...(av ? [cvSection(header,av)] : [cvText(header)]),
    cvSep(), cvText(`### 📝 Message Preview\n\`\`\`\n${preview}\n\`\`\``),
    cvSep(), cvText("### 🚦 Speed Options\n🐢 **Slow (5s)** — Safest\n🚶 **Normal (2s)** — Balanced\n🏃 **Fast (0.8s)** — Higher risk"),
    cvText("> ⚠️ Faster speeds may trigger Discord rate limits."), cvSep(),
    cvRow(cvBtn(`${prefix}speed_slow`,"🐢 Slow (5s)",ButtonStyle.Success),cvBtn(`${prefix}speed_normal`,"🚶 Normal (2s)",ButtonStyle.Primary),cvBtn(`${prefix}speed_fast`,"🏃 Fast (0.8s)",ButtonStyle.Danger)),
    cvRow(cvBtn("btn_refresh_panel","← Cancel",ButtonStyle.Secondary)),
    cvSep(false,1), footerLine("Broadcast Bot • Speed Selection", callerId),
  )]};
}

function buildProgressScreen(groupName, memberCount, botCount, delayMs, mode="normal", callerId="0") {
  const av = botAvatarUrl();
  return { flags:CV2_FLAG, components:[cvContainer(COLORS.orange,
    ...(av ? [cvSection("# 📡 Broadcast In Progress…",av)] : [cvText("# 📡 Broadcast In Progress…")]),
    cvText(`Broadcasting to **${memberCount}** members using **${botCount}** bots.`), cvSep(),
    cvText(`### ⚙️ Configuration\n📦 Group: **${groupName}**\n🎯 Target: **${mode==="obc"?"🟢 Online Only":"📢 All Members"}**\n⏱️ Delay: **${delayMs}ms** per message\n\n_Please wait…_`),
    cvSep(false,1), footerLine("Broadcast Bot • Running", callerId),
  )]};
}

function buildStatsScreen(groupName, stats, mode="normal", callerId="0") {
  const totalSent   = stats.reduce((a,b) => a+b.sent,   0);
  const totalFailed = stats.reduce((a,b) => a+b.failed, 0);
  const successRate = totalSent+totalFailed > 0 ? ((totalSent/(totalSent+totalFailed))*100).toFixed(1) : "0";
  const color       = parseFloat(successRate) >= 90 ? COLORS.green : COLORS.yellow;
  const av          = botAvatarUrl();
  const perBot      = stats.map(s => [
    `**${s.label}** (\`${s.token}\`)`,
    `${progressBar(s.sent,s.assignedCount)}  ${s.completed?"✅ Done":"⏳ Running"}`,
    `✅ \`${s.sent}\`  ❌ \`${s.failed}\`  👥 \`${s.assignedCount}\``,
  ].join("\n")).join("\n\n");
  return { flags:CV2_FLAG, components:[cvContainer(color,
    ...(av ? [cvSection("# 📊 Broadcast Statistics",av)] : [cvText("# 📊 Broadcast Statistics")]),
    cvText(`**Group:** ${groupName}  •  **Mode:** ${mode==="obc"?"🟢 Online-Only":"📢 All Members"}`), cvSep(),
    cvText(`### 🏆 Overall Results\n✅ Total Sent: \`${totalSent}\`\n❌ Total Failed: \`${totalFailed}\`\n📈 Success Rate: \`${successRate}%\``),
    cvSep(), cvText("### 🤖 Per-Bot Breakdown"), cvText(perBot || "_No bots ran._"), cvSep(),
    cvRow(cvBtn("btn_refresh_panel","🏠 Back to Panel",ButtonStyle.Primary)),
    cvSep(false,1), footerLine("Broadcast Bot • Broadcast Complete", callerId),
  )]};
}

// ═══════════════════════════════════════════════════════════════════════════════
//  BROADCAST ENGINE (Bot commands)
// ═══════════════════════════════════════════════════════════════════════════════
async function runBotBroadcast(interaction, sess, delayMs, mode="normal") {
  const data  = loadData();
  const group = data.groups.find(g => g.name === sess.selectedGroup);
  if (!group) return interaction.reply({ content:"❌ Group not found.", ephemeral:true });

  const groupTokens = data.tokens.slice(0, group.tokenCount);
  if (!groupTokens.length) return interaction.reply({ content:"❌ No tokens for this group.", ephemeral:true });

  const allMembers = await interaction.guild.members.fetch();
  let   memberList = [...allMembers.values()].filter(m => !m.user.bot);

  if (mode === "obc") {
    memberList = memberList.filter(m => m.presence && ["online","idle","dnd"].includes(m.presence.status));
  }
  if (mode === "rbc" && sess.roleId) {
    memberList = memberList.filter(m => m.roles.cache.has(sess.roleId));
  }
  if (memberList.length === 0) {
    return interaction.update({ flags:CV2_FLAG, components:[cvContainer(COLORS.yellow,
      cvText("# ⚠️ No Members to Broadcast To"),
      cvText(mode==="obc" ? "No online members found. Try `$bc` instead." : "No members found in this server."),
      cvSep(false,1), cvRow(cvBtn("btn_refresh_panel","← Back to Panel",ButtonStyle.Secondary)),
      footerLine("Broadcast Bot", interaction.user.id),
    )]});
  }

  const perBot = Math.ceil(memberList.length / groupTokens.length);
  await interaction.update(buildProgressScreen(group.name, memberList.length, groupTokens.length, delayMs, mode, interaction.user.id));

  const stats = groupTokens.map((token, i) => {
    const slice = memberList.slice(i*perBot, (i+1)*perBot);
    return { label:`Bot #${i+1}`, token:token.slice(0,12)+"…", rawToken:token, sent:0, failed:0, assignedCount:slice.length, completed:false, slice };
  });

  await Promise.all(stats.map(async (s) => {
    const rest = new REST({ version:"10" }).setToken(s.rawToken);
    try { await rest.get(Routes.user()); } catch { s.failed = s.slice.length; s.completed = true; return; }
    for (const member of s.slice) {
      await sleep(delayMs);
      try {
        const dm = await rest.post(Routes.userChannels(), { body:{ recipient_id: member.user.id } });
        await rest.post(Routes.channelMessages(dm.id), { body:{ content: sess.broadcastMessage + `\n<@${member.user.id}>` } });
        s.sent++;
      } catch { s.failed++; }
    }
    s.completed = true;
  }));

  const cleanStats = stats.map(s => ({ label:s.label, token:s.token, sent:s.sent, failed:s.failed, assignedCount:s.assignedCount, completed:s.completed }));
  try { await interaction.editReply(buildStatsScreen(group.name, cleanStats, mode, interaction.user.id)); } catch {}
}

function fetchImageAsBase64(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith("https") ? require("https") : require("http");
    mod.get(url, res => {
      const chunks = [];
      res.on("data", c => chunks.push(c));
      res.on("end", () => resolve(`data:${res.headers["content-type"]||"image/png"};base64,${Buffer.concat(chunks).toString("base64")}`));
      res.on("error", reject);
    }).on("error", reject);
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
//  BOT — MESSAGE COMMANDS
// ═══════════════════════════════════════════════════════════════════════════════
discordClient.on("messageCreate", async (message) => {
  if (message.author.bot) return;
  const content   = message.content.trim();
  const isAdmin   = message.member?.permissions.has(PermissionFlagsBits.Administrator);
  const data0     = loadData();
  const isOwner   = data0.botOwnerId === message.author.id || data0.owners.includes(message.author.id);
  const hasAccess = isAdmin || isOwner;
  const uid       = message.author.id;

  const protectedCmds = ["$panel","$bc","$obc","$rbc","$groups","$remove-group","$tokens","$set-avatar","$set-name","$add-owner","$remove-owner","$owners"];
  if (!hasAccess && protectedCmds.some(c => content.startsWith(c)))
    return message.reply("❌ You don't have permission to use this command.");

  // ── $help ──────────────────────────────────────────────────────────────────
  if (content === "$help") {
    const cmds = [
      ["$panel","فتح لوحة التحكم الرئيسية"],
      ["$bc `<رسالة>`","بروادكاست لـ **كل الأعضاء**"],
      ["$obc `<رسالة>`","بروادكاست لـ **الأونلاين فقط**"],
      ["$rbc `<رتبة_id> <رسالة>`","بروادكاست لـ **أعضاء رتبة محددة فقط**"],
      ["$groups","عرض كل المجموعات مع ID"],
      ["$remove-group `<id|all>`","حذف مجموعة أو كلها"],
      ["$tokens","عرض كل التوكنات"],
      ["$add-owner `<user_id>`","إضافة owner"],
      ["$remove-owner `<user_id>`","إزالة owner"],
      ["$owners","عرض قائمة الـ owners"],
      ["$set-avatar `<url>`","تغيير الأفاتار لكل البوتات"],
      ["$set-name `<اسم>`","تغيير اسم كل البوتات (2–32 حرف)"],
      ["$status","إحصائيات البوت المباشرة"],
    ];
    const av = botAvatarUrl();
    return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.blurple,
      ...(av ? [cvSection("# 📖 Broadcast Bot — Commands",av)] : [cvText("# 📖 Broadcast Bot — Commands")]),
      cvText("كل الأوامر تبدأ بـ `$` — تحتاج صلاحية **Administrator** أو **Owner**."),
      cvSep(), cvText(cmds.map(([c,d]) => `> **${c}**\n> ${d}`).join("\n\n")),
      cvSep(false,1), footerLine("Broadcast Bot • Help", uid),
    )]});
  }

  // ── $panel ─────────────────────────────────────────────────────────────────
  if (content === "$panel") {
    return message.channel.send(buildPanel(message.guild, loadData(), uid));
  }

  // ── $status ────────────────────────────────────────────────────────────────
  if (content === "$status") {
    const data = loadData();
    const logs = loadLogs();
    const totalSent   = logs.reduce((a,l)=>a+(l.sent||0),0);
    const totalFailed = logs.reduce((a,l)=>a+(l.failed||0),0);
    const totalBC     = logs.length;
    const rate        = totalSent+totalFailed>0 ? ((totalSent/(totalSent+totalFailed))*100).toFixed(1)+"%" : "—";
    const pending     = loadSchedules().filter(s=>s.status==="pending").length;
    const bl          = loadBlacklist().length;
    const av          = botAvatarUrl();
    const uptime      = process.uptime();
    const uptimeStr   = `${Math.floor(uptime/3600)}h ${Math.floor((uptime%3600)/60)}m`;
    return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.blurple,
      ...(av ? [cvSection("# 📊 Broadcast Bot — Status",av)] : [cvText("# 📊 Broadcast Bot — Status")]),
      cvSep(),
      cvText([
        "### 🤖 Bot Info",
        `> 🔑 **Tokens:** \`${data.tokens.length}\`  •  📦 **Groups:** \`${data.groups.length}\``,
        `> 👑 **Owners:** \`${data.owners.length}\`  •  🚫 **Blacklist:** \`${bl}\``,
        `> ⏱️ **Uptime:** \`${uptimeStr}\``,
      ].join("\n")),
      cvSep(),
      cvText([
        "### 📨 Broadcast Stats",
        `> ✅ **Total Sent:** \`${totalSent.toLocaleString()}\``,
        `> ❌ **Total Failed:** \`${totalFailed.toLocaleString()}\``,
        `> 📢 **Total Broadcasts:** \`${totalBC}\``,
        `> 📈 **Success Rate:** \`${rate}\``,
        `> ⏰ **Pending Schedules:** \`${pending}\``,
      ].join("\n")),
      cvSep(false,1), footerLine("Broadcast Bot • Status", uid),
    )]});
  }

  // ── $obc <message> ─────────────────────────────────────────────────────────
  if (content.startsWith("$obc ") || content === "$obc") {
    const broadcastMsg = content.slice(5).trim();
    if (!broadcastMsg) return message.reply("❌ الاستخدام: `$obc <رسالتك>`");
    const data = loadData();
    if (!data.groups.length) return message.reply("❌ ما في مجموعات. افتح `$panel` وأنشئ مجموعة أولاً.");
    botSessions.set(uid, { broadcastMessage:broadcastMsg, mode:"obc", callerId:uid });
    return message.channel.send(buildSelectGroupScreen(data.groups, "obc", uid));
  }

  // ── $bc <message> ──────────────────────────────────────────────────────────
  if (content.startsWith("$bc ") || content === "$bc") {
    const broadcastMsg = content.slice(4).trim();
    if (!broadcastMsg) return message.reply("❌ الاستخدام: `$bc <رسالتك>`");
    const data = loadData();
    if (!data.groups.length) return message.reply("❌ ما في مجموعات. افتح `$panel` وأنشئ مجموعة أولاً.");
    botSessions.set(uid, { broadcastMessage:broadcastMsg, mode:"bc", callerId:uid });
    return message.channel.send(buildSelectGroupScreen(data.groups, "bc", uid));
  }


  // ── $rbc <role_id|role_name> <message> ────────────────────────────────────
  if (content.startsWith("$rbc ") || content === "$rbc") {
    const rest = content.slice(5).trim();
    if (!rest) return message.reply("❌ الاستخدام: `$rbc <role_id_or_name> <رسالتك>`\n\nأو استخدم `$rbc list` لعرض الرتب المتاحة.");

    if (rest === "list") {
      const rolesCol = await message.guild.roles.fetch();
      const roles    = [...rolesCol.values()]
        .filter(r => r.name !== "@everyone")
        .sort((a,b) => b.position - a.position)
        .slice(0,25);
      const data = loadData();
      if (!data.groups.length) return message.reply("❌ ما في مجموعات.");
      const av = botAvatarUrl();
      botSessions.set(uid, { mode:"rbc", callerId:uid });
      return message.channel.send(buildRoleSelectScreen(roles.map(r=>({id:r.id,name:r.name,memberCount:r.members.size})), uid));
    }

    // Parse: first word = roleId or role name, rest = message
    const parts = rest.split(" ");
    let roleId, broadcastMsg;
    const guild = message.guild;
    // Try direct role ID
    const rolesCol = await guild.roles.fetch();
    const byId = rolesCol.get(parts[0]);
    if (byId) {
      roleId       = parts[0];
      broadcastMsg = parts.slice(1).join(" ");
    } else {
      // Try role name (first word)
      const byName = [...rolesCol.values()].find(r => r.name.toLowerCase() === parts[0].toLowerCase());
      if (byName) { roleId = byName.id; broadcastMsg = parts.slice(1).join(" "); }
      else { return message.reply(`❌ ما وجدنا رتبة بـ ID أو اسم \`${parts[0]}\`\nاستخدم \`$rbc list\` لعرض الرتب.`); }
    }

    if (!broadcastMsg) return message.reply("❌ اكتب الرسالة بعد اسم/ID الرتبة");
    const data = loadData();
    if (!data.groups.length) return message.reply("❌ ما في مجموعات. افتح `$panel` وأنشئ مجموعة أولاً.");
    botSessions.set(uid, { broadcastMessage:broadcastMsg, mode:"rbc", roleId, callerId:uid });
    return message.channel.send(buildSelectGroupScreen(data.groups, "rbc", uid));
  }

  // ── $groups ────────────────────────────────────────────────────────────────
  if (content === "$groups") {
    const data = loadData();
    if (!data.groups.length) return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.grey,
      cvText("# 📦 Groups"), cvText("_ما في مجموعات بعد._"), cvSep(false,1), footerLine("Broadcast Bot • Groups",uid),
    )]});
    let dirty = false;
    for (const g of data.groups) { if (!g.id) { g.id = generateGroupId(data.groups); dirty = true; } }
    if (dirty) saveData(data);
    const av = botAvatarUrl();
    return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.blurple,
      ...(av ? [cvSection(`# 📦 Groups — ${data.groups.length} مجموعة`,av)] : [cvText(`# 📦 Groups — ${data.groups.length} مجموعة`)]),
      cvSep(), cvText(data.groups.map(g => `📦 **${g.name}**\n> 🆔 \`${g.id}\`  •  🤖 ${g.tokenCount} tokens`).join("\n\n")),
      cvSep(false,1), footerLine("Broadcast Bot • Groups  •  انسخ ID ثم `$remove-group <id>`", uid),
    )]});
  }

  // ── $remove-group ──────────────────────────────────────────────────────────
  if (content.startsWith("$remove-group")) {
    const arg = content.slice(13).trim();
    if (!arg) return message.reply("❌ الاستخدام: `$remove-group <id>` أو `$remove-group all`");
    const data = loadData();
    if (arg === "all") {
      const count = data.groups.length; data.groups = []; saveData(data);
      return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.red,
        cvText("# 🗑️ تم حذف كل المجموعات"), cvText(`تم مسح **${count}** مجموعة.`),
        cvSep(false,1), footerLine("Broadcast Bot • Group Management",uid),
      )]});
    }
    const idx = data.groups.findIndex(g => g.id === arg);
    if (idx === -1) return message.reply(`❌ ما في مجموعة بهذا الـ ID \`${arg}\`.`);
    const removed = data.groups.splice(idx,1)[0]; saveData(data);
    return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.red,
      cvText("# 🗑️ تم حذف المجموعة"), cvText(`المجموعة **${removed.name}** (\`${removed.id}\`) تم مسحها.`),
      cvSep(false,1), footerLine("Broadcast Bot • Group Management",uid),
    )]});
  }

  // ── $tokens ────────────────────────────────────────────────────────────────
  if (content === "$tokens") {
    const data = loadData();
    if (!data.tokens.length) return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.grey,
      cvText("# 🤖 Tokens"), cvText("_ما في توكنات بعد._"), cvSep(false,1), footerLine("Broadcast Bot • Tokens",uid),
    )]});
    const statusMsg = await message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.orange,
      cvText("# 🤖 Registered Tokens"), cvText(`جاري فحص **${data.tokens.length}** توكن…`),
      cvSep(false,1), footerLine("Broadcast Bot • Tokens",uid),
    )]});
    const rest2 = new REST({ version:"10" }); const guildId = message.guild.id; const infos = [];
    for (let i = 0; i < data.tokens.length; i++) {
      const token = data.tokens[i];
      try {
        rest2.setToken(token);
        const app = await rest2.get(Routes.currentApplication());
        let inServer = false;
        try { await rest2.get(Routes.guildMember(guildId, app.id)); inServer = true; } catch {}
        const groups = data.groups.filter(g => data.tokens.slice(0,g.tokenCount).includes(token)).map(g => g.name);
        infos.push({ index:i, name:app.username, valid:true, inServer, groups, snippet:token.slice(0,12)+"…" });
      } catch { infos.push({ index:i, valid:false, snippet:token.slice(0,12)+"…", groups:[] }); }
    }
    const av = botAvatarUrl(); const rows = [];
    for (const t of infos) {
      const badge = t.valid ? (t.inServer ? "🟢 Valid + In Server" : "🟡 Valid (not in server)") : "🔴 Invalid";
      rows.push(cvText(`**#${t.index+1}** \`${t.snippet}\` — ${t.valid?t.name:"Unknown"}\n${badge}  •  Groups: ${t.groups.join(", ")||"—"}`));
      rows.push(cvRow(cvBtn(`del_token_${t.index}__${t.snippet.replace("…","")}`,`🗑️ Delete #${t.index+1}`,ButtonStyle.Danger)));
    }
    return statusMsg.edit({ flags:CV2_FLAG, components:[cvContainer(COLORS.blurple,
      ...(av ? [cvSection(`# 🤖 Tokens (${infos.length})`,av)] : [cvText(`# 🤖 Tokens (${infos.length})`)]),
      cvSep(), ...rows, cvSep(false,1), footerLine("Broadcast Bot • Tokens",uid),
    )]});
  }

  // ── $add-owner ─────────────────────────────────────────────────────────────
  if (content.startsWith("$add-owner")) {
    const targetId = content.slice(10).trim();
    if (!/^\d{17,20}$/.test(targetId)) return message.reply("❌ الاستخدام: `$add-owner <user_id>`");
    const data = loadData();
    if (!data.botOwnerId) data.botOwnerId = uid;
    if (data.botOwnerId !== uid && !isAdmin) return message.reply("❌ فقط صاحب البوت أو Admin يقدر يضيف owners.");
    if (!data.owners.includes(targetId)) data.owners.push(targetId);
    saveData(data);
    return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.green,
      cvText(`# ✅ تمت إضافة Owner\n<@${targetId}> أصبح لديه صلاحية استخدام البوت.`),
      cvSep(false,1), footerLine("Broadcast Bot • Owner Management",uid),
    )]});
  }

  // ── $remove-owner ──────────────────────────────────────────────────────────
  if (content.startsWith("$remove-owner")) {
    const targetId = content.slice(13).trim();
    if (!/^\d{17,20}$/.test(targetId)) return message.reply("❌ الاستخدام: `$remove-owner <user_id>`");
    const data = loadData();
    if (data.botOwnerId !== uid && !isAdmin) return message.reply("❌ فقط صاحب البوت أو Admin.");
    if (targetId === data.botOwnerId) return message.reply("❌ لا يمكن إزالة صاحب البوت.");
    data.owners = data.owners.filter(id => id !== targetId); saveData(data);
    return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.red,
      cvText(`# 🚫 تم إزالة Owner\n<@${targetId}> لم يعد لديه صلاحية.`),
      cvSep(false,1), footerLine("Broadcast Bot • Owner Management",uid),
    )]});
  }

  // ── $owners ────────────────────────────────────────────────────────────────
  if (content === "$owners") {
    const data = loadData(); const av = botAvatarUrl();
    return message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.blurple,
      ...(av ? [cvSection("# 👑 Bot Owners",av)] : [cvText("# 👑 Bot Owners")]),
      cvSep(),
      cvText(data.botOwnerId ? `👑 **صاحب البوت:** <@${data.botOwnerId}>` : "👑 **صاحب البوت:** _لم يُحدَّد بعد_"),
      cvSep(false,1),
      cvText(`### 🛡️ Owners\n${data.owners.length ? data.owners.map((id,i)=>`> ${i+1}. <@${id}>`).join("\n") : "> _لا يوجد owners_"}`),
      cvSep(false,1), footerLine("Broadcast Bot • Owners",uid),
    )]});
  }

  // ── $set-avatar ────────────────────────────────────────────────────────────
  if (content.startsWith("$set-avatar ") || content === "$set-avatar") {
    const url = content.slice(12).trim();
    if (!url || !url.startsWith("http")) return message.reply("❌ الاستخدام: `$set-avatar <image_url>`");
    const data = loadData();
    if (!data.tokens.length) return message.reply("❌ ما في توكنات.");
    const av = botAvatarUrl();
    const statusMsg = await message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.orange,
      ...(av?[cvSection("# 🎨 Updating Avatars…",av)]:[cvText("# 🎨 Updating Avatars…")]),
      cvText(`جاري التحديث على **${data.tokens.length}** بوت…`), cvSep(false,1), footerLine("Broadcast Bot",uid),
    )]});
    let imageData;
    try { imageData = await fetchImageAsBase64(url); }
    catch { return statusMsg.edit({ ...cvNotice(COLORS.red,"❌ Failed to fetch image",[["URL",url]],false), flags:CV2_FLAG }); }
    const results = []; const rest3 = new REST({ version:"10" });
    for (const token of data.tokens) {
      try { rest3.setToken(token); const a = await rest3.get(Routes.currentApplication()); await rest3.patch(Routes.user(),{body:{avatar:imageData}}); results.push({name:a.username,ok:true}); }
      catch(e) { results.push({name:token.slice(0,12)+"…",ok:false,err:e.message}); }
      await sleep(1500);
    }
    const ok = results.filter(r=>r.ok).length; const av2 = botAvatarUrl();
    return statusMsg.edit({ flags:CV2_FLAG, components:[cvContainer(ok===results.length?COLORS.green:COLORS.yellow,
      ...(av2?[cvSection("# 🎨 Avatar Update Complete",av2)]:[cvText("# 🎨 Avatar Update Complete")]),
      cvSep(), cvText(`✅ Updated: \`${ok}\`  •  ❌ Failed: \`${results.length-ok}\``),
      cvSep(), cvText(results.map(r=>r.ok?`> ✅ **${r.name}**`:`> ❌ \`${r.name}\` — ${r.err}`).join("\n")),
      cvSep(false,1), footerLine("Broadcast Bot • Avatar Update",uid),
    )]});
  }

  // ── $set-name ──────────────────────────────────────────────────────────────
  if (content.startsWith("$set-name ") || content === "$set-name") {
    const newName = content.slice(10).trim();
    if (!newName || newName.length < 2 || newName.length > 32) return message.reply("❌ الاستخدام: `$set-name <اسم>` (2–32 حرف)");
    const data = loadData();
    if (!data.tokens.length) return message.reply("❌ ما في توكنات.");
    const av = botAvatarUrl();
    const statusMsg = await message.channel.send({ flags:CV2_FLAG, components:[cvContainer(COLORS.orange,
      ...(av?[cvSection("# ✏️ Updating Names…",av)]:[cvText("# ✏️ Updating Names…")]),
      cvText(`جاري تطبيق الاسم **${newName}** على **${data.tokens.length}** بوت…`), cvSep(false,1), footerLine("Broadcast Bot",uid),
    )]});
    const results = []; const rest4 = new REST({ version:"10" });
    for (const token of data.tokens) {
      try { rest4.setToken(token); const before = await rest4.get(Routes.currentApplication()); await rest4.patch(Routes.user(),{body:{username:newName}}); results.push({oldName:before.username,ok:true}); }
      catch(e) { results.push({oldName:token.slice(0,12)+"…",ok:false,err:e.message}); }
      await sleep(2000);
    }
    const ok = results.filter(r=>r.ok).length; const av2 = botAvatarUrl();
    return statusMsg.edit({ flags:CV2_FLAG, components:[cvContainer(ok===results.length?COLORS.green:COLORS.yellow,
      ...(av2?[cvSection("# ✏️ Name Update Complete",av2)]:[cvText("# ✏️ Name Update Complete")]),
      cvSep(), cvText(`✅ Updated: \`${ok}\`  •  ❌ Failed: \`${results.length-ok}\``),
      cvSep(), cvText(results.map(r=>r.ok?`> ✅ **${r.oldName}** → **${newName}**`:`> ❌ \`${r.oldName}\` — ${r.err}`).join("\n")),
      cvSep(false,1), footerLine("Broadcast Bot • Name Update",uid),
    )]});
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
//  BOT — INTERACTION HANDLER
// ═══════════════════════════════════════════════════════════════════════════════
discordClient.on("interactionCreate", async (interaction) => {
  const uid = interaction.user.id;

  // ── BUTTONS ─────────────────────────────────────────────────────────────────
  if (interaction.isButton()) {
    const id = interaction.customId;

    if (id === "btn_refresh_panel") {
      return interaction.update(buildPanel(interaction.guild, loadData(), uid));
    }
    if (id === "btn_add_tokens") {
      const modal = new ModalBuilder().setCustomId("modal_add_tokens").setTitle("➕ Add Bot Tokens");
      modal.addComponents(new ActionRowBuilder().addComponents(
        new TextInputBuilder().setCustomId("tokens_input").setLabel("Tokens (one per line)")
          .setStyle(TextInputStyle.Paragraph).setPlaceholder("MTE4Nz...\nMTE4OD...").setRequired(true)
      ));
      return interaction.showModal(modal);
    }
    if (id === "btn_add_bots") {
      const data = loadData();
      if (!data.tokens.length) return interaction.reply({ content:"❌ No tokens yet.", ephemeral:true });
      await interaction.deferUpdate();
      const rest5 = new REST({ version:"10" }); const items = [];
      for (const token of data.tokens) {
        try { rest5.setToken(token); const a = await rest5.get(Routes.currentApplication()); items.push({valid:true,name:a.username,url:`https://discord.com/api/oauth2/authorize?client_id=${a.id}&permissions=536888320&scope=bot`,snippet:token.slice(0,15)}); }
        catch { items.push({valid:false,snippet:token.slice(0,15)}); }
      }
      const valid = items.filter(i => i.valid); const linkRows = [];
      for (let i = 0; i < valid.length; i+=5)
        linkRows.push(cvRow(...valid.slice(i,i+5).map(b => cvLinkBtn(b.url,`Invite ${b.name}`))));
      const av = botAvatarUrl();
      return interaction.editReply({ flags:CV2_FLAG, components:[cvContainer(COLORS.blurple,
        ...(av?[cvSection("# 🔗 Bot Invite Links",av)]:[cvText("# 🔗 Bot Invite Links")]),
        cvSep(), cvText(items.map(i=>i.valid?`> 🟢 **${i.name}**`:`> 🔴 Invalid: \`${i.snippet}\``).join("\n")),
        cvSep(), ...linkRows, cvRow(cvBtn("btn_refresh_panel","← Back",ButtonStyle.Secondary)),
        cvSep(false,1), footerLine("Broadcast Bot • Invite Links",uid),
      )]});
    }
    if (id === "btn_create_group") {
      const modal = new ModalBuilder().setCustomId("modal_create_group").setTitle("🆕 Create Group");
      modal.addComponents(
        new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId("group_name").setLabel("Group Name").setStyle(TextInputStyle.Short).setPlaceholder("e.g. Alpha Squad").setRequired(true)),
        new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId("group_token_count").setLabel("Number of Tokens").setStyle(TextInputStyle.Short).setPlaceholder("e.g. 5").setRequired(true))
      );
      return interaction.showModal(modal);
    }
    if (id === "btn_select_group") {
      const data = loadData();
      if (!data.groups.length) return interaction.reply({ content:"❌ No groups yet.", ephemeral:true });
      return interaction.update(buildSelectGroupScreen(data.groups, "normal", uid));
    }
    if (["speed_slow","speed_normal","speed_fast"].includes(id)) {
      const delay = {speed_slow:5000,speed_normal:2000,speed_fast:800}[id];
      const sess  = botSessions.get(uid);
      if (!sess?.selectedGroup||!sess?.broadcastMessage) return interaction.reply({ content:"❌ Session expired.", ephemeral:true });
      return runBotBroadcast(interaction, sess, delay, "normal");
    }
    if (["obc_speed_slow","obc_speed_normal","obc_speed_fast"].includes(id)) {
      const delay = {obc_speed_slow:5000,obc_speed_normal:2000,obc_speed_fast:800}[id];
      const sess  = botSessions.get(uid);
      if (!sess?.selectedGroup||!sess?.broadcastMessage) return interaction.reply({ content:"❌ Session expired.", ephemeral:true });
      return runBotBroadcast(interaction, sess, delay, "obc");
    }
    if (["bc_speed_slow","bc_speed_normal","bc_speed_fast"].includes(id)) {
      const delay = {bc_speed_slow:5000,bc_speed_normal:2000,bc_speed_fast:800}[id];
      const sess  = botSessions.get(uid);
      if (!sess?.selectedGroup||!sess?.broadcastMessage) return interaction.reply({ content:"❌ Session expired.", ephemeral:true });
      return runBotBroadcast(interaction, sess, delay, "bc");
    }
    if (["rbc_speed_slow","rbc_speed_normal","rbc_speed_fast"].includes(id)) {
      const delay = {rbc_speed_slow:5000,rbc_speed_normal:2000,rbc_speed_fast:800}[id];
      const sess  = botSessions.get(uid);
      if (!sess?.selectedGroup||!sess?.broadcastMessage) return interaction.reply({ content:"❌ Session expired.", ephemeral:true });
      return runBotBroadcast(interaction, sess, delay, "rbc");
    }
    if (id.startsWith("del_token_")) {
      const snippet = id.split("__")[1];
      const data    = loadData();
      const idx     = data.tokens.findIndex(t => t.startsWith(snippet));
      if (idx === -1) return interaction.reply({ content:"❌ Token not found.", ephemeral:true });
      const removed = data.tokens.splice(idx,1)[0]; saveData(data);
      return interaction.reply({ ...cvNotice(COLORS.red,"🗑️ Token Deleted",[["Token",`\`${removed.slice(0,24)}…\``],["Remaining",`\`${data.tokens.length}\``]]), ephemeral:true });
    }
  }

  // ── SELECT MENUS ─────────────────────────────────────────────────────────────
  if (interaction.isStringSelectMenu()) {
    if (interaction.customId === "select_group") {
      const groupName = interaction.values[0]; const sess = botSessions.get(uid) || {};
      sess.selectedGroup = groupName; sess.mode = "normal"; botSessions.set(uid, sess);
      const modal = new ModalBuilder().setCustomId("modal_broadcast_message").setTitle("📝 Broadcast Message");
      modal.addComponents(new ActionRowBuilder().addComponents(
        new TextInputBuilder().setCustomId("broadcast_content").setLabel("Message to broadcast")
          .setStyle(TextInputStyle.Paragraph).setPlaceholder("اكتب رسالتك هنا…").setRequired(true)
      ));
      return interaction.showModal(modal);
    }
    if (interaction.customId === "obc_select_group") {
      const groupName = interaction.values[0]; const sess = botSessions.get(uid) || {};
      sess.selectedGroup = groupName; botSessions.set(uid, sess);
      return interaction.update(buildSpeedScreen(groupName, sess.broadcastMessage, "obc", uid));
    }
    if (interaction.customId === "bc_select_group") {
      const groupName = interaction.values[0]; const sess = botSessions.get(uid) || {};
      sess.selectedGroup = groupName; botSessions.set(uid, sess);
      return interaction.update(buildSpeedScreen(groupName, sess.broadcastMessage, "bc", uid));
    }
    if (interaction.customId === "rbc_select_group") {
      const groupName = interaction.values[0]; const sess = botSessions.get(uid) || {};
      sess.selectedGroup = groupName; botSessions.set(uid, sess);
      return interaction.update(buildSpeedScreen(groupName, sess.broadcastMessage||"...", "rbc", uid));
    }
    if (interaction.customId === "rbc_select_role") {
      const roleId  = interaction.values[0];
      const sess    = botSessions.get(uid) || {};
      const guild   = interaction.guild;
      const role    = await guild.roles.fetch(roleId);
      sess.roleId   = roleId;
      sess.roleName = role?.name || roleId;
      botSessions.set(uid, sess);
      // Show group select
      const data = loadData();
      if (!data.groups.length) return interaction.reply({content:"❌ No groups yet.",ephemeral:true});
      return interaction.update(buildSelectGroupScreen(data.groups,"rbc",uid));
    }
  }

  // ── MODALS ───────────────────────────────────────────────────────────────────
  if (interaction.isModalSubmit()) {
    if (interaction.customId === "modal_add_tokens") {
      const raw      = interaction.fields.getTextInputValue("tokens_input");
      const incoming = raw.split("\n").map(t => t.trim()).filter(t => t.length > 20);
      const data     = loadData(); const before = data.tokens.length;
      data.tokens    = [...new Set([...data.tokens, ...incoming])]; saveData(data);
      const added    = data.tokens.length - before;
      return interaction.reply({ ...cvNotice(COLORS.green,"✅ Tokens Updated",[["➕ Added",`\`${added}\``],["📦 Total",`\`${data.tokens.length}\``],["⚠️ Duplicates Skipped",`\`${incoming.length-added}\``]]), ephemeral:true });
    }
    if (interaction.customId === "modal_create_group") {
      const name       = interaction.fields.getTextInputValue("group_name").trim();
      const tokenCount = parseInt(interaction.fields.getTextInputValue("group_token_count"), 10);
      const data       = loadData();
      if (isNaN(tokenCount) || tokenCount < 1) return interaction.reply({ content:"❌ عدد التوكنات يجب أن يكون رقم صحيح موجب.", ephemeral:true });
      if (data.tokens.length < tokenCount) return interaction.reply({ content:`❌ عندك **${data.tokens.length}** توكن فقط.`, ephemeral:true });
      if (data.groups.find(g => g.name === name)) return interaction.reply({ content:`❌ مجموعة بالاسم **${name}** موجودة مسبقاً.`, ephemeral:true });
      const groupId = generateGroupId(data.groups); data.groups.push({id:groupId,name,tokenCount}); saveData(data);
      return interaction.reply({ ...cvNotice(COLORS.green,"✅ Group Created",[["📦 Name",`**${name}**`],["🆔 ID",`\`${groupId}\``],["🤖 Tokens",`\`${tokenCount}\``]]), ephemeral:true });
    }
    if (interaction.customId === "modal_broadcast_message") {
      const msgContent = interaction.fields.getTextInputValue("broadcast_content");
      const sess = botSessions.get(uid) || {}; sess.broadcastMessage = msgContent; botSessions.set(uid, sess);
      return interaction.reply(buildSpeedScreen(sess.selectedGroup, msgContent, "normal", uid));
    }
  }
});



// ═══════════════════════════════════════════════════════════════════════════════
//  LOGS SYSTEM
// ═══════════════════════════════════════════════════════════════════════════════
const LOGS_FILE = path.join(__dirname, "logs.json");

function loadLogs() {
  if (!fs.existsSync(LOGS_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(LOGS_FILE, "utf8")); } catch { return []; }
}
function appendLog(entry) {
  const logs = loadLogs();
  logs.unshift({ ...entry, id: randomBytes(4).toString("hex"), timestamp: new Date().toISOString() });
  if (logs.length > 300) logs.splice(300);
  fs.writeFileSync(LOGS_FILE, JSON.stringify(logs, null, 2));
}

// ═══════════════════════════════════════════════════════════════════════════════
//  AUDIT LOG
// ═══════════════════════════════════════════════════════════════════════════════
const AUDIT_FILE = path.join(__dirname, "audit.json");
function loadAudit() {
  if (!fs.existsSync(AUDIT_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(AUDIT_FILE, "utf8")); } catch { return []; }
}
function appendAudit(action, userId, username, details={}) {
  const logs = loadAudit();
  logs.unshift({ id:randomBytes(4).toString("hex"), action, userId, username, details, timestamp:new Date().toISOString() });
  if (logs.length > 500) logs.splice(500);
  fs.writeFileSync(AUDIT_FILE, JSON.stringify(logs, null, 2));
}


// ═══════════════════════════════════════════════════════════════════════════════
//  SCHEDULED BROADCASTS
// ═══════════════════════════════════════════════════════════════════════════════
const SCHEDULES_FILE = path.join(__dirname, "schedules.json");
const BACKUP_DIR     = path.join(__dirname, "backups");
const RECURRING_FILE = path.join(__dirname, "recurring.json");

function loadRecurring() {
  if (!fs.existsSync(RECURRING_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(RECURRING_FILE, "utf8")); } catch { return []; }
}
function saveRecurring(arr) { fs.writeFileSync(RECURRING_FILE, JSON.stringify(arr, null, 2)); }

function doBackup() {
  if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive:true });
  const stamp = new Date().toISOString().replace(/[:.]/g,"-").slice(0,19);
  const files = ["data.json","blacklist.json","templates.json","schedules.json","logs.json"];
  const backup = {};
  files.forEach(f => { try { backup[f] = JSON.parse(fs.readFileSync(path.join(__dirname,f),"utf8")); } catch {} });
  fs.writeFileSync(path.join(BACKUP_DIR, `backup-${stamp}.json`), JSON.stringify(backup,null,2));
  // Keep only last 10 backups
  const existing = fs.readdirSync(BACKUP_DIR).filter(f=>f.endsWith(".json")).sort();
  while (existing.length > 10) { fs.unlinkSync(path.join(BACKUP_DIR, existing.shift())); }
  console.log("💾 Auto-backup saved:", stamp);
}

// Auto backup every 24h
setInterval(doBackup, 24*60*60*1000);
const scheduledJobs  = new Map();

function loadSchedules() {
  if (!fs.existsSync(SCHEDULES_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(SCHEDULES_FILE, "utf8")); } catch { return []; }
}
function saveSchedules(arr) { fs.writeFileSync(SCHEDULES_FILE, JSON.stringify(arr, null, 2)); }

function scheduleAllOnStartup() {
  for (const s of loadSchedules()) {
    if (s.status === "pending") scheduleJob(s);
  }
}

function scheduleJob(s) {
  const delay = new Date(s.scheduledAt).getTime() - Date.now();
  if (delay <= 0) { markSchedule(s.id, "missed"); return; }
  const tid = setTimeout(() => runScheduledBroadcast(s), delay);
  scheduledJobs.set(s.id, tid);
  console.log(`[SCHED] Job "${s.id}" scheduled in ${Math.round(delay/1000)}s`);
}

function cancelSchedule(id) {
  const tid = scheduledJobs.get(id);
  if (tid) { clearTimeout(tid); scheduledJobs.delete(id); }
}

function markSchedule(id, status) {
  const arr = loadSchedules();
  const s   = arr.find(x => x.id === id);
  if (s) { s.status = status; s.finishedAt = new Date().toISOString(); }
  saveSchedules(arr);
}

async function runScheduledBroadcast(s) {
  console.log(`[SCHED] Firing broadcast ${s.id} group=${s.groupName}`);
  const data  = loadData();
  const group = data.groups.find(g => g.name === s.groupName);
  if (!group) { markSchedule(s.id, "failed_no_group"); appendLog({ type:"scheduled", scheduleId:s.id, groupName:s.groupName, guildId:s.guildId, mode:s.mode, status:"failed_no_group", sent:0, failed:0, total:0 }); return; }

  const tokens = data.tokens.slice(0, group.tokenCount);
  if (!tokens.length) { markSchedule(s.id, "failed_no_tokens"); return; }

  let memberList;
  try {
    const guild = await discordClient.guilds.fetch(s.guildId);
    const col   = await guild.members.fetch();
    memberList  = [...col.values()].filter(m => !m.user.bot);
    if (s.mode === "obc") memberList = memberList.filter(m => m.presence && ["online","idle","dnd"].includes(m.presence.status));
    if (s.roleId)         memberList = memberList.filter(m => m.roles.cache.has(s.roleId));
    // Blacklist
    const bl = new Set(loadBlacklist().map(x => x.userId));
    if (bl.size) memberList = memberList.filter(m => !bl.has(m.user.id));
  } catch (e) {
    markSchedule(s.id, "failed_fetch");
    appendLog({ type:"scheduled", scheduleId:s.id, groupName:s.groupName, guildId:s.guildId, mode:s.mode, status:"error", error:e.message, sent:0, failed:0, total:0 });
    return;
  }

  const perBot = Math.ceil(memberList.length / tokens.length);
  let totalSent = 0, totalFailed = 0;

  await Promise.all(tokens.map(async (token, bi) => {
    const slice = memberList.slice(bi*perBot, (bi+1)*perBot);
    const rest  = new REST({ version:"10" }).setToken(token);
    try { await rest.get(Routes.user()); } catch { totalFailed += slice.length; return; }
    for (const member of slice) {
      await sleep(s.delay || 2000);
      try {
        const dm = await rest.post(Routes.userChannels(), { body:{ recipient_id: member.user.id } });
        await rest.post(Routes.channelMessages(dm.id), { body:{ content: s.message + `\n<@${member.user.id}>` } });
        totalSent++;
      } catch { totalFailed++; }
    }
  }));

  markSchedule(s.id, "done");
  appendLog({ type:"scheduled", scheduleId:s.id, groupName:s.groupName, guildId:s.guildId, mode:s.mode, roleId:s.roleId||null, sent:totalSent, failed:totalFailed, total:memberList.length, status:"done", message:s.message.slice(0,80) });
  console.log(`[SCHED] Done ${s.id} sent=${totalSent} failed=${totalFailed}`);
}



// ─── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json());
const FileStore = require("session-file-store")(session);
app.use(session({
  store: new FileStore({ path: "./sessions", ttl: 604800, reapInterval: 3600 }),
  secret: SESSION_SECRET || "broadcast-bot-secret-2024",
  name: "bcbot.sid",
  resave: false,
  saveUninitialized: false,
  rolling: true,
  cookie: {
    maxAge: 7 * 24 * 60 * 60 * 1000,
    httpOnly: true,
    sameSite: "lax",
    secure: false,
    path: "/",
  },
}));

// ─── Auth guard middleware ─────────────────────────────────────────────────────
function requireAuth(req, res, next) {
  if (!req.session.user) return res.status(401).json({ error: "Not authenticated" });
  const role = getUserRole(req.session.user.id);
  if (!role) return res.status(403).json({ error: "No permission — ask an owner to add you via $add-owner" });
  req.userRole = role;
  next();
}

// ─── Discord OAuth2 ────────────────────────────────────────────────────────────
app.get("/auth/login", (req, res) => {
  const redirectUri = `http://localhost:${PORT}/dashboard`;
  const params = new URLSearchParams({
    client_id    : CLIENT_ID,
    redirect_uri : redirectUri,
    response_type: "code",
    scope        : "identify",
  });
  res.redirect(`https://discord.com/oauth2/authorize?${params}`);
});

// Handle /dashboard as OAuth callback (Discord Portal redirect)
app.get("/dashboard", async (req, res) => {
  const { code, error } = req.query;

  // No code = not a callback, show dashboard normally
  if (!code && !error) return res.redirect("/");

  if (error) return res.redirect("/?error=" + encodeURIComponent(error));

  const redirectUri = `http://localhost:${PORT}/dashboard`;
  console.log("🔑 /dashboard callback received");
  console.log("   code:", code?.slice(0,10)+"...");
  console.log("   redirect_uri being sent:", redirectUri);
  console.log("   CLIENT_ID:", CLIENT_ID);
  console.log("   CLIENT_SECRET exists:", !!CLIENT_SECRET);

  try {
    const tokenRes = await axios.post("https://discord.com/api/oauth2/token",
      new URLSearchParams({
        client_id    : CLIENT_ID,
        client_secret: CLIENT_SECRET,
        grant_type   : "authorization_code",
        code,
        redirect_uri : redirectUri,
      }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );
    console.log("✅ Token received");
    const userRes = await axios.get("https://discord.com/api/users/@me", {
      headers: { Authorization: `Bearer ${tokenRes.data.access_token}` },
    });
    console.log(`✅ User: ${userRes.data.username} (${userRes.data.id})`);
    req.session.user = {
      id         : userRes.data.id,
      username   : userRes.data.username,
      avatar     : userRes.data.avatar
        ? `https://cdn.discordapp.com/avatars/${userRes.data.id}/${userRes.data.avatar}.png`
        : `https://cdn.discordapp.com/embed/avatars/0.png`,
      accessToken: tokenRes.data.access_token,
    };
    req.session.save((err) => {
      if (err) {
        console.error("❌ Session save error:", err);
        return res.redirect("/?error=session_failed");
      }
      console.log("✅ Session saved!");
      console.log("   session ID:", req.sessionID);
      res.redirect("/");
    });
  } catch (e) {
    const errData = e.response?.data;
    console.error("❌ OAuth FAILED:", JSON.stringify(errData) || e.message);
    res.redirect("/?error=oauth_failed");
  }
});

async function handleOAuthCallback(req, res) {
  const { code, error } = req.query;
  if (error) return res.redirect("/?error=" + encodeURIComponent(error));
  if (!code)  return res.redirect("/?error=no_code");
  const redirectUri = getRedirectUri(req);
  try {
    const tokenRes = await axios.post("https://discord.com/api/oauth2/token",
      new URLSearchParams({
        client_id    : CLIENT_ID,
        client_secret: CLIENT_SECRET,
        grant_type   : "authorization_code",
        code,
        redirect_uri : redirectUri,
      }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );
    const userRes = await axios.get("https://discord.com/api/users/@me", {
      headers: { Authorization: `Bearer ${tokenRes.data.access_token}` },
    });
    req.session.user = {
      id         : userRes.data.id,
      username   : userRes.data.username,
      avatar     : userRes.data.avatar
        ? `https://cdn.discordapp.com/avatars/${userRes.data.id}/${userRes.data.avatar}.png`
        : `https://cdn.discordapp.com/embed/avatars/0.png`,
      accessToken: tokenRes.data.access_token,
    };
    console.log(`✅ Login: ${userRes.data.username} (${userRes.data.id})`);
    req.session.save((err) => {
      if (err) { console.error("Session save error:", err); return res.redirect("/?error=session_failed"); }
      res.redirect("/");
    });
  } catch (e) {
    console.error("OAuth error:", e.response?.data || e.message);
    res.redirect("/?error=oauth_failed");
  }
}

app.get("/auth/callback", async (req, res) => {
  const { code, error } = req.query;
  if (error) {
    console.error("OAuth declined:", error);
    return res.redirect("/?error=" + encodeURIComponent(error));
  }
  if (!code) return res.redirect("/?error=no_code");
  try {
    const tokenRes = await axios.post("https://discord.com/api/oauth2/token",
      new URLSearchParams({
        client_id    : CLIENT_ID,
        client_secret: CLIENT_SECRET,
        grant_type   : "authorization_code",
        code,
        redirect_uri : REDIRECT_URI,
      }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );
    const userRes = await axios.get("https://discord.com/api/users/@me", {
      headers: { Authorization: `Bearer ${tokenRes.data.access_token}` },
    });
    req.session.user = {
      id          : userRes.data.id,
      username    : userRes.data.username,
      avatar      : userRes.data.avatar
        ? `https://cdn.discordapp.com/avatars/${userRes.data.id}/${userRes.data.avatar}.png`
        : `https://cdn.discordapp.com/embed/avatars/0.png`,
      accessToken : tokenRes.data.access_token,
    };
    console.log(`✅ Login: ${userRes.data.username} (${userRes.data.id})`);
    req.session.save((err) => {
      if (err) {
        console.error("Session save error:", err);
        return res.redirect("/?error=session_failed");
      }
      res.redirect("/");
    });
  } catch (e) {
    console.error("OAuth error:", e.response?.data || e.message);
    res.redirect("/?error=oauth_failed");
  }
});

app.get("/auth/logout", (req, res) => {
  req.session.destroy();
  res.redirect("/");
});

app.get("/auth/me", (req, res) => {
  console.log("📍 /auth/me called");
  console.log("   sessionID:", req.sessionID);
  console.log("   session.user:", req.session?.user?.username || "NONE");
  console.log("   cookies header:", req.headers.cookie || "NO COOKIES");
  console.log("   full session:", JSON.stringify(req.session));
  console.log("   Set-Cookie will be:", res.getHeader('Set-Cookie'));
  if (!req.session.user) return res.json({ user: null });
  const data  = loadData();
  const uid   = req.session.user.id;
  const role  = data.botOwnerId === uid ? "Bot Owner"
              : data.owners.includes(uid) ? "Owner"
              : "No Access";
  res.json({ user: req.session.user, role, hasAccess: role !== "No Access" });
});

// ─── API: Overview ─────────────────────────────────────────────────────────────
app.get("/api/overview", requireAuth, (req, res) => {
  const data = loadData();
  res.json({
    tokenCount : data.tokens.length,
    groupCount : data.groups.length,
    ownerCount : data.owners.length,
    botOwnerId : data.botOwnerId,
  });
});

// ─── API: Tokens ───────────────────────────────────────────────────────────────
app.get("/api/tokens", requireAuth, async (req, res) => {
  const data = loadData();
  const rest = new REST({ version: "10" });
  const result = [];
  for (let i = 0; i < data.tokens.length; i++) {
    const token = data.tokens[i];
    try {
      rest.setToken(token);
      const app2 = await rest.get(Routes.currentApplication());
      result.push({
        index   : i,
        snippet : token.slice(0, 24) + "…",
        name    : app2.username,
        avatar  : app2.icon
          ? `https://cdn.discordapp.com/app-icons/${app2.id}/${app2.icon}.png`
          : null,
        appId   : app2.id,
        valid   : true,
        groups  : data.groups.filter(g => data.tokens.slice(0, g.tokenCount).includes(token)).map(g => g.name),
      });
    } catch {
      result.push({ index: i, snippet: token.slice(0, 24) + "…", valid: false, groups: [] });
    }
  }
  res.json(result);
});

app.post("/api/tokens", requireAuth, (req, res) => {
  const { tokens } = req.body;
  if (!Array.isArray(tokens)) return res.status(400).json({ error: "tokens must be array" });
  const data   = loadData();
  const before = data.tokens.length;
  data.tokens  = [...new Set([...data.tokens, ...tokens.filter(t => t.length > 20)])];
  saveData(data);
  appendAudit("add_tokens", req.session.user.id, req.session.user.username, { added: data.tokens.length - before });
  res.json({ added: data.tokens.length - before, total: data.tokens.length });
});

app.delete("/api/tokens/:index", requireAuth, (req, res) => {
  const idx  = parseInt(req.params.index);
  const data = loadData();
  if (isNaN(idx) || idx < 0 || idx >= data.tokens.length)
    return res.status(404).json({ error: "Token not found" });
  const removed = data.tokens.splice(idx, 1)[0];
  saveData(data);
  appendAudit("delete_token", req.session.user.id, req.session.user.username, { token: removed.slice(0,16)+"…" });
  res.json({ removed: removed.slice(0, 24) + "…", total: data.tokens.length });
});

// ─── API: Groups ───────────────────────────────────────────────────────────────
app.get("/api/groups", requireAuth, (req, res) => {
  const data = loadData();
  // migrate old groups
  let dirty = false;
  for (const g of data.groups) {
    if (!g.id) { g.id = generateGroupId(data.groups); dirty = true; }
  }
  if (dirty) saveData(data);
  res.json(data.groups);
});

app.post("/api/groups", requireAuth, (req, res) => {
  const { name, tokenCount } = req.body;
  const data = loadData();
  if (!name || !tokenCount || tokenCount < 1)
    return res.status(400).json({ error: "name and tokenCount required" });
  if (data.tokens.length < tokenCount)
    return res.status(400).json({ error: `Only ${data.tokens.length} tokens available` });
  if (data.groups.find(g => g.name === name))
    return res.status(400).json({ error: "Group name already exists" });
  const id = generateGroupId(data.groups);
  data.groups.push({ id, name, tokenCount });
  saveData(data);
  appendAudit("create_group", req.session.user.id, req.session.user.username, { name, tokenCount });
  res.json({ id, name, tokenCount });
});

app.delete("/api/groups/:id", requireAuth, (req, res) => {
  const data = loadData();
  const idx  = data.groups.findIndex(g => g.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: "Group not found" });
  const removed = data.groups.splice(idx, 1)[0];
  saveData(data);
  appendAudit("delete_group", req.session.user.id, req.session.user.username, { name: removed.name });
  res.json({ removed });
});

// ─── API: Owners ───────────────────────────────────────────────────────────────
app.get("/api/owners", requireAuth, async (req, res) => {
  const data = loadData();
  const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN);
  const resolve = async (id) => {
    try {
      const u = await rest.get(Routes.user(id));
      return { id, username: u.username,
        avatar: u.avatar
          ? `https://cdn.discordapp.com/avatars/${id}/${u.avatar}.png`
          : `https://cdn.discordapp.com/embed/avatars/0.png` };
    } catch { return { id, username: `Unknown (${id})`, avatar: null }; }
  };
  const botOwner = data.botOwnerId ? await resolve(data.botOwnerId) : null;
  const owners   = await Promise.all(data.owners.map(resolve));
  res.json({ botOwner, owners });
});

app.post("/api/owners", requireAuth, (req, res) => {
  const { userId } = req.body;
  if (!userId || !/^\d{17,20}$/.test(userId))
    return res.status(400).json({ error: "Invalid Discord user ID" });
  const data = loadData();
  const uid  = req.session.user.id;
  if (data.botOwnerId !== uid)
    return res.status(403).json({ error: "Only the bot owner can add owners" });
  if (!data.botOwnerId) data.botOwnerId = uid;
  if (data.owners.includes(userId))
    return res.status(400).json({ error: "Already an owner" });
  data.owners.push(userId);
  saveData(data);
  appendAudit("add_owner", req.session.user.id, req.session.user.username, { targetId: userId });
  res.json({ ok: true });
});

app.delete("/api/owners/:userId", requireAuth, (req, res) => {
  const data = loadData();
  const uid  = req.session.user.id;
  if (data.botOwnerId !== uid)
    return res.status(403).json({ error: "Only the bot owner can remove owners" });
  const idx = data.owners.indexOf(req.params.userId);
  if (idx === -1) return res.status(404).json({ error: "Owner not found" });
  data.owners.splice(idx, 1);
  saveData(data);
  res.json({ ok: true });
});

// ─── API: Bot info (avatar/name for all tokens) ────────────────────────────────
app.get("/api/bots/info", requireAuth, async (req, res) => {
  const data = loadData();
  const rest = new REST({ version: "10" });
  const bots = [];
  for (const token of data.tokens) {
    try {
      rest.setToken(token);
      const app2 = await rest.get(Routes.currentApplication());
      bots.push({ name: app2.username, id: app2.id, valid: true });
    } catch { bots.push({ valid: false }); }
  }
  res.json(bots);
});

app.post("/api/bots/avatar", requireAuth, async (req, res) => {
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: "url required" });
  let imageData;
  try {
    imageData = await fetchBase64(url);
  } catch { return res.status(400).json({ error: "Failed to fetch image" }); }
  const data = loadData();
  const rest = new REST({ version: "10" });
  const results = [];
  for (const token of data.tokens) {
    try {
      rest.setToken(token);
      const app2 = await rest.get(Routes.currentApplication());
      await rest.patch(Routes.user(), { body: { avatar: imageData } });
      results.push({ name: app2.username, ok: true });
    } catch (e) { results.push({ name: "?", ok: false, err: e.message }); }
    await sleep(1500);
  }
  res.json(results);
});

app.post("/api/bots/name", requireAuth, async (req, res) => {
  const { name } = req.body;
  if (!name || name.length < 2 || name.length > 32)
    return res.status(400).json({ error: "Name must be 2–32 chars" });
  const data = loadData();
  const rest = new REST({ version: "10" });
  const results = [];
  for (const token of data.tokens) {
    try {
      rest.setToken(token);
      const before = await rest.get(Routes.currentApplication());
      await rest.patch(Routes.user(), { body: { username: name } });
      results.push({ oldName: before.username, ok: true });
    } catch (e) { results.push({ ok: false, err: e.message }); }
    await sleep(2000);
  }
  res.json(results);
});


// ─── API: Logs ─────────────────────────────────────────────────────────────────
app.get("/api/logs", requireAuth, (req, res) => res.json(loadLogs()));
app.delete("/api/logs", requireAuth, (req, res) => { fs.writeFileSync(LOGS_FILE, "[]"); res.json({ ok:true }); });

// ─── API: Schedules ────────────────────────────────────────────────────────────
app.get("/api/schedules", requireAuth, (req, res) => res.json(loadSchedules()));

app.post("/api/schedules", requireAuth, (req, res) => {
  const { groupName, guildId, message, scheduledAt, delay, mode, roleId } = req.body;
  if (!groupName||!guildId||!message||!scheduledAt)
    return res.status(400).json({ error:"groupName, guildId, message, scheduledAt required" });
  const fireAt = new Date(scheduledAt).getTime();
  if (isNaN(fireAt)||fireAt<=Date.now())
    return res.status(400).json({ error:"scheduledAt must be a future datetime" });
  const s = {
    id:randomBytes(4).toString("hex"), groupName, guildId, message,
    scheduledAt, delay:delay||2000, mode:mode||"bc", roleId:roleId||null,
    status:"pending", createdAt:new Date().toISOString(), createdBy:req.session.user.id,
  };
  const arr = loadSchedules(); arr.push(s); saveSchedules(arr);
  scheduleJob(s);
  res.json(s);
});

app.delete("/api/schedules/:id", requireAuth, (req, res) => {
  const arr = loadSchedules();
  const idx = arr.findIndex(s => s.id === req.params.id);
  if (idx===-1) return res.status(404).json({ error:"Not found" });
  cancelSchedule(req.params.id);
  arr.splice(idx,1); saveSchedules(arr);
  res.json({ ok:true });
});

// ─── API: Guild Roles ──────────────────────────────────────────────────────────
app.get("/api/guild/:guildId/roles", requireAuth, async (req, res) => {
  try {
    const guild = await discordClient.guilds.fetch(req.params.guildId);
    const roles = await guild.roles.fetch();
    const list  = [...roles.values()]
      .filter(r => r.name !== "@everyone")
      .sort((a,b) => b.position - a.position)
      .map(r => ({ id:r.id, name:r.name, color:r.hexColor, memberCount:r.members.size }));
    res.json(list);
  } catch(e) { res.status(400).json({ error:e.message }); }
});



// ─── API: Guilds (bot servers list) ───────────────────────────────────────────
app.get("/api/guilds", requireAuth, async (req, res) => {
  try {
    const guilds = [...discordClient.guilds.cache.values()].map(g => ({
      id:   g.id,
      name: g.name,
      icon: g.icon ? `https://cdn.discordapp.com/icons/${g.id}/${g.icon}.png` : null,
      memberCount: g.memberCount,
    }));
    guilds.sort((a,b) => b.memberCount - a.memberCount);
    res.json(guilds);
  } catch(e) { res.status(400).json({ error: e.message }); }
});

// ─── API: Blacklist ───────────────────────────────────────────────────────────
const BL_FILE = path.join(__dirname, "blacklist.json");
function loadBlacklist() {
  if (!fs.existsSync(BL_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(BL_FILE,"utf8")); } catch { return []; }
}
function saveBlacklist(arr) { fs.writeFileSync(BL_FILE, JSON.stringify(arr,null,2)); }

app.get("/api/blacklist", requireAuth, (req, res) => res.json(loadBlacklist()));
app.post("/api/blacklist", requireAuth, (req, res) => {
  const { userId, note } = req.body;
  if (!userId || !/^\d{17,20}$/.test(userId))
    return res.status(400).json({ error: "Invalid user ID" });
  const bl = loadBlacklist();
  if (bl.find(x => x.userId === userId))
    return res.status(400).json({ error: "Already blacklisted" });
  bl.push({ userId, note: note||"", addedAt: new Date().toISOString(), addedBy: req.session.user.id });
  saveBlacklist(bl);
  appendAudit("blacklist_add", req.session.user.id, req.session.user.username, { userId, note });
  res.json({ ok: true });
});
app.delete("/api/blacklist/:userId", requireAuth, (req, res) => {
  const bl  = loadBlacklist();
  const idx = bl.findIndex(x => x.userId === req.params.userId);
  if (idx === -1) return res.status(404).json({ error: "Not found" });
  bl.splice(idx, 1);
  saveBlacklist(bl);
  res.json({ ok: true });
});

// ─── API: Message Templates ───────────────────────────────────────────────────
const TPL_FILE = path.join(__dirname, "templates.json");
function loadTemplates() {
  if (!fs.existsSync(TPL_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(TPL_FILE,"utf8")); } catch { return []; }
}
function saveTemplates(arr) { fs.writeFileSync(TPL_FILE, JSON.stringify(arr,null,2)); }

app.get("/api/templates", requireAuth, (req, res) => res.json(loadTemplates()));
app.post("/api/templates", requireAuth, (req, res) => {
  const { name, content } = req.body;
  if (!name || !content) return res.status(400).json({ error: "name and content required" });
  const arr = loadTemplates();
  if (arr.find(t => t.name === name)) return res.status(400).json({ error: "Name already exists" });
  const tpl = { id: randomBytes(4).toString("hex"), name, content, createdAt: new Date().toISOString() };
  arr.push(tpl);
  saveTemplates(arr);
  res.json(tpl);
});
app.delete("/api/templates/:id", requireAuth, (req, res) => {
  const arr = loadTemplates();
  const idx = arr.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: "Not found" });
  arr.splice(idx, 1);
  saveTemplates(arr);
  res.json({ ok: true });
});

// ─── API: Stats (historical charts) ──────────────────────────────────────────
app.get("/api/stats/daily", requireAuth, (req, res) => {
  const logs  = loadLogs();
  const byDay = {};
  for (const l of logs) {
    const day = l.timestamp.slice(0,10);
    if (!byDay[day]) byDay[day] = { sent:0, failed:0, count:0 };
    byDay[day].sent   += l.sent   || 0;
    byDay[day].failed += l.failed || 0;
    byDay[day].count++;
  }
  const days = [];
  for (let i=13; i>=0; i--) {
    const d = new Date(Date.now() - i*86400000).toISOString().slice(0,10);
    days.push({ date:d, ...(byDay[d]||{sent:0,failed:0,count:0}) });
  }
  res.json(days);
});

// ─── API: Token health check ──────────────────────────────────────────────────
app.post("/api/tokens/check", requireAuth, async (req, res) => {
  const data = loadData();
  const rest = new REST({ version:"10" });
  const results = [];
  for (let i=0; i<data.tokens.length; i++) {
    const token = data.tokens[i];
    try {
      rest.setToken(token);
      const u = await rest.get(Routes.user());
      results.push({ index:i, valid:true, name:u.username });
    } catch { results.push({ index:i, valid:false }); }
    await sleep(300);
  }
  // Remove invalid tokens from data
  if (req.query.cleanup === "true") {
    const validIndexes = new Set(results.filter(r=>r.valid).map(r=>r.index));
    data.tokens = data.tokens.filter((_,i) => validIndexes.has(i));
    saveData(data);
  }
  res.json(results);
});

// ─── API: Export logs ─────────────────────────────────────────────────────────
app.get("/api/logs/export", requireAuth, (req, res) => {
  const fmt  = req.query.format || "json";
  const logs = loadLogs();
  if (fmt === "csv") {
    const hdr  = "id,timestamp,type,groupName,guildId,mode,sent,failed,total,status,message\n";
    const rows = logs.map(l =>
      [l.id,l.timestamp,l.type||"",l.groupName||"",l.guildId||"",l.mode||"",
       l.sent||0,l.failed||0,l.total||0,l.status||"",(l.message||"").replace(/,/g,";")].join(",")
    ).join("\n");
    res.setHeader("Content-Type","text/csv");
    res.setHeader("Content-Disposition","attachment; filename=broadcast-logs.csv");
    return res.send(hdr + rows);
  }
  res.setHeader("Content-Type","application/json");
  res.setHeader("Content-Disposition","attachment; filename=broadcast-logs.json");
  res.send(JSON.stringify(logs,null,2));
});


// ─── API: Audit Log ───────────────────────────────────────────────────────────
app.get("/api/audit", requireAuth, (req, res) => res.json(loadAudit()));
app.delete("/api/audit", requireAuth, (req, res) => { fs.writeFileSync(AUDIT_FILE, "[]"); res.json({ ok:true }); });

// ─── Bot Labels API ───────────────────────────────────────────────────────────
app.get("/api/tokens/health", requireAuth, (req, res) => {
  res.json({ results: global._tokenHealth||[], checkedAt: global._tokenHealthCheckedAt||null });
});
app.post("/api/tokens/health/refresh", requireRole("mod"), async (req, res) => {
  const d = loadData();
  const results = [];
  for (let i=0; i<d.tokens.length; i++) {
    try {
      const rest = new REST({version:"10"}).setToken(d.tokens[i]);
      const user = await rest.get(Routes.user());
      results.push({ index:i, valid:true, username:user.username, id:user.id });
    } catch(e) { results.push({ index:i, valid:false, error:e.message }); }
  }
  global._tokenHealth = results;
  global._tokenHealthCheckedAt = new Date().toISOString();
  res.json({ results, checkedAt: global._tokenHealthCheckedAt });
});

app.get("/api/bot-labels", requireAuth, (req, res) => {
  const d = loadData(); res.json(d.botLabels||{});
});
app.post("/api/bot-labels", requireRole("admin"), (req, res) => {
  const { index, label } = req.body;
  const d = loadData();
  if (label) d.botLabels[index] = label;
  else delete d.botLabels[index];
  saveData(d); res.json({ ok:true });
});

// ─── Roles API ────────────────────────────────────────────────────────────────
app.get("/api/roles", requireAuth, (req, res) => {
  const d = loadData();
  res.json({ roles: d.roles, botOwnerId: d.botOwnerId, myRole: req.userRole });
});
app.post("/api/roles", requireRole("admin"), (req, res) => {
  const { userId, role } = req.body;
  const valid = ["admin","mod","viewer"];
  if (!userId || !valid.includes(role)) return res.status(400).json({ error:"Invalid role or userId" });
  const d = loadData(); d.roles[userId] = role; saveData(d);
  appendAudit("set_role", req.session.user.id, req.session.user.username, { targetId:userId, role });
  res.json({ ok:true });
});
app.delete("/api/roles/:userId", requireRole("admin"), (req, res) => {
  const d = loadData(); delete d.roles[req.params.userId]; saveData(d);
  res.json({ ok:true });
});
app.get("/api/me", requireAuth, (req, res) => {
  res.json({ user: req.session.user, role: req.userRole });
});

// ─── 2FA API ──────────────────────────────────────────────────────────────────
app.post("/api/2fa/request", async (req, res) => {
  if (!req.session.pendingUser) return res.status(400).json({ error:"No pending login" });
  const uid = req.session.pendingUser.id;
  const code = Math.floor(100000 + Math.random() * 900000).toString();
  const expires = Date.now() + 5 * 60 * 1000; // 5 min
  const d = loadData(); d.twofa[uid] = { code, expires }; saveData(d);
  // Send code via Discord DM using first token
  try {
    const tokens = d.tokens; if (!tokens.length) throw new Error("No tokens");
    const rest = new REST({version:"10"}).setToken(tokens[0]);
    const dm = await rest.post(Routes.userChannels(), { body:{ recipient_id:uid } });
    await rest.post(Routes.channelMessages(dm.id), { body:{ content:`🔐 **كود التحقق الخاص بك:** \`${code}\`
ينتهي خلال 5 دقائق.` } });
    res.json({ ok:true, hint:"Code sent via Discord DM" });
  } catch(e) { res.status(500).json({ error:"Failed to send DM: "+e.message }); }
});
app.post("/api/2fa/verify", (req, res) => {
  if (!req.session.pendingUser) return res.status(400).json({ error:"No pending login" });
  const uid = req.session.pendingUser.id;
  const d = loadData();
  const record = d.twofa[uid];
  if (!record || Date.now() > record.expires) return res.status(400).json({ error:"Code expired" });
  if (record.code !== req.body.code) return res.status(400).json({ error:"Wrong code" });
  delete d.twofa[uid]; saveData(d);
  req.session.user = req.session.pendingUser;
  delete req.session.pendingUser;
  req.session.save(() => res.json({ ok:true }));
});
app.get("/api/2fa/status", requireAuth, (req, res) => {
  const d = loadData();
  const has2fa = !!d.twofa2 && !!d.twofa2[req.session.user.id];
  res.json({ enabled: has2fa });
});

// ─── Recurring Broadcast API ──────────────────────────────────────────────────
app.get("/api/recurring", requireAuth, (req, res) => res.json(loadRecurring()));
app.post("/api/recurring", requireRole("mod"), (req, res) => {
  const { groupName, guildId, message, intervalHours, mode, delay } = req.body;
  if (!groupName||!guildId||!message||!intervalHours) return res.status(400).json({ error:"Missing fields" });
  const arr = loadRecurring();
  const rec = { id:randomBytes(4).toString("hex"), groupName, guildId, message, intervalHours:Number(intervalHours), mode:mode||"bc", delay:delay||2000, enabled:true, lastRun:null, nextRun:new Date(Date.now()+Number(intervalHours)*3600000).toISOString(), createdAt:new Date().toISOString(), createdBy:req.session.user.id };
  arr.push(rec);
  saveRecurring(arr);
  scheduleRecurring(rec);
  appendAudit("add_recurring", req.session.user.id, req.session.user.username, { groupName, intervalHours });
  res.json(rec);
});
app.delete("/api/recurring/:id", requireRole("mod"), (req, res) => {
  const arr = loadRecurring().filter(r=>r.id!==req.params.id);
  saveRecurring(arr);
  clearRecurring(req.params.id);
  res.json({ ok:true });
});
app.patch("/api/recurring/:id/toggle", requireRole("mod"), (req, res) => {
  const arr = loadRecurring();
  const r = arr.find(x=>x.id===req.params.id);
  if (!r) return res.status(404).json({ error:"Not found" });
  r.enabled = !r.enabled;
  saveRecurring(arr);
  res.json({ ok:true, enabled:r.enabled });
});

// ─── Backup API ───────────────────────────────────────────────────────────────
app.get("/api/backups", requireRole("admin"), (req, res) => {
  if (!fs.existsSync(BACKUP_DIR)) return res.json([]);
  const files = fs.readdirSync(BACKUP_DIR).filter(f=>f.endsWith(".json")).sort().reverse();
  res.json(files.map(f => ({ name:f, size:fs.statSync(path.join(BACKUP_DIR,f)).size, date:f.replace("backup-","").replace(".json","") })));
});
app.post("/api/backups/now", requireRole("admin"), (req, res) => {
  try { doBackup(); res.json({ ok:true }); } catch(e) { res.status(500).json({ error:e.message }); }
});
app.get("/api/backups/:name", requireRole("admin"), (req, res) => {
  const file = path.join(BACKUP_DIR, req.params.name);
  if (!fs.existsSync(file)) return res.status(404).json({ error:"Not found" });
  res.download(file);
});

// ─── WebSocket: Real-time broadcast ───────────────────────────────────────────
const wsClients = new Map();

// ─── Failed members store (for retry) ────────────────────────────────────────
const FAILED_FILE = path.join(__dirname, "failed.json");

// ─── Recurring job scheduler ──────────────────────────────────────────────────
const recurringTimers = new Map();
function scheduleRecurring(rec) {
  clearRecurring(rec.id);
  if (!rec.enabled) return;
  const ms = rec.intervalHours * 3600 * 1000;
  const delay = Math.max(0, new Date(rec.nextRun).getTime() - Date.now());
  const run = async () => {
    const arr = loadRecurring();
    const r = arr.find(x=>x.id===rec.id);
    if (!r || !r.enabled) return;
    const data = loadData();
    const group = data.groups.find(g=>g.name===r.groupName);
    if (!group) return;
    const tokens = data.tokens.slice(0, group.tokenCount);
    let memberList = [];
    try {
      const rest = new REST({version:"10"}).setToken(tokens[0]);
      const m = await rest.get(Routes.guildMembers(r.guildId), { query:{ limit:1000 } });
      memberList = m.filter(mem=>!mem.user?.bot);
    } catch { return; }
    const bl = loadBlacklist ? loadBlacklist().map(b=>b.userId) : [];
    memberList = memberList.filter(m=>!bl.includes(m.user.id));
    if (memberList.length) {
      const sk = "rec_"+r.id+"_"+Date.now();
      await broadcastToMembers(null, { tokens, memberList, message:r.message, delay:r.delay||2000, guildId:r.guildId, groupName:r.groupName, mode:r.mode||"bc", roleId:null, sessionKey:sk, imageUrl:"" });
    }
    r.lastRun = new Date().toISOString();
    r.nextRun = new Date(Date.now()+ms).toISOString();
    saveRecurring(arr);
    const timer = setTimeout(run, ms);
    recurringTimers.set(rec.id, timer);
  };
  const timer = setTimeout(run, delay);
  recurringTimers.set(rec.id, timer);
}
function clearRecurring(id) {
  if (recurringTimers.has(id)) { clearTimeout(recurringTimers.get(id)); recurringTimers.delete(id); }
}
// Start all recurring on boot
setTimeout(() => { loadRecurring().forEach(r => { if(r.enabled) scheduleRecurring(r); }); }, 3000);

// Auto token health check on boot
setTimeout(async () => {
  const d = loadData();
  if (!d.tokens.length) return;
  console.log(`🔍 Auto-checking ${d.tokens.length} tokens...`);
  const results = [];
  for (let i=0; i<d.tokens.length; i++) {
    try {
      const rest = new REST({version:"10"}).setToken(d.tokens[i]);
      const user = await rest.get(Routes.user());
      results.push({ index:i, valid:true, username:user.username });
      console.log(`  ✅ Token ${i+1}: ${user.username}`);
    } catch(e) {
      results.push({ index:i, valid:false, error:e.message });
      console.log(`  ❌ Token ${i+1}: INVALID — ${e.message}`);
    }
  }
  const invalid = results.filter(r=>!r.valid).length;
  if (invalid) console.log(`⚠️  ${invalid} invalid token(s) detected`);
  else console.log(`✅ All ${d.tokens.length} tokens are valid`);
  global._tokenHealth = results;
  global._tokenHealthCheckedAt = new Date().toISOString();
}, 5000);
function loadFailed() {
  if (!fs.existsSync(FAILED_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(FAILED_FILE,"utf8")); } catch { return {}; }
}
function saveFailed(obj) { fs.writeFileSync(FAILED_FILE, JSON.stringify(obj,null,2)); }

// ─── Group history store ──────────────────────────────────────────────────────
const GH_FILE = path.join(__dirname, "group_history.json");
function loadGroupHistory() {
  if (!fs.existsSync(GH_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(GH_FILE,"utf8")); } catch { return {}; }
}
function updateGroupHistory(groupName, sent, failed) {
  const gh = loadGroupHistory();
  if (!gh[groupName]) gh[groupName] = { totalSent:0, totalFailed:0, broadcastCount:0, lastAt:null };
  gh[groupName].totalSent    += sent;
  gh[groupName].totalFailed  += failed;
  gh[groupName].broadcastCount++;
  gh[groupName].lastAt = new Date().toISOString();
  fs.writeFileSync(GH_FILE, JSON.stringify(gh,null,2));
}

// ─── API: Group history ───────────────────────────────────────────────────────
app.get("/api/groups/history", requireAuth, (req, res) => res.json(loadGroupHistory()));

// ─── API: Preview broadcast ───────────────────────────────────────────────────
app.post("/api/broadcast/preview", requireAuth, async (req, res) => {
  const { groupName, guildIds, mode, roleId } = req.body;
  if (!groupName || !guildIds || !guildIds.length) return res.status(400).json({ error:"Missing fields" });
  const data  = loadData();
  const group = data.groups.find(g => g.name===groupName);
  if (!group) return res.status(404).json({ error:"Group not found" });
  const bl    = new Set(loadBlacklist().map(x => x.userId));

  const guilds = [];
  for (const guildId of guildIds) {
    try {
      const guild = await discordClient.guilds.fetch(guildId);
      const col   = await guild.members.fetch();
      let members = [...col.values()].filter(m => !m.user.bot);
      if (mode==="obc")  members = members.filter(m => m.presence&&["online","idle","dnd"].includes(m.presence.status));
      if (roleId)        members = members.filter(m => m.roles.cache.has(roleId));
      if (bl.size)       members = members.filter(m => !bl.has(m.user.id));
      guilds.push({ guildId, name:guild.name, memberCount:members.length });
    } catch(e) { guilds.push({ guildId, name:"?", memberCount:0, error:e.message }); }
  }
  const totalMembers = guilds.reduce((a,g) => a + g.memberCount, 0);
  const bots         = Math.min(group.tokenCount, data.tokens.length);
  const blacklisted  = bl.size;
  res.json({ groupName, bots, totalMembers, blacklisted, guilds, mode, roleId:roleId||null });
});

// ─── API: Retry failed members ────────────────────────────────────────────────
app.get("/api/failed", requireAuth, (req, res) => {
  const f = loadFailed();
  const keys = Object.keys(f);
  res.json({ count: keys.reduce((a,k)=>a+f[k].members.length,0), sessions: keys.map(k=>({id:k,...f[k]})) });
});
app.delete("/api/failed/:sessionId", requireAuth, (req, res) => {
  const f = loadFailed(); delete f[req.params.sessionId]; saveFailed(f);
  res.json({ ok:true });
});

// ─── API: Webhook settings ────────────────────────────────────────────────────
const WH_FILE = path.join(__dirname, "webhooks.json");
function loadWebhooks() {
  if (!fs.existsSync(WH_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(WH_FILE,"utf8")); } catch { return []; }
}
function saveWebhooks(arr) { fs.writeFileSync(WH_FILE, JSON.stringify(arr,null,2)); }

app.get("/api/webhooks",  requireAuth, (req, res) => res.json(loadWebhooks()));
app.post("/api/webhooks", requireAuth, (req, res) => {
  const { url, label, events } = req.body;
  if (!url || !url.startsWith("https://discord.com/api/webhooks/")) return res.status(400).json({ error:"Invalid Discord webhook URL" });
  const arr = loadWebhooks();
  const wh  = { id: randomBytes(4).toString("hex"), url, label:label||"Webhook", events:events||["done","error"], createdAt:new Date().toISOString() };
  arr.push(wh); saveWebhooks(arr);
  res.json(wh);
});
app.delete("/api/webhooks/:id", requireAuth, (req, res) => {
  const arr = loadWebhooks(); const idx = arr.findIndex(w=>w.id===req.params.id);
  if (idx===-1) return res.status(404).json({ error:"Not found" });
  arr.splice(idx,1); saveWebhooks(arr); res.json({ ok:true });
});
app.post("/api/webhooks/test", requireAuth, async (req, res) => {
  const { url } = req.body;
  try {
    await axios.post(url, { embeds:[{ title:"🔔 Test Webhook", description:"Connection OK!", color:0x23d18b, timestamp:new Date().toISOString() }] });
    res.json({ ok:true });
  } catch(e) { res.status(400).json({ error:e.message }); }
});

// ─── API: Bulk blacklist import ───────────────────────────────────────────────
app.post("/api/blacklist/bulk", requireAuth, (req, res) => {
  const { userIds, note } = req.body;
  if (!Array.isArray(userIds)) return res.status(400).json({ error:"userIds must be array" });
  const bl  = loadBlacklist();
  const now = new Date().toISOString();
  let added = 0, skipped = 0;
  for (const userId of userIds) {
    if (!/^\d{17,20}$/.test(userId)) { skipped++; continue; }
    if (bl.find(x=>x.userId===userId)) { skipped++; continue; }
    bl.push({ userId, note:note||"Bulk import", addedAt:now, addedBy:req.session.user.id });
    added++;
  }
  saveBlacklist(bl);
  res.json({ ok:true, added, skipped });
});

// ─── Helper: fire webhooks ────────────────────────────────────────────────────
async function fireWebhooks(event, data) {
  const hooks = loadWebhooks().filter(w => w.events.includes(event));
  for (const wh of hooks) {
    try {
      const color = event==="done" ? 0x23d18b : event==="error" ? 0xf04747 : 0x5865f2;
      const embed = {
        title: event==="done" ? "✅ Broadcast Complete" : event==="error" ? "❌ Broadcast Error" : "📡 Broadcast Update",
        color,
        fields: Object.entries(data).map(([name,value])=>({ name, value:String(value), inline:true })),
        timestamp: new Date().toISOString(),
        footer: { text: "Broadcast Bot" },
      };
      await axios.post(wh.url, { embeds:[embed] });
    } catch {}
  }
}

// ─── WebSocket ────────────────────────────────────────────────────────────────
wss.on("connection", (ws) => {
  let authed = false;
  ws.on("message", async (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (!authed) {
      if (msg.action==="auth"&&msg.userId) {
        const data = loadData();
        const ok   = data.botOwnerId===msg.userId||data.owners.includes(msg.userId);
        if (!ok) { wsSend(ws,{type:"error",msg:"No permission"}); ws.close(); return; }
        authed = true; wsClients.set(ws,{id:msg.userId}); wsSend(ws,{type:"authed"});
      } else { wsSend(ws,{type:"error",msg:"Unauthorized"}); ws.close(); }
      return;
    }
    if (msg.action==="broadcast")      await handleWsBroadcast(ws, msg);
    if (msg.action==="broadcast_custom") await handleWsCustomBroadcast(ws, msg);
    if (msg.action==="retry")          await handleWsRetry(ws, msg);
    if (msg.action==="test_broadcast") await handleWsTest(ws, msg);
    if (msg.action==="stop")            { ws._stopped = true; }
    if (msg.action==="pause")           { ws._paused = true; wsSend(ws,{type:"paused"}); }
    if (msg.action==="resume")          { ws._paused = false; wsSend(ws,{type:"resumed"}); }
  });
  ws.on("close", () => wsClients.delete(ws));
});

function wsSend(ws, obj) { if (ws.readyState===1) ws.send(JSON.stringify(obj)); }

// ─── Core broadcast engine ────────────────────────────────────────────────────
async function broadcastToMembers(ws, { tokens, memberList, message, messages=null, delay, guildId, groupName, mode, roleId, sessionKey, imageUrl="", embed=null }) {
  const total  = memberList.length;
  const perBot = Math.ceil(total / tokens.length);

  wsSend(ws, { type:"start", total, bots:tokens.length, group:groupName });

  const stats = tokens.map((token,i) => ({
    index:i, label:`Bot #${i+1}`, token:token.slice(0,12)+"…",
    rawToken:token, sent:0, failed:0, assignedCount:0, completed:false,
  }));
  let globalSent=0, globalFailed=0;
  const failedMembers = [];

  await Promise.all(tokens.map(async (token,bi) => {
    const slice = memberList.slice(bi*perBot, (bi+1)*perBot);
    stats[bi].assignedCount = slice.length;
    const rest = new REST({version:"10"}).setToken(token);

    // Validate token
    try { await rest.get(Routes.user()); } catch {
      stats[bi].failed=slice.length; stats[bi].completed=true; globalFailed+=slice.length;
      failedMembers.push(...slice.map(m=>m.user.id));
      wsSend(ws,{type:"stat",stats:sanitizeStats(stats),globalSent,globalFailed,total}); return;
    }

    for (const member of slice) {
      if (ws._stopped) break;
      // Pause support
      while (ws._paused && !ws._stopped) { await sleep(500); }
      if (ws._stopped) break;
      await sleep(delay);

      // Rate limit aware send
      let retries = 0;
      while (retries < 3) {
        try {
          const dm = await rest.post(Routes.userChannels(),{body:{recipient_id:member.user.id}});
          const msgList = messages && messages.length ? messages : [message];
          for (let mi=0; mi<msgList.length; mi++) {
            const msgBody = { content: msgList[mi]+`\n<@${member.user.id}>` };
            if (embed && mi===msgList.length-1) {
              msgBody.embeds = [{
                title: embed.title||undefined,
                description: embed.description||undefined,
                color: embed.color ? parseInt(embed.color.replace("#",""),16) : undefined,
                image: (embed.imageUrl||imageUrl) ? { url: embed.imageUrl||imageUrl } : undefined,
                footer: embed.footer ? { text: embed.footer } : undefined,
                thumbnail: embed.thumbnail ? { url: embed.thumbnail } : undefined,
              }];
            } else if (imageUrl && mi===msgList.length-1) {
              msgBody.embeds = [{ image: { url: imageUrl } }];
            }
            await rest.post(Routes.channelMessages(dm.id),{body:msgBody});
            if (mi < msgList.length-1) await sleep(1000);
          }
          stats[bi].sent++; globalSent++;
          break;
        } catch(e) {
          const isRateLimit = e?.status===429 || (e?.message||"").includes("rate");
          if (isRateLimit && retries < 2) {
            const waitMs = (e?.rawError?.retry_after || 5) * 1000;
            wsSend(ws, { type:"ratelimit", bot:bi, waitMs, msg:`Bot #${bi+1} rate limited — waiting ${Math.ceil(waitMs/1000)}s` });
            await sleep(waitMs);
            retries++;
          } else {
            stats[bi].failed++; globalFailed++;
            failedMembers.push(member.user.id);
            break;
          }
        }
      }
      wsSend(ws,{type:"stat",stats:sanitizeStats(stats),globalSent,globalFailed,total});
    }
    stats[bi].completed=true;
    wsSend(ws,{type:"stat",stats:sanitizeStats(stats),globalSent,globalFailed,total});
  }));

  // Save failed members for retry
  if (failedMembers.length > 0 && sessionKey) {
    const f = loadFailed();
    f[sessionKey] = { groupName, guildId, mode, roleId:roleId||null, message, delay, members:failedMembers, savedAt:new Date().toISOString() };
    saveFailed(f);
  }

  return { stats, globalSent, globalFailed, total, failedCount:failedMembers.length, sessionKey };
}

// ─── Main broadcast handler ───────────────────────────────────────────────────
async function handleWsBroadcast(ws, { groupName, message, messages=null, delay, mode, guildIds, guildId: singleGuildId, roleId, imageUrl="", embed=null, filters={}, dryRun=false }) {
  const resolvedGuildIds = guildIds && guildIds.length ? guildIds : (singleGuildId ? [singleGuildId] : []);
  if (!resolvedGuildIds.length) return wsSend(ws, { type:"error", msg:"No guild selected" });

  const data  = loadData();
  const group = data.groups.find(g => g.name===groupName);
  if (!group) return wsSend(ws, { type:"error", msg:"Group not found" });
  const tokens = data.tokens.slice(0, group.tokenCount);
  if (!tokens.length) return wsSend(ws, { type:"error", msg:"No tokens in group" });

  const bl = new Set(loadBlacklist().map(x => x.userId));
  const sessionKey = `${groupName}_${Date.now()}`;

  // Multi-guild: collect members from all guilds
  let memberList = [];
  for (const guildId of resolvedGuildIds) {
    try {
      const guild = await discordClient.guilds.fetch(guildId);
      const col   = await guild.members.fetch();
      let members = [...col.values()].filter(m => !m.user.bot);
      if (mode==="obc")       members = members.filter(m => m.presence&&["online","idle","dnd"].includes(m.presence.status));
      if (roleId)              members = members.filter(m => m.roles.cache.has(roleId));
      if (bl.size)             members = members.filter(m => !bl.has(m.user.id));
      if (filters.noAvatar)    members = members.filter(m => !m.user.avatar);
      if (filters.hasAvatar)   members = members.filter(m => !!m.user.avatar);
      if (filters.minAge)      members = members.filter(m => m.joinedTimestamp && (Date.now()-m.joinedTimestamp) >= filters.minAge*24*3600000);
      wsSend(ws, { type:"info", msg:`${guild.name}: ${members.length} members` });
      memberList.push(...members.map(m=>({ user:{ id:m.user.id, avatar:m.user.avatar } })));
    } catch(e) { wsSend(ws, { type:"info", msg:`Guild ${guildId} error: ${e.message}` }); }
  }

  // Deduplicate across guilds
  const seen = new Set();
  memberList = memberList.filter(m => { if(seen.has(m.user.id)) return false; seen.add(m.user.id); return true; });
  if (!memberList.length) return wsSend(ws, { type:"error", msg:"No members found" });

  // Dry Run: report count without sending
  if (dryRun) {
    wsSend(ws, { type:"done", globalSent:0, globalFailed:0, total:memberList.length, dryRun:true,
      msg:`🔍 Dry Run — ${memberList.length} members would receive this message (nothing sent)` });
    return;
  }

  const result = await broadcastToMembers(ws, { tokens, memberList, message, messages, delay, guildId:resolvedGuildIds.join(","), groupName, mode, roleId, sessionKey, imageUrl:imageUrl||"", embed });

  // Save log
  appendLog({ type:"manual", groupName, guildId:resolvedGuildIds.join(","), mode, roleId:roleId||null,
    sent:result.globalSent, failed:result.globalFailed, total:result.total, message:message.slice(0,80) });
  updateGroupHistory(groupName, result.globalSent, result.globalFailed);

  // Fire webhooks
  fireWebhooks("done", { Group:groupName, Sent:result.globalSent, Failed:result.globalFailed,
    Total:result.total, Guilds:resolvedGuildIds.length, Mode:mode });

  wsSend(ws, { type:"done", stats:sanitizeStats(result.stats), globalSent:result.globalSent,
    globalFailed:result.globalFailed, total:result.total, failedCount:result.failedCount, sessionKey });
}

// ─── Retry failed handler ─────────────────────────────────────────────────────
async function handleWsRetry(ws, { sessionId }) {
  const f = loadFailed();
  const session = f[sessionId];
  if (!session) return wsSend(ws, { type:"error", msg:"Session not found" });

  const data  = loadData();
  const group = data.groups.find(g => g.name===session.groupName);
  if (!group) return wsSend(ws, { type:"error", msg:"Group not found" });
  const tokens = data.tokens.slice(0, group.tokenCount);
  const memberList = session.members.map(id=>({ user:{ id } }));

  wsSend(ws, { type:"info", msg:`Retrying ${memberList.length} failed members…` });

  const newKey = `${session.groupName}_retry_${Date.now()}`;
  const result = await broadcastToMembers(ws, { tokens, memberList, message:session.message,
    delay:session.delay, guildId:session.guildId, groupName:session.groupName,
    mode:session.mode, roleId:session.roleId, sessionKey:newKey });

  // Remove old session
  delete f[sessionId]; saveFailed(f);

  appendLog({ type:"retry", groupName:session.groupName, guildId:session.guildId,
    sent:result.globalSent, failed:result.globalFailed, total:result.total, message:session.message.slice(0,80) });
  updateGroupHistory(session.groupName, result.globalSent, result.globalFailed);
  fireWebhooks("done", { Group:session.groupName, Type:"Retry", Sent:result.globalSent, Failed:result.globalFailed });

  wsSend(ws, { type:"done", stats:sanitizeStats(result.stats), globalSent:result.globalSent,
    globalFailed:result.globalFailed, total:result.total, failedCount:result.failedCount, sessionKey:newKey });
}

// ─── Test broadcast handler ───────────────────────────────────────────────────
// ─── Custom member list broadcast ─────────────────────────────────────────────
async function handleWsCustomBroadcast(ws, { groupName, message, messages=null, userIds, delay, imageUrl="", embed=null }) {
  if (!groupName || !message || !userIds?.length) return wsSend(ws, { type:"error", msg:"Missing fields" });
  const data  = loadData();
  const group = data.groups.find(g => g.name===groupName);
  if (!group) return wsSend(ws, { type:"error", msg:"Group not found" });
  const tokens = data.tokens.slice(0, group.tokenCount);
  if (!tokens.length) return wsSend(ws, { type:"error", msg:"No tokens in group" });
  const bl = new Set(loadBlacklist().map(x => x.userId));
  const memberList = userIds.filter(id => !bl.has(id)).map(id => ({ user:{ id } }));
  if (!memberList.length) return wsSend(ws, { type:"error", msg:"No valid members" });
  wsSend(ws, { type:"info", msg:`Custom list: ${memberList.length} members` });
  const sessionKey = `custom_${groupName}_${Date.now()}`;
  const result = await broadcastToMembers(ws, { tokens, memberList, message, messages, delay:delay||2000, guildId:"custom", groupName, mode:"custom", roleId:null, sessionKey, imageUrl, embed });
  appendLog({ type:"custom", groupName, guildId:"custom", mode:"custom", sent:result.globalSent, failed:result.globalFailed, total:result.total, message:message.slice(0,80) });
  updateGroupHistory(groupName, result.globalSent, result.globalFailed);
  fireWebhooks("done", { Group:groupName, Type:"Custom", Sent:result.globalSent, Failed:result.globalFailed });
  wsSend(ws, { type:"done", stats:sanitizeStats(result.stats), globalSent:result.globalSent, globalFailed:result.globalFailed, total:result.total, failedCount:result.failedCount, sessionKey });
}

async function handleWsTest(ws, { userId, message }) {
  const data = loadData();
  if (!data.tokens.length) return wsSend(ws, { type:"error", msg:"No tokens available" });
  const token = data.tokens[0];
  const rest  = new REST({version:"10"}).setToken(token);
  wsSend(ws, { type:"start", total:1, bots:1, group:"TEST" });
  try {
    const dm = await rest.post(Routes.userChannels(),{body:{recipient_id:userId}});
    await rest.post(Routes.channelMessages(dm.id),{body:{content:`[TEST] ${message}\n<@${userId}>`}});
    wsSend(ws, { type:"done", stats:[{label:"Bot #1",sent:1,failed:0,assignedCount:1,completed:true,token:token.slice(0,12)+"…"}], globalSent:1, globalFailed:0, total:1, failedCount:0 });
  } catch(e) {
    wsSend(ws, { type:"error", msg:"Test failed: "+e.message });
    fireWebhooks("error", { Type:"Test", Error:e.message });
  }
}



function sanitizeStats(stats) {
  return stats.map(s => ({
    label: s.label, token: s.token, sent: s.sent,
    failed: s.failed, assignedCount: s.assignedCount, completed: s.completed,
  }));
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function fetchBase64(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith("https") ? https : require("http");
    mod.get(url, res => {
      const chunks = [];
      res.on("data", c => chunks.push(c));
      res.on("end", () => {
        const mime = res.headers["content-type"] || "image/png";
        resolve(`data:${mime};base64,${Buffer.concat(chunks).toString("base64")}`);
      });
      res.on("error", reject);
    }).on("error", reject);
  });
}

// (scheduleAllOnStartup is called inside the ready event above)



const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="ar" dir="rtl" id="htmlroot">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Broadcast Bot</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Tajawal:wght@400;500;700;900&family=Inter:wght@400;500;600;700;900&display=swap" rel="stylesheet"/>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{--bg:#0a0b0f;--sf:#111318;--sf2:#181b22;--sf3:#1e2229;--bd:#252832;--bd2:#2e3340;--bl:#5865f2;--bl2:#4752c4;--bl3:rgba(88,101,242,.12);--gr:#23d18b;--rd:#f04747;--yl:#faa61a;--pu:#9b59b6;--tx:#e8eaf0;--tx2:#b0b8cc;--mu:#6b7280;--mo:'IBM Plex Mono',monospace;--sh:0 4px 24px rgba(0,0,0,.4);--sh2:0 8px 40px rgba(0,0,0,.5)}
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;background:var(--bg);color:var(--tx)}
[lang=ar] body{font-family:'Tajawal',sans-serif}[lang=en] body{font-family:'Inter',sans-serif}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--bd2);border-radius:3px}::-webkit-scrollbar-thumb:hover{background:var(--bl)}
/* LOGIN */
#lp{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;gap:1.8rem;position:relative;overflow:hidden}
.lp-bg{position:absolute;inset:0;background:radial-gradient(ellipse 100% 70% at 50% -10%,rgba(88,101,242,.22) 0%,transparent 65%),radial-gradient(ellipse 60% 40% at 80% 80%,rgba(155,89,182,.1) 0%,transparent 60%)}
.lp-grid{position:absolute;inset:0;background-image:linear-gradient(var(--bd) 1px,transparent 1px),linear-gradient(90deg,var(--bd) 1px,transparent 1px);background-size:50px 50px;opacity:.3}
.l-card{position:relative;z-index:1;text-align:center;display:flex;flex-direction:column;align-items:center;gap:1.5rem;background:rgba(17,19,24,.85);border:1px solid var(--bd2);border-radius:24px;padding:3rem 2.5rem;backdrop-filter:blur(20px);box-shadow:var(--sh2);max-width:400px;width:90%}
.l-logo{width:72px;height:72px;border-radius:50%;background:linear-gradient(135deg,var(--bl) 0%,var(--pu) 100%);display:flex;align-items:center;justify-content:center;font-size:2rem;box-shadow:0 0 0 12px rgba(88,101,242,.1),0 0 40px rgba(88,101,242,.4);animation:breathe 3s ease-in-out infinite}
@keyframes breathe{0%,100%{box-shadow:0 0 0 12px rgba(88,101,242,.1),0 0 40px rgba(88,101,242,.4)}50%{box-shadow:0 0 0 20px rgba(88,101,242,.06),0 0 70px rgba(88,101,242,.6)}}
.l-title{font-size:1.9rem;font-weight:900;letter-spacing:-1px}.l-title span{background:linear-gradient(135deg,var(--bl),var(--pu));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.l-sub{color:var(--mu);font-size:.9rem}
.l-btn{display:flex;align-items:center;gap:.7rem;padding:.85rem 1.8rem;background:var(--bl);color:#fff;text-decoration:none;border-radius:12px;font-size:.95rem;font-weight:700;transition:all .2s;box-shadow:0 4px 20px rgba(88,101,242,.4);width:100%;justify-content:center}
.l-btn:hover{background:var(--bl2);transform:translateY(-2px);box-shadow:0 8px 32px rgba(88,101,242,.5)}
.l-btn svg{width:20px;height:20px}
.l-err{background:rgba(240,71,71,.1);border:1px solid rgba(240,71,71,.25);color:var(--rd);padding:.6rem 1rem;border-radius:10px;font-size:.83rem;display:none;width:100%;text-align:center}
/* APP */
#app{display:none;height:100vh;flex-direction:column}#app.show{display:flex}
/* TOPBAR */
.tb{height:54px;background:var(--sf);border-bottom:1px solid var(--bd);display:flex;align-items:center;padding:0 1.4rem;gap:1rem;flex-shrink:0;position:relative;z-index:10}
.tb::after{content:'';position:absolute;bottom:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,var(--bl),transparent);opacity:.3}
.tb-brand{font-size:1rem;font-weight:900;display:flex;align-items:center;gap:.5rem}
.tb-dot{width:8px;height:8px;border-radius:50%;background:var(--gr);box-shadow:0 0 8px var(--gr);animation:blink 2s ease-in-out infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.tb-brand .n span{color:var(--bl)}
.tb-sp{flex:1}
.theme-btn{background:var(--sf2);border:1px solid var(--bd);color:var(--mu);width:32px;height:32px;border-radius:8px;cursor:pointer;font-size:.9rem;display:flex;align-items:center;justify-content:center;transition:all .15s}
.theme-btn:hover{border-color:var(--bl);color:var(--bl)}
.theme-opt{padding:.35rem .7rem;border-radius:7px;cursor:pointer;font-size:.82rem;font-weight:600;transition:background .15s}
.theme-opt:hover{background:var(--sf2)}
#theme-dd.open{display:flex!important}
.lang-wrap{display:flex;background:var(--sf2);border:1px solid var(--bd);border-radius:8px;padding:2px;gap:2px}
.lb{background:none;border:none;color:var(--mu);padding:.22rem .5rem;border-radius:6px;cursor:pointer;font-size:.72rem;font-weight:800;font-family:var(--mo);transition:all .15s}
.lb.on{background:var(--bl);color:#fff;box-shadow:0 2px 8px rgba(88,101,242,.4)}
.uc{display:flex;align-items:center;gap:.5rem;background:var(--sf2);border:1px solid var(--bd);padding:.22rem .7rem .22rem .3rem;border-radius:999px}
.uc img{width:26px;height:26px;border-radius:50%;border:1.5px solid var(--bl)}
.un{font-size:.8rem;font-weight:700}.ur{font-size:.68rem;color:var(--mu);font-family:var(--mo)}
.btn-lo{background:none;border:1px solid var(--bd);color:var(--mu);padding:.28rem .75rem;border-radius:6px;cursor:pointer;font-size:.76rem;transition:all .15s}
.btn-lo:hover{border-color:var(--rd);color:var(--rd)}
/* LAYOUT */
.main{display:flex;flex:1;overflow:hidden}
/* SIDEBAR */
.sb{width:215px;background:var(--sf);border-left:1px solid var(--bd);flex-shrink:0;padding:.8rem 0;overflow-y:auto;display:flex;flex-direction:column}
[lang=en] .sb{border-left:none;border-right:1px solid var(--bd)}
.sb-sec{padding:.6rem 1.1rem .2rem;font-size:.65rem;font-weight:700;color:var(--mu);letter-spacing:.1em;text-transform:uppercase;margin-top:.3rem}
.ni{display:flex;align-items:center;gap:.6rem;padding:.58rem 1.1rem;cursor:pointer;border-right:2px solid transparent;transition:all .18s;color:var(--mu);font-size:.86rem;font-weight:600;user-select:none;margin:1px 0}
[lang=en] .ni{border-right:none;border-left:2px solid transparent}
.ni:hover{color:var(--tx);background:var(--sf2)}
.ni.on{color:var(--tx);border-right-color:var(--bl);background:linear-gradient(90deg,var(--bl3),transparent)}
[lang=en] .ni.on{border-right-color:transparent;border-left-color:var(--bl);background:linear-gradient(270deg,var(--bl3),transparent)}
.ni-ic{font-size:.95rem;width:18px;text-align:center}
.ni-badge{margin-right:auto;margin-left:.2rem;background:var(--bl);color:#fff;font-size:.65rem;font-weight:700;padding:.05rem .4rem;border-radius:999px;min-width:18px;text-align:center}
[lang=en] .ni-badge{margin-right:.2rem;margin-left:auto}
/* CONTENT */
.cnt{flex:1;overflow-y:auto;padding:1.8rem}
.pg{display:none;animation:fadeIn .2s ease}.pg.on{display:block}
@keyframes fadeIn{from{opacity:0;transform:translateY(8px)}}
.pt{font-size:1.35rem;font-weight:900;margin-bottom:1.4rem;letter-spacing:-.5px;display:flex;align-items:center;gap:.6rem}
/* STATS */
.sg{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1rem;margin-bottom:1.6rem}
.sc{background:var(--sf);border:1px solid var(--bd);border-radius:14px;padding:1.1rem;position:relative;overflow:hidden;transition:transform .15s,border-color .15s;cursor:default}
.sc:hover{transform:translateY(-2px);border-color:var(--bd2)}
.sc::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--bl),var(--pu));opacity:0;transition:opacity .2s}
.sc:hover::before{opacity:1}
.sl{font-size:.7rem;color:var(--mu);font-weight:700;text-transform:uppercase;letter-spacing:.08em;margin-bottom:.5rem}
.sv{font-size:1.9rem;font-weight:900;font-family:var(--mo);line-height:1;background:linear-gradient(135deg,var(--tx),var(--tx2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.ss{font-size:.76rem;color:var(--mu);margin-top:.25rem}
.sc-icon{position:absolute;top:.8rem;left:.9rem;font-size:1.4rem;opacity:.12}
[lang=en] .sc-icon{left:auto;right:.9rem}
/* CARD */
.card{background:var(--sf);border:1px solid var(--bd);border-radius:14px;overflow:hidden;margin-bottom:1.3rem;transition:border-color .15s}
.card:hover{border-color:var(--bd2)}
.ch{padding:.85rem 1.1rem;border-bottom:1px solid var(--bd);display:flex;align-items:center;justify-content:space-between;background:var(--sf2)}
.ct{font-size:.88rem;font-weight:700}.cb{padding:1.1rem}
/* TABLE */
table{width:100%;border-collapse:collapse;font-size:.84rem}
th{color:var(--mu);font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;padding:.55rem .75rem;text-align:right;border-bottom:1px solid var(--bd);background:var(--sf2)}
[lang=en] th{text-align:left}
td{padding:.7rem .75rem;border-bottom:1px solid var(--bd);vertical-align:middle}
tr:last-child td{border-bottom:none}
tr{transition:background .12s}tr:hover td{background:var(--sf2)}
/* BADGE */
.b{display:inline-flex;align-items:center;gap:.22rem;padding:.17rem .5rem;border-radius:6px;font-size:.71rem;font-weight:700;font-family:var(--mo)}
.b-gr{background:rgba(35,209,139,.12);color:var(--gr)}.b-rd{background:rgba(240,71,71,.12);color:var(--rd)}
.b-yl{background:rgba(250,166,26,.12);color:var(--yl)}.b-mu{background:rgba(107,114,128,.12);color:var(--mu)}
.b-bl{background:rgba(88,101,242,.14);color:#818cf8}.b-pu{background:rgba(155,89,182,.12);color:#c084fc}
/* BTNS */
.btn{display:inline-flex;align-items:center;gap:.38rem;padding:.48rem .95rem;border-radius:9px;font-size:.82rem;font-weight:700;cursor:pointer;border:none;transition:all .15s;white-space:nowrap}
[lang=ar] .btn{font-family:'Tajawal',sans-serif}[lang=en] .btn{font-family:'Inter',sans-serif}
.bp{background:linear-gradient(135deg,var(--bl),var(--bl2));color:#fff;box-shadow:0 2px 12px rgba(88,101,242,.3)}
.bp:hover{transform:translateY(-1px);box-shadow:0 4px 20px rgba(88,101,242,.45)}
.br{background:rgba(240,71,71,.1);color:var(--rd);border:1px solid rgba(240,71,71,.2)}.br:hover{background:rgba(240,71,71,.2)}
.bg{background:var(--sf2);color:var(--tx2);border:1px solid var(--bd)}.bg:hover{border-color:var(--bl);color:var(--bl)}
.bs{background:rgba(35,209,139,.1);color:var(--gr);border:1px solid rgba(35,209,139,.2)}.bs:hover{background:rgba(35,209,139,.2)}
.btn:disabled{opacity:.4;cursor:not-allowed;transform:none!important}
.btn-sm{padding:.26rem .62rem;font-size:.74rem;border-radius:7px}
/* FORM */
.fg{margin-bottom:.9rem}.fl{display:block;font-size:.77rem;font-weight:700;margin-bottom:.38rem;color:var(--tx2)}
.fi,.fta,.fse{width:100%;background:var(--sf2);border:1px solid var(--bd);color:var(--tx);padding:.6rem .85rem;border-radius:9px;font-size:.86rem;outline:none;transition:border-color .15s,box-shadow .15s}
[lang=ar] .fi,[lang=ar] .fta,[lang=ar] .fse{font-family:'Tajawal',sans-serif}
[lang=en] .fi,[lang=en] .fta,[lang=en] .fse{font-family:'Inter',sans-serif}
.fi:focus,.fta:focus,.fse:focus{border-color:var(--bl);box-shadow:0 0 0 3px rgba(88,101,242,.12)}
.fta{resize:vertical;min-height:90px;line-height:1.5}.fse option{background:var(--sf2)}
.fr2{display:grid;grid-template-columns:1fr 1fr;gap:1rem}
.fr3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:1rem}
.fh{font-size:.72rem;color:var(--mu);margin-top:.28rem;display:flex;align-items:center;gap:.3rem}
/* SEARCH BAR */
.search-wrap{position:relative;margin-bottom:1rem}
.search-wrap .fi{padding-right:2rem}
.search-icon{position:absolute;right:.7rem;top:50%;transform:translateY(-50%);color:var(--mu);font-size:.9rem;pointer-events:none}
[lang=en] .search-wrap .fi{padding-right:.85rem;padding-left:2rem}
[lang=en] .search-icon{right:auto;left:.7rem}
/* MODAL */
.mo{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:200;align-items:center;justify-content:center;backdrop-filter:blur(6px)}
.mo.open{display:flex}
.md{background:var(--sf);border:1px solid var(--bd2);border-radius:18px;padding:2rem;width:480px;max-width:94vw;max-height:88vh;overflow-y:auto;animation:mup .2s cubic-bezier(.34,1.56,.64,1);box-shadow:var(--sh2)}
@keyframes mup{from{transform:translateY(30px) scale(.96);opacity:0}}
.mdt{font-size:1.05rem;font-weight:900;margin-bottom:1.2rem}
.mdf{display:flex;gap:.6rem;justify-content:flex-end;margin-top:1.3rem}
/* GUILD PICKER */
.guild-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:.75rem;max-height:380px;overflow-y:auto;padding:.25rem}
.guild-card{background:var(--sf2);border:1.5px solid var(--bd);border-radius:10px;padding:.75rem;cursor:pointer;transition:all .15s;display:flex;align-items:center;gap:.7rem}
.guild-card:hover{border-color:var(--bl);background:var(--sf3)}
.guild-card.selected{border-color:var(--bl);background:var(--bl3)}
.guild-icon{width:38px;height:38px;border-radius:50%;object-fit:cover;flex-shrink:0}
.guild-icon-placeholder{width:38px;height:38px;border-radius:50%;background:linear-gradient(135deg,var(--bl),var(--pu));display:flex;align-items:center;justify-content:center;font-size:.95rem;font-weight:900;color:#fff;flex-shrink:0}
.guild-name{font-size:.84rem;font-weight:700;line-height:1.2}
.guild-mc{font-size:.72rem;color:var(--mu);font-family:var(--mo)}
/* GUILD INPUT COMBO */
.guild-input-wrap{position:relative}
.guild-preview{display:flex;align-items:center;gap:.6rem;padding:.5rem .75rem;background:var(--sf2);border:1px solid var(--bl);border-radius:8px;margin-top:.4rem}
.guild-preview-icon{width:24px;height:24px;border-radius:50%}
/* BOT ROW */
.brow{display:flex;align-items:center;gap:.6rem}
.bav{width:28px;height:28px;border-radius:50%;border:1.5px solid var(--bd2)}
.bavp{width:28px;height:28px;border-radius:50%;background:linear-gradient(135deg,var(--bl),var(--pu));display:inline-flex;align-items:center;justify-content:center;font-size:.8rem;font-weight:700;color:#fff}
/* BROADCAST */
.bcl{display:grid;grid-template-columns:1fr 360px;gap:1.3rem}
@media(max-width:860px){.bcl{grid-template-columns:1fr}}

/* ── MOBILE RESPONSIVE ───────────────────────────────────── */
@media(max-width:768px){
  .app{flex-direction:column}
  .tb{padding:.5rem .9rem}
  .tb-brand .n{display:none}
  .main{flex-direction:column;overflow:visible}
  .sb{width:100%;border-left:none!important;border-right:none!important;border-bottom:1px solid var(--bd);flex-direction:row;flex-wrap:wrap;padding:.3rem .5rem;gap:.2rem;overflow-x:auto}
  .sb-sec{display:none}
  .ni{padding:.35rem .6rem;border-radius:8px;border:none!important;font-size:.78rem;gap:.3rem;white-space:nowrap}
  .ni.on{background:var(--bl);color:#fff}
  .ni-ic{width:auto}
  .cnt{padding:.9rem}
  .sg{grid-template-columns:repeat(2,1fr)}
  .fr2{grid-template-columns:1fr}
  .card{border-radius:12px}
  .pt{font-size:1.1rem;margin-bottom:1rem}
  .uc .un{display:none}
  .uc .ur{display:none}
}
@media(max-width:480px){
  .sg{grid-template-columns:1fr 1fr}
  .tb{gap:.4rem}
  .cnt{padding:.6rem}
}

/* ── COLOR THEMES ─────────────────────────────────────────── */
body.theme-green{--bl:#2ecc71;--bl2:#27ae60;--bl3:rgba(46,204,113,.12)}
body.theme-red{--bl:#e74c3c;--bl2:#c0392b;--bl3:rgba(231,76,60,.12)}
body.theme-orange{--bl:#e67e22;--bl2:#d35400;--bl3:rgba(230,126,34,.12)}
body.theme-pink{--bl:#e91e8c;--bl2:#c2185b;--bl3:rgba(233,30,140,.12)}
body.theme-cyan{--bl:#00bcd4;--bl2:#0097a7;--bl3:rgba(0,188,212,.12)}
body.theme-yellow{--bl:#f1c40f;--bl2:#f39c12;--bl3:rgba(241,196,15,.12)}
body.light{--bg:#f0f2f5;--sf:#ffffff;--sf2:#f5f6fa;--sf3:#eaedf2;--bd:#dde1ea;--bd2:#c8cdd8;--tx:#1a1d24;--tx2:#4a5568;--mu:#8a94a6;--sh:0 2px 12px rgba(0,0,0,.08);--sh2:0 4px 20px rgba(0,0,0,.12)}

/* ── LIVE CHART ───────────────────────────────────────────── */
#bc-chart-wrap{margin-top:.8rem;display:none}
#bc-chart{width:100%;height:120px;background:var(--sf3);border-radius:10px;border:1px solid var(--bd)}

/* ── KEYBOARD SHORTCUT HINT ──────────────────────────────── */
.kbd{display:inline-block;background:var(--sf3);border:1px solid var(--bd2);border-radius:4px;padding:.05rem .35rem;font-size:.7rem;font-family:var(--mo);color:var(--mu)}
#shortcut-help{position:fixed;bottom:1.2rem;left:1.2rem;background:var(--sf);border:1px solid var(--bd);border-radius:12px;padding:.8rem 1rem;font-size:.78rem;box-shadow:var(--sh2);z-index:999;display:none;max-width:220px}
[lang=ar] #shortcut-help{left:auto;right:1.2rem}
.pbar-out{background:var(--sf2);border-radius:999px;height:6px;overflow:hidden;margin:.4rem 0}
.pbar-in{height:100%;background:linear-gradient(90deg,var(--bl),var(--pu));border-radius:999px;transition:width .35s ease}
.bsr{display:flex;align-items:center;gap:.55rem;padding:.55rem .7rem;border-radius:9px;background:var(--sf2);margin-bottom:.4rem;border:1px solid var(--bd)}
.bsrl{font-size:.78rem;font-weight:700;flex:1}.bsrn{font-size:.73rem;color:var(--mu);font-family:var(--mo)}.bsrp{flex:1}
/* ALERT */
.al{padding:.65rem 1rem;border-radius:9px;font-size:.82rem;margin:.5rem 0;display:flex;align-items:center;gap:.5rem;animation:fadeIn .2s ease}
.al-ok{background:rgba(35,209,139,.09);border:1px solid rgba(35,209,139,.18);color:var(--gr)}
.al-err{background:rgba(240,71,71,.09);border:1px solid rgba(240,71,71,.18);color:var(--rd)}
.al-inf{background:rgba(88,101,242,.09);border:1px solid rgba(88,101,242,.18);color:#a5b4fc}
.al-yl{background:rgba(250,166,26,.09);border:1px solid rgba(250,166,26,.18);color:var(--yl)}
/* SPINNER */
.sp{display:inline-block;width:14px;height:14px;border:2px solid var(--bd2);border-top-color:var(--bl);border-radius:50%;animation:spin .6s linear infinite;flex-shrink:0}
@keyframes spin{to{transform:rotate(360deg)}}
/* EMPTY */
.em{text-align:center;padding:3rem 1rem;color:var(--mu)}.ei{font-size:2.5rem;margin-bottom:.7rem;opacity:.5}.et{font-size:.88rem}
/* OWNER ROW */
.or{display:flex;align-items:center;gap:.7rem;padding:.7rem;border-radius:10px;background:var(--sf2);margin-bottom:.45rem;border:1px solid var(--bd);transition:border-color .15s}
.or:hover{border-color:var(--bd2)}.or img{width:34px;height:34px;border-radius:50%;border:1.5px solid var(--bd2)}
.orn{font-weight:700;font-size:.87rem}.ori{font-size:.72rem;color:var(--mu);font-family:var(--mo)}
/* LOG ROW */
.lr{display:flex;align-items:flex-start;gap:.65rem;padding:.75rem;border-radius:10px;background:var(--sf2);margin-bottom:.4rem;border:1px solid var(--bd);transition:border-color .15s}
.lr:hover{border-color:var(--bd2)}.li{font-size:1.1rem;flex-shrink:0;margin-top:1px}.linf{flex:1;font-size:.8rem}.lt{font-size:.69rem;color:var(--mu);font-family:var(--mo);margin-top:.2rem}
/* SCHEDULE ROW */
.shr{display:flex;align-items:center;gap:.7rem;padding:.8rem 1rem;border-radius:10px;background:var(--sf2);margin-bottom:.4rem;border:1px solid var(--bd);transition:all .15s}
.shr:hover{border-color:var(--bd2)}.shi{flex:1;font-size:.83rem}.sht{font-size:.73rem;color:var(--mu);font-family:var(--mo);margin-top:.18rem}
/* TABS */
.tabs{display:flex;gap:0;margin-bottom:1.2rem;border-bottom:1px solid var(--bd)}
.tab{padding:.55rem 1rem;cursor:pointer;font-size:.83rem;font-weight:600;color:var(--mu);border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .15s;display:flex;align-items:center;gap:.35rem}
.tab.on{color:var(--tx);border-bottom-color:var(--bl)}.tab:hover:not(.on){color:var(--tx2)}
/* MISC */
.live-dot{width:6px;height:6px;border-radius:50%;background:var(--gr);box-shadow:0 0 6px var(--gr);display:inline-block;animation:blink 1.5s ease-in-out infinite}
.countdown{font-family:var(--mo);font-size:.72rem;color:var(--yl);background:rgba(250,166,26,.1);border:1px solid rgba(250,166,26,.2);padding:.15rem .5rem;border-radius:6px;display:inline-block}
.qa-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(195px,1fr));gap:.9rem;margin-bottom:1.4rem}
.qa-card{background:var(--sf);border:1px solid var(--bd);border-radius:14px;padding:1.1rem;cursor:pointer;transition:all .18s;display:flex;align-items:center;gap:.9rem}
.qa-card:hover{border-color:var(--bl);background:var(--sf2);transform:translateY(-2px);box-shadow:0 4px 20px rgba(88,101,242,.15)}
.qa-icon{width:40px;height:40px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:1.3rem;flex-shrink:0}
.qa-icon.bl{background:rgba(88,101,242,.15)}.qa-icon.gr{background:rgba(35,209,139,.12)}.qa-icon.yl{background:rgba(250,166,26,.1)}.qa-icon.pu{background:rgba(155,89,182,.12)}.qa-icon.rd{background:rgba(240,71,71,.1)}
.qa-label{font-size:.88rem;font-weight:700}.qa-sub{font-size:.76rem;color:var(--mu);margin-top:.15rem}
.status-bar{display:flex;align-items:center;gap:.6rem;padding:.6rem 1rem;background:rgba(35,209,139,.07);border:1px solid rgba(35,209,139,.15);border-radius:9px;margin-bottom:1.2rem;font-size:.82rem}
/* TEMPLATE CHIP */
.tpl-chip{display:inline-flex;align-items:center;gap:.4rem;padding:.3rem .7rem;background:var(--sf2);border:1px solid var(--bd);border-radius:8px;font-size:.78rem;cursor:pointer;transition:all .15s;margin:.2rem}
.tpl-chip:hover{border-color:var(--bl);color:var(--bl)}
/* CHART */
.chart-wrap{position:relative;height:180px}
/* LIGHT MODE */
body.light{--bg:#f0f2f5;--sf:#fff;--sf2:#f5f6fa;--sf3:#ecedf2;--bd:#dde1ea;--bd2:#c8ccd6;--tx:#1a1d24;--tx2:#4a5068;--mu:#8a93a8}
body.light .tb,.body.light .sb{background:var(--sf)}
body.light th{background:var(--sf2)}
body.light .al-ok{background:rgba(35,209,139,.08)}body.light .al-err{background:rgba(240,71,71,.08)}body.light .al-inf{background:rgba(88,101,242,.08)}
body.light .l-card{background:rgba(255,255,255,.9)}
</style>
</head>
<body>

<!-- LOGIN -->
<div id="lp">
  <div class="lp-bg"></div><div class="lp-grid"></div>
  <div class="l-card">
    <div class="l-logo">📡</div>
    <div><div class="l-title">Broadcast <span>Bot</span></div><div class="l-sub" id="lsub" style="margin-top:.5rem">لوحة التحكم</div></div>
    <a href="/auth/login" class="l-btn">
      <svg viewBox="0 0 127.14 96.36" fill="#fff"><path d="M107.7,8.07A105.15,105.15,0,0,0,81.47,0a72.06,72.06,0,0,0-3.36,6.83A97.68,97.68,0,0,0,49,6.83,72.37,72.37,0,0,0,45.64,0,105.89,105.89,0,0,0,19.39,8.09C2.79,32.65-1.71,56.6.54,80.21h0A105.73,105.73,0,0,0,32.71,96.36,77.7,77.7,0,0,0,39.6,85.25a68.42,68.42,0,0,1-10.85-5.18c.91-.66,1.8-1.34,2.66-2a75.57,75.57,0,0,0,64.32,0c.87.71,1.76,1.39,2.66,2a68.68,68.68,0,0,1-10.87,5.19,77,77,0,0,0,6.89,11.1A105.25,105.25,0,0,0,126.6,80.22h0C129.24,52.84,122.09,29.11,107.7,8.07ZM42.45,65.69C36.18,65.69,31,60,31,53s5-12.74,11.43-12.74S54,46,53.89,53,48.84,65.69,42.45,65.69Zm42.24,0C78.41,65.69,73.25,60,73.25,53s5-12.74,11.44-12.74S96.23,46,96.12,53,91.08,65.69,84.69,65.69Z"/></svg>
      <span id="lbtn">تسجيل الدخول بـ Discord</span>
    </a>
    <div class="l-err" id="lerr"></div>
  </div>
</div>

<!-- APP -->
<div id="app">
  <div class="tb">
    <div class="tb-brand"><div class="tb-dot"></div><div class="n">Broadcast <span>Bot</span></div></div>
    <div class="tb-sp"></div>
    <button class="theme-btn" onclick="toggleTheme()" title="Toggle theme" id="theme-dark-btn">🌙</button>
    <div style="position:relative" id="theme-picker-wrap">
      <button class="theme-btn" onclick="document.getElementById('theme-dd').classList.toggle('open')" title="ثيم اللون">🎨</button>
      <div id="theme-dd" style="display:none;position:absolute;top:calc(100% + 6px);left:0;background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:.5rem;box-shadow:var(--sh2);z-index:99;flex-direction:column;gap:.3rem;min-width:140px">
        <div onclick="setTheme('')" class="theme-opt">⚫ الافتراضي</div>
        <div onclick="setTheme('green')" class="theme-opt" style="color:#2ecc71">🟢 أخضر</div>
        <div onclick="setTheme('red')" class="theme-opt" style="color:#e74c3c">🔴 أحمر</div>
        <div onclick="setTheme('orange')" class="theme-opt" style="color:#e67e22">🟠 برتقالي</div>
        <div onclick="setTheme('pink')" class="theme-opt" style="color:#e91e8c">🩷 وردي</div>
        <div onclick="setTheme('cyan')" class="theme-opt" style="color:#00bcd4">🔵 سماوي</div>
        <div onclick="setTheme('yellow')" class="theme-opt" style="color:#f1c40f">🟡 ذهبي</div>
      </div>
    </div>
    <button class="theme-btn" onclick="toggleShortcutHelp()" title="اختصارات">⌨️</button>
    <div class="lang-wrap">
      <button class="lb on" id="lb-ar" onclick="setLang('ar')">ع</button>
      <button class="lb" id="lb-en" onclick="setLang('en')">EN</button>
    </div>
    <div class="uc"><img id="tb-av" src="" alt=""/><div><div class="un" id="tb-un">—</div><div class="ur" id="tb-ur">—</div></div></div>
    <button class="btn-lo" id="btn-lo" onclick="location.href='/auth/logout'">خروج</button>
  </div>
  <div class="main">
    <nav class="sb">
      <div class="sb-sec" id="ns1">عام</div>
      <div class="ni on" data-p="overview"><span class="ni-ic">🏠</span><span id="ni-overview">الرئيسية</span></div>
      <div class="sb-sec" id="ns2">إدارة</div>
      <div class="ni" data-p="tokens"><span class="ni-ic">🔑</span><span id="ni-tokens">التوكنات</span></div>
      <div class="ni" data-p="groups"><span class="ni-ic">📦</span><span id="ni-groups">المجموعات</span></div>
      <div class="ni" data-p="owners"><span class="ni-ic">👑</span><span id="ni-owners">الصلاحيات</span></div>
      <div class="ni" data-p="blacklist"><span class="ni-ic">🚫</span><span id="ni-blacklist">القائمة السوداء</span></div>
      <div class="sb-sec" id="ns3">بث</div>
      <div class="ni" data-p="broadcast"><span class="ni-ic">📢</span><span id="ni-broadcast">بث رسالة</span></div>
      <div class="ni" data-p="schedule"><span class="ni-ic">⏰</span><span id="ni-schedule">بث مجدول</span><span class="ni-badge" id="sched-badge" style="display:none">0</span></div>
      <div class="sb-sec" id="ns4">أخرى</div>
      <div class="ni" data-p="retry"><span class="ni-ic">♻️</span><span id="ni-retry">إعادة الإرسال</span><span class="ni-badge" id="retry-badge" style="display:none">0</span></div>
      <div class="ni" data-p="custbc"><span class="ni-ic">🎯</span><span id="ni-custbc">بث مخصص</span></div>
      <div class="ni" data-p="webhooks"><span class="ni-ic">🔔</span><span id="ni-webhooks">الـ Webhooks</span></div>
      <div class="ni" data-p="audit"><span class="ni-ic">🔍</span><span id="ni-audit">سجل التدقيق</span></div>
      <div class="ni" data-p="stats"><span class="ni-ic">📊</span><span id="ni-stats">الإحصائيات</span></div>
      <div class="ni" data-p="logs"><span class="ni-ic">📋</span><span id="ni-logs">السجلات</span></div>
      <div class="ni" data-p="bots"><span class="ni-ic">🤖</span><span id="ni-bots">إعدادات البوتات</span></div>
      <div class="sb-sec">متقدم</div>
      <div class="ni" data-p="embed"><span class="ni-ic">🎨</span><span>Embed Builder</span></div>
      <div class="ni" data-p="recurring"><span class="ni-ic">🔁</span><span>بث متكرر</span><span class="ni-badge" id="rec-badge" style="display:none">0</span></div>
      <div class="ni" data-p="roles"><span class="ni-ic">🛡️</span><span>الصلاحيات</span></div>
      <div class="ni" data-p="backup"><span class="ni-ic">💾</span><span>النسخ الاحتياطي</span></div>
      <div class="ni" data-p="security"><span class="ni-ic">🔐</span><span>الأمان</span></div>
      <div class="ni" data-p="bothealth"><span class="ni-ic">🤖</span><span>صحة البوتات</span></div>
    </nav>

    <div class="cnt">

      <!-- OVERVIEW -->
      <div class="pg on" id="pg-overview">
        <div class="pt">🏠 <span id="pt-ov">الرئيسية</span></div>
        <div id="bot-status-bar" class="status-bar" style="display:none"><div class="live-dot"></div><span id="bot-st">البوت متصل</span><span style="color:var(--mu);font-family:var(--mo);font-size:.75rem;margin-right:auto" id="bot-stname"></span></div>
        <div class="sg">
          <div class="sc"><div class="sc-icon">🔑</div><div class="sl" id="sl-tok">التوكنات</div><div class="sv" id="ov-tok">—</div><div class="ss" id="ss-tok">بوت مسجّل</div></div>
          <div class="sc"><div class="sc-icon">📦</div><div class="sl" id="sl-grp">المجموعات</div><div class="sv" id="ov-grp">—</div><div class="ss" id="ss-grp">مجموعة</div></div>
          <div class="sc"><div class="sc-icon">📨</div><div class="sl" id="sl-sent">مجموع المُرسل</div><div class="sv" id="ov-sent">—</div><div class="ss" id="ss-sent">رسالة</div></div>
          <div class="sc"><div class="sc-icon">⏰</div><div class="sl" id="sl-sch">المجدولة</div><div class="sv" id="ov-sch">—</div><div class="ss" id="ss-sch">بث معلّق</div></div>
        </div>
        <div class="qa-grid">
          <div class="qa-card" onclick="gp('broadcast')"><div class="qa-icon bl">📢</div><div><div class="qa-label" id="qa-bc">بث رسالة</div><div class="qa-sub" id="qa-bc-sub">أرسل للأعضاء الآن</div></div></div>
          <div class="qa-card" onclick="gp('schedule')"><div class="qa-icon yl">⏰</div><div><div class="qa-label" id="qa-sc">جدولة بث</div><div class="qa-sub" id="qa-sc-sub">حدد وقت ويوم</div></div></div>
          <div class="qa-card" onclick="gp('stats')"><div class="qa-icon gr">📊</div><div><div class="qa-label" id="qa-st">الإحصائيات</div><div class="qa-sub" id="qa-st-sub">تحليل البث</div></div></div>
          <div class="qa-card" onclick="gp('logs')"><div class="qa-icon pu">📋</div><div><div class="qa-label" id="qa-lg">السجلات</div><div class="qa-sub" id="qa-lg-sub">تاريخ كل البث</div></div></div>
        </div>
        <div class="card">
          <div class="ch"><span class="ct" id="ct-recent">🕐 آخر النشاط</span></div>
          <div class="cb" id="recent-list"><div class="em"><div class="sp"></div></div></div>
        </div>
      </div>

      <!-- TOKENS -->
      <div class="pg" id="pg-tokens">
        <div class="pt">🔑 <span id="pt-tok">التوكنات</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct" id="ct-tadd">➕ إضافة توكنات</span></div>
          <div class="cb">
            <div class="fg"><label class="fl" id="fl-tk">التوكنات (سطر لكل توكن)</label><textarea class="fta" id="tok-in" style="min-height:110px" placeholder="MTE4Nz...&#10;MTE4OD..."></textarea><div class="fh">ℹ️ <span id="fh-tk">كل توكن في سطر — التكرار يُتجاهل</span></div></div>
            <div style="display:flex;align-items:center;gap:.75rem;flex-wrap:wrap">
              <button class="btn bp" onclick="addTokens()" id="btn-tadd">➕ إضافة</button>
              <button class="btn bg" onclick="checkTokens()" id="btn-chk">🔍 فحص الكل</button>
              <span id="tmsg"></span>
            </div>
          </div>
        </div>
        <div class="card">
          <div class="ch"><span class="ct" id="ct-tlist">قائمة التوكنات</span><button class="btn bg btn-sm" onclick="loadTokens()">🔄</button></div>
          <div id="tok-wrap"><div class="em"><div class="sp"></div></div></div>
        </div>
      </div>

      <!-- GROUPS -->
      <div class="pg" id="pg-groups">
        <div class="pt">📦 <span id="pt-grp">المجموعات</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct" id="ct-gnew">🆕 مجموعة جديدة</span></div>
          <div class="cb">
            <div class="fr2">
              <div class="fg"><label class="fl" id="fl-gn">اسم المجموعة</label><input class="fi" id="gn" placeholder="Alpha Squad"/></div>
              <div class="fg"><label class="fl" id="fl-gc">عدد التوكنات</label><input class="fi" id="gc" type="number" min="1" placeholder="5"/></div>
            </div>
            <div style="display:flex;align-items:center;gap:.75rem"><button class="btn bp" onclick="createGroup()" id="btn-gc">إنشاء</button><span id="gmsg"></span></div>
          </div>
        </div>
        <div class="card"><div class="ch"><span class="ct" id="ct-glist">المجموعات</span><button class="btn bg btn-sm" onclick="loadGroups()">🔄</button></div><div id="grp-wrap"><div class="em"><div class="sp"></div></div></div></div>
        <!-- Group history -->
        <div class="card" style="margin-top:1.2rem">
          <div class="ch"><span class="ct" id="ct-grphist"><span data-i18n="ct-grphist">📊 إحصائيات المجموعات</span></span><button class="btn bg btn-sm" onclick="loadGroupHistory()">🔄</button></div>
          <div class="cb" id="grp-hist-wrap"><div class="em"><div class="sp"></div></div></div>
        </div>
      </div>

      <!-- OWNERS -->
      <div class="pg" id="pg-owners">
        <div class="pt">👑 <span id="pt-own">الصلاحيات</span></div>
        <div class="card" style="margin-bottom:1.2rem"><div class="ch"><span class="ct" id="ct-oadd">➕ إضافة Owner</span></div><div class="cb"><div class="fg"><label class="fl">Discord User ID</label><input class="fi" id="oin" placeholder="123456789012345678"/></div><div style="display:flex;align-items:center;gap:.75rem"><button class="btn bp" onclick="addOwner()" id="btn-oadd">إضافة</button><span id="omsg"></span></div></div></div>
        <div class="card"><div class="ch"><span class="ct" id="ct-olist">Owners</span><button class="btn bg btn-sm" onclick="loadOwners()">🔄</button></div><div class="cb" id="own-list"><div class="em"><div class="sp"></div></div></div></div>
      </div>

      <!-- BLACKLIST -->
      <div class="pg" id="pg-blacklist">
        <div class="pt">🚫 <span id="pt-bl">القائمة السوداء</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct" id="ct-bladd">➕ إضافة للقائمة السوداء</span></div>
          <div class="cb">
            <div class="fr2">
              <div class="fg"><label class="fl" id="fl-blid">User ID</label><input class="fi" id="bl-id" placeholder="123456789012345678"/></div>
              <div class="fg"><label class="fl" id="fl-blnote">ملاحظة (اختياري)</label><input class="fi" id="bl-note" data-i18n-ph="bl-note-ph" placeholder="طلب إيقاف البث"/></div>
            </div>
            <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:.8rem"><button class="btn br" onclick="addBlacklist()" id="btn-bladd">🚫 إضافة</button><span id="blmsg"></span></div>
            <div style="border-top:1px solid var(--bd);padding-top:.8rem">
              <div class="fg"><label class="fl" id="fl-blbulk"><span data-i18n="fl-blbulk">📋 استيراد بالجملة (ID في كل سطر)</span></label><textarea class="fta" id="bl-bulk" style="min-height:80px" placeholder="123456789012345678&#10;987654321098765432"></textarea></div>
              <div style="display:flex;align-items:center;gap:.75rem"><button class="btn bg" onclick="bulkAddBL()" id="btn-blbulk">📥 <span data-i18n="btn-blbulk">استيراد</span></button><span id="bl-bulk-msg"></span></div>
            </div>
          </div>
        </div>
        <div class="card"><div class="ch"><span class="ct" id="ct-bllist">المحظورون</span><button class="btn bg btn-sm" onclick="loadBlacklist()">🔄</button></div><div id="bl-wrap"><div class="em"><div class="sp"></div></div></div></div>
      </div>

      <!-- BROADCAST -->
      <div class="pg" id="pg-broadcast">
        <div class="pt">📢 <span id="pt-bc">بث رسالة</span></div>
        <div class="bcl">
          <div>
            <div class="card">
              <div class="ch"><span class="ct" id="ct-bset">⚙️ إعدادات البث</span></div>
              <div class="cb">
                <div class="tabs">
                  <div class="tab on" id="bt-all" onclick="bcTabFn('all')"><span id="tab-all-txt">📢 كل الأعضاء</span></div>
                  <div class="tab" id="bt-role" onclick="bcTabFn('role')"><span id="tab-role-txt">🏷️ حسب الرتبة</span></div>
                </div>
                <div class="fr2">
                  <div class="fg"><label class="fl" id="fl-bcg">المجموعة</label><select class="fse" id="bc-grp"></select></div>
                  <div class="fg">
                    <label class="fl" id="fl-bcgid"><span data-i18n="fl-bcgid">السيرفرات</span></label>
                    <div style="display:flex;gap:.5rem">
                      <button class="btn bg btn-sm" onclick="loadGuilds('bc')" id="btn-lguild" style="white-space:nowrap">📡 <span data-i18n="btn-lguild-txt">تحميل</span></button>
                      <button class="btn bg btn-sm" onclick="selAllGuilds()" id="btn-selall" style="white-space:nowrap">☑️ <span data-i18n="btn-selall">الكل</span></button>
                    </div>
                    <div id="bc-guilds-wrap" style="margin-top:.5rem;max-height:180px;overflow-y:auto;display:flex;flex-direction:column;gap:.3rem"></div>
                    <div id="bc-guild-count" style="font-size:.72rem;color:var(--mu);margin-top:.3rem"></div>
                  </div>
                </div>
                <div id="bc-role-sec" style="display:none">
                  <div class="fg"><label class="fl" id="fl-bcr">الرتبة</label><div style="display:flex;gap:.5rem"><select class="fse" id="bc-role" style="flex:1"></select><button class="btn bg btn-sm" onclick="loadRoles()" id="btn-rl">تحميل</button></div></div>
                </div>
                <div class="fr2">
                  <div class="fg"><label class="fl" id="fl-bcm">نوع البث</label><select class="fse" id="bc-mode"><option value="bc" id="bcm-all">📢 كل الأعضاء</option><option value="obc" id="bcm-obc">🟢 الأونلاين فقط</option></select></div>
                  <div class="fg"><label class="fl" id="fl-bcs">السرعة</label><select class="fse" id="bc-spd"><option value="5000" id="bcs-slow">🐢 بطيء (5s)</option><option value="2000" selected id="bcs-norm">🚶 متوسط (2s)</option><option value="800" id="bcs-fast">🏃 سريع (0.8s)</option></select></div>
                </div>
                <!-- Member Filters -->
                <details style="margin-bottom:.6rem">
                  <summary style="cursor:pointer;font-size:.82rem;font-weight:700;color:var(--mu);padding:.3rem 0;user-select:none">🔽 فلترة الأعضاء (اختياري)</summary>
                  <div style="margin-top:.5rem;padding:.7rem;background:var(--sf3);border-radius:10px;border:1px solid var(--bd);display:flex;flex-direction:column;gap:.5rem">
                    <label style="display:flex;align-items:center;gap:.5rem;cursor:pointer;font-size:.82rem"><input type="checkbox" id="f-noavatar"/> 🚫 استثناء بدون avatar</label>
                    <label style="display:flex;align-items:center;gap:.5rem;cursor:pointer;font-size:.82rem"><input type="checkbox" id="f-hasavatar"/> ✅ فقط من عندهم avatar</label>
                    <div style="display:flex;align-items:center;gap:.5rem;font-size:.82rem">
                      <label style="white-space:nowrap">📅 عضو منذ أكثر من</label>
                      <input class="fi" id="f-minage" type="number" min="0" placeholder="0" style="width:70px;padding:.25rem .5rem"/>
                      <span style="color:var(--mu)">يوم</span>
                    </div>
                    <label style="display:flex;align-items:center;gap:.5rem;cursor:pointer;font-size:.82rem"><input type="checkbox" id="f-dryrun"/> 🔍 Dry Run (بدون إرسال — يحسب الأعضاء فقط)</label>
                  </div>
                </details>
                <!-- Templates -->
                <div class="fg">
                  <label class="fl" id="fl-bcmsg">الرسالة</label>
                  <div id="tpl-chips" style="margin-bottom:.5rem;display:flex;flex-wrap:wrap;gap:.2rem"></div>
                  <textarea class="fta" id="bc-msg" style="min-height:120px" placeholder="..."></textarea>
                  <div style="display:flex;justify-content:space-between;align-items:center;margin-top:.3rem">
                    <div class="fh">ℹ️ <span id="fh-bc">سيُضاف منشن المستخدم تلقائياً</span></div>
                    <button class="btn bg btn-sm" onclick="saveTplModal()" id="btn-savetpl">💾 حفظ كـ Template</button>
                  </div>
                </div>
                <div style="display:flex;gap:.5rem;margin-bottom:.5rem">
                  <button class="btn bg" id="btn-bc-prev" onclick="previewBroadcast()" style="flex:1;justify-content:center">👁️ <span id="btn-prev-txt">معاينة</span></button>
                  <button class="btn bs" id="btn-bc-test" onclick="testBroadcast()" style="flex:1;justify-content:center">🧪 <span id="btn-test-txt">اختبار</span></button>
                </div>
                <div class="fg" id="bc-img-wrap" style="margin-bottom:.6rem">
                  <label class="fl" id="fl-bcimg">🖼️ <span data-i18n="fl-bcimg">رابط صورة (اختياري)</span></label>
                  <input class="fi" id="bc-img-url" placeholder="https://example.com/image.png"/>
                </div>
                <button class="btn bp" id="btn-bc-send" onclick="startBroadcast()" style="width:100%;justify-content:center;padding:.6rem">📡 <span id="btn-bc-txt">بدء البث</span></button>
                <div id="bc-ctrl-btns" style="display:none;margin-top:.5rem;display:none;gap:.4rem">
                  <button class="btn br btn-sm" id="btn-bc-stop" style="flex:1;justify-content:center" onclick="stopBc()">⛔ <span id="btn-stop-txt">إيقاف</span></button>
                  <button class="btn bg btn-sm" id="btn-bc-pause" style="flex:1;justify-content:center" onclick="pauseBc()">⏸️ <span id="btn-pause-txt">إيقاف مؤقت</span></button>
                </div>
                <div id="bc-al"></div>
              </div>
            </div>
          </div>
          <div>
            <div class="card" style="position:sticky;top:0">
              <div class="ch"><span class="ct" id="ct-bstat">📊 مباشر</span><div class="live-dot" id="bc-ld" style="display:none"></div></div>
              <div class="cb">
                <div id="bc-ratelimit" style="display:none;padding:.4rem .6rem;background:rgba(250,166,26,.1);border:1px solid rgba(250,166,26,.25);border-radius:8px;font-size:.78rem;margin-bottom:.5rem">⚠️ <span id="bc-rl-msg"></span></div>
                <div id="bc-chart-wrap"><canvas id="bc-chart"></canvas></div>
                <div id="bc-idle" style="text-align:center;padding:2rem;color:var(--mu)"><div style="font-size:2.2rem;margin-bottom:.6rem;opacity:.4">📡</div><div id="bc-idle-txt" style="font-size:.88rem">لم يبدأ البث بعد</div></div>
                <div id="bc-prog" style="display:none">
                  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.25rem"><span style="font-size:.8rem;font-weight:700;color:var(--tx2)" id="bc-ptxt">الإجمالي</span><span style="font-size:.78rem;font-family:var(--mo);color:var(--mu)" id="bc-pnum">0/0</span></div>
                  <div class="pbar-out"><div class="pbar-in" id="bc-pbar" style="width:0%"></div></div>
                  <div style="display:flex;gap:.4rem;margin:.65rem 0;flex-wrap:wrap">
                    <span class="b b-gr" id="bc-sent">✅ 0</span><span class="b b-rd" id="bc-fail">❌ 0</span><span class="b b-bl" id="bc-rate">—%</span><span class="b b-mu" id="bc-eta">—</span>
                  </div>
                  <div style="font-size:.74rem;font-weight:700;color:var(--mu);margin-bottom:.5rem;text-transform:uppercase;letter-spacing:.06em" id="bc-btxt">البوتات</div>
                  <div id="bc-bstats"></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- SCHEDULE -->
      <div class="pg" id="pg-schedule">
        <div class="pt">⏰ <span id="pt-sc">البث المجدول</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct" id="ct-scnew">➕ جدولة جديدة</span></div>
          <div class="cb">
            <div class="fr2">
              <div class="fg"><label class="fl" id="fl-scg">المجموعة</label><select class="fse" id="sc-grp"></select></div>
              <div class="fg">
                <label class="fl" id="fl-scgid">السيرفر</label>
                <div style="display:flex;gap:.5rem">
                  <select class="fse" id="sc-guild-sel" style="flex:1" onchange="onGuildSelect('sc')"></select>
                  <button class="btn bg btn-sm" onclick="loadGuilds('sc')" id="btn-scguild">تحميل</button>
                </div>
                <div id="sc-guild-preview" style="display:none" class="guild-preview">
                  <img class="guild-preview-icon" id="sc-gprev-icon" src=""/>
                  <div><div style="font-size:.82rem;font-weight:700" id="sc-gprev-name"></div><div style="font-size:.72rem;color:var(--mu);font-family:var(--mo)" id="sc-gprev-id"></div></div>
                </div>
              </div>
            </div>
            <div class="fr2">
              <div class="fg"><label class="fl" id="fl-scdt">📅 التاريخ والوقت</label><input class="fi" id="sc-dt" type="datetime-local"/><div class="fh">🕐 <span id="fh-scdt">توقيت جهازك</span></div></div>
              <div class="fg"><label class="fl" id="fl-scm">نوع البث</label><select class="fse" id="sc-mode"><option value="bc" id="scm-all">📢 كل الأعضاء</option><option value="obc" id="scm-obc">🟢 الأونلاين فقط</option></select></div>
            </div>
            <div class="fr2">
              <div class="fg"><label class="fl" id="fl-scs">السرعة</label><select class="fse" id="sc-spd"><option value="5000" id="scs-slow">🐢 بطيء (5s)</option><option value="2000" selected id="scs-norm">🚶 متوسط (2s)</option><option value="800" id="scs-fast">🏃 سريع (0.8s)</option></select></div>
              <div class="fg"><label class="fl" id="fl-scr">Role ID <span data-i18n="optional-ph" style="font-weight:400;color:var(--mu)">(اختياري)</span></label><input class="fi" id="sc-role" placeholder="(optional)"/></div>
            </div>
            <div class="fg"><label class="fl" id="fl-scmsg">الرسالة</label><textarea class="fta" id="sc-msg" style="min-height:90px"></textarea></div>
            <div style="display:flex;align-items:center;gap:.75rem;flex-wrap:wrap"><button class="btn bp" onclick="createSchedule()" id="btn-scadd">⏰ جدولة</button><span id="scmsg"></span></div>
          </div>
        </div>
        <div class="card"><div class="ch"><span class="ct" id="ct-sclist">البث المجدول</span><button class="btn bg btn-sm" onclick="loadSchedules()">🔄</button></div><div id="sc-list"><div class="em"><div class="sp"></div></div></div></div>
      </div>

      <!-- STATS -->
      <div class="pg" id="pg-stats">
        <div class="pt">📊 <span id="pt-stats">الإحصائيات</span></div>
        <div class="sg">
          <div class="sc"><div class="sc-icon">📨</div><div class="sl" id="sl-total-sent">إجمالي المرسل</div><div class="sv" id="st-total-sent">—</div></div>
          <div class="sc"><div class="sc-icon">❌</div><div class="sl" id="sl-total-fail">إجمالي الفشل</div><div class="sv" id="st-total-fail">—</div></div>
          <div class="sc"><div class="sc-icon">📢</div><div class="sl" id="sl-total-bc">عدد البث</div><div class="sv" id="st-total-bc">—</div></div>
          <div class="sc"><div class="sc-icon">✅</div><div class="sl" id="sl-rate">معدل النجاح</div><div class="sv" id="st-rate">—</div></div>
        </div>
        <div class="card">
          <div class="ch"><span class="ct" id="ct-chart">📈 البث - آخر 14 يوم</span><button class="btn bg btn-sm" onclick="loadStats()">🔄</button></div>
          <div class="cb"><div class="chart-wrap"><canvas id="bc-chart"></canvas></div></div>
        </div>
      </div>

      <!-- LOGS -->
      <div class="pg" id="pg-logs">
        <div class="pt">📋 <span id="pt-logs">السجلات</span></div>
        <div class="card">
          <div class="ch">
            <span class="ct" id="ct-logs">سجل العمليات</span>
            <div style="display:flex;gap:.5rem;flex-wrap:wrap">
              <button class="btn bg btn-sm" onclick="loadLogs()">🔄</button>
              <button class="btn bs btn-sm" onclick="exportLogs('json')">⬇️ JSON</button>
              <button class="btn bs btn-sm" onclick="exportLogs('csv')">⬇️ CSV</button>
              <button class="btn br btn-sm" onclick="clearLogs()" id="btn-lclr">🗑️ مسح</button>
            </div>
          </div>
          <div class="cb" style="padding-bottom:0">
            <div class="search-wrap"><input class="fi" id="log-search" data-i18n-ph="log-search-ph" placeholder="🔍 بحث…" oninput="filterLogs()"/></div>
          </div>
          <div class="cb" id="logs-list" style="padding-top:.5rem"><div class="em"><div class="sp"></div></div></div>
        </div>
      </div>

      <!-- BOT SETTINGS -->
      <div class="pg" id="pg-bots">
        <div class="pt">🤖 <span id="pt-bots">إعدادات البوتات</span></div>
        <div class="fr2">
          <div class="card"><div class="ch"><span class="ct" id="ct-av">🖼️ تغيير الأفاتار</span></div><div class="cb"><div class="fg"><label class="fl" id="fl-av">رابط الصورة</label><input class="fi" id="av-url" placeholder="https://example.com/av.png"/></div><button class="btn bp" onclick="updateAvatar()" id="btn-av">تطبيق على الكل</button><div id="av-msg"></div></div></div>
          <div class="card"><div class="ch"><span class="ct" id="ct-nm">✏️ تغيير الأسماء</span></div><div class="cb"><div class="fg"><label class="fl" id="fl-nm">الاسم الجديد (2–32)</label><input class="fi" id="nm-in" placeholder="BroadcastBot"/></div><button class="btn bp" onclick="updateName()" id="btn-nm">تطبيق على الكل</button><div id="nm-msg"></div></div></div>
        </div>
        <!-- Templates manager -->
        <div class="card">
          <div class="ch"><span class="ct" id="ct-tpl">💾 Templates الرسائل</span><button class="btn bg btn-sm" onclick="loadTemplates()">🔄</button></div>
          <div class="cb" id="tpl-list"><div class="em"><div class="sp"></div></div></div>
        </div>
      </div>


      <!-- AUDIT LOG -->
      <div class="pg" id="pg-audit">
        <div class="pt">🔍 <span id="pt-audit">سجل التدقيق</span></div>
        <div class="card">
          <div class="ch">
            <span class="ct" id="ct-audit">الأحداث الإدارية</span>
            <div style="display:flex;gap:.5rem">
              <button class="btn bg btn-sm" onclick="loadAudit()">🔄</button>
              <button class="btn br btn-sm" onclick="clearAudit()">🗑️ مسح</button>
            </div>
          </div>
          <div class="cb" id="audit-list"><div class="em"><div class="sp"></div></div></div>
        </div>
      </div>

      <!-- CUSTOM BROADCAST -->
      <div class="pg" id="pg-custbc">
        <div class="pt">🎯 <span id="pt-custbc">بث مخصص</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct" id="ct-custbc">إعدادات البث المخصص</span></div>
          <div class="cb">
            <div class="fg"><label class="fl" id="fl-custgrp">المجموعة</label><select class="fse" id="cust-grp"></select></div>
            <div class="fg">
              <label class="fl" id="fl-custids">📋 قائمة User IDs (سطر لكل ID)</label>
              <textarea class="fta" id="cust-ids" style="min-height:120px;font-family:var(--mo);font-size:.8rem" placeholder="123456789012345678&#10;987654321098765432&#10;..." oninput="const ids=[...new Set(this.value.replace(/,/g,' ').split(' ').map(x=>x.trim()).filter(x=>x.length>=17&&x.length<=20&&!isNaN(Number(x))))];document.getElementById('cust-count-lbl').textContent=ids.length+' ID'"></textarea>
              <div class="fh">ℹ️ <span id="fh-custids">سيُرسل فقط للـ IDs المدخلة — القائمة السوداء مستثناة</span></div>
            </div>
            <div class="fg"><label class="fl" id="fl-custimg">🖼️ رابط صورة (اختياري)</label><input class="fi" id="cust-img" placeholder="https://example.com/image.png"/></div>
            <div class="fr2">
              <div class="fg"><label class="fl" id="fl-custspd">السرعة</label><select class="fse" id="cust-spd"><option value="5000">🐢 بطيء (5s)</option><option value="2000" selected>🚶 متوسط (2s)</option><option value="800">🏃 سريع (0.8s)</option></select></div>
              <div class="fg" style="display:flex;align-items:flex-end"><div id="cust-count-lbl" style="font-size:.82rem;color:var(--mu)">0 ID</div></div>
            </div>
            <div class="fg"><label class="fl" id="fl-custmsg">الرسالة</label><textarea class="fta" id="cust-msg" style="min-height:100px"></textarea></div>
            <div style="display:flex;gap:.5rem">
              <button class="btn bp" id="btn-cust-send" onclick="startCustomBroadcast()" style="flex:1;justify-content:center">🎯 <span id="btn-cust-txt">بدء البث المخصص</span></button>
              <button class="btn br btn-sm" id="btn-cust-stop" style="display:none;justify-content:center" onclick="stopBc()">⛔</button>
            </div>
            <div id="cust-al"></div>
          </div>
        </div>
      </div>

      <!-- RETRY FAILED -->
      <div class="pg" id="pg-retry">
        <div class="pt">♻️ <span id="pt-retry">إعادة الإرسال</span></div>
        <div class="card">
          <div class="ch"><span class="ct" id="ct-retry"><span data-i18n="ct-retry">جلسات البث الفاشلة</span></span><button class="btn bg btn-sm" onclick="loadRetry()">🔄</button></div>
          <div class="cb" id="retry-list"><div class="em"><div class="sp"></div></div></div>
        </div>
      </div>

      <!-- WEBHOOKS -->
      <div class="pg" id="pg-webhooks">
        <div class="pt">🔔 <span id="pt-wh">الـ Webhooks</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct" id="ct-whadd">➕ إضافة Webhook</span></div>
          <div class="cb">
            <div class="fr2">
              <div class="fg"><label class="fl" id="fl-whurl">Discord Webhook URL</label><input class="fi" id="wh-url" placeholder="https://discord.com/api/webhooks/..."/></div>
              <div class="fg"><label class="fl" id="fl-whlbl"><span data-i18n="fl-whlbl">الاسم (اختياري)</span></label><input class="fi" id="wh-label" placeholder="إشعارات البث"/></div>
            </div>
            <div class="fg">
              <label class="fl" id="fl-whev"><span data-i18n="fl-whev">الأحداث</span></label>
              <div style="display:flex;gap:.6rem;flex-wrap:wrap;margin-top:.3rem">
                <label style="display:flex;align-items:center;gap:.4rem;cursor:pointer"><input type="checkbox" id="whe-done" checked/> <span id="whe-done-lbl" data-i18n="whe-done-lbl">✅ اكتمال البث</span></label>
                <label style="display:flex;align-items:center;gap:.4rem;cursor:pointer"><input type="checkbox" id="whe-error" checked/> <span id="whe-error-lbl" data-i18n="whe-error-lbl">❌ خطأ في البث</span></label>
              </div>
            </div>
            <div style="display:flex;gap:.6rem;flex-wrap:wrap;align-items:center">
              <button class="btn bp" onclick="addWebhook()" id="btn-whadd">➕ <span data-i18n="btn-whadd-txt">إضافة</span></button>
              <button class="btn bg" onclick="testWebhook()" id="btn-whtest">🧪 <span data-i18n="btn-whtest-txt">اختبار</span></button>
              <span id="whmsg"></span>
            </div>
          </div>
        </div>
        <div class="card"><div class="ch"><span class="ct" id="ct-whlist">الـ Webhooks المضافة</span><button class="btn bg btn-sm" onclick="loadWebhooks()">🔄</button></div><div id="wh-list"><div class="em"><div class="sp"></div></div></div></div>
      </div>


      <!-- ROLES & PERMISSIONS -->
      <div class="pg" id="pg-roles">
        <div class="pt">🛡️ <span>الصلاحيات</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct">➕ منح صلاحية</span></div>
          <div class="cb">
            <div class="fr2">
              <div class="fg"><label class="fl">User ID</label><input class="fi" id="role-uid" placeholder="123456789012345678"/></div>
              <div class="fg"><label class="fl">الصلاحية</label><select class="fse" id="role-sel"><option value="viewer">👁️ Viewer — عرض فقط</option><option value="mod">🛠️ Mod — تشغيل البث</option><option value="admin" selected>👑 Admin — كامل</option></select></div>
            </div>
            <div style="display:flex;gap:.6rem;align-items:center">
              <button class="btn bp" onclick="grantRole()">➕ منح</button>
              <span id="role-msg"></span>
            </div>
          </div>
        </div>
        <div class="card"><div class="ch"><span class="ct">👥 الأعضاء والصلاحيات</span><button class="btn bg btn-sm" onclick="loadRoles()">🔄</button></div><div id="roles-list"><div class="em"><div class="sp"></div></div></div></div>
      </div>

      <!-- RECURRING BROADCASTS -->
      <div class="pg" id="pg-recurring">
        <div class="pt">🔁 <span>بث متكرر</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct">➕ جدولة بث متكرر</span></div>
          <div class="cb">
            <div class="fr2">
              <div class="fg"><label class="fl">المجموعة</label><select class="fse" id="rec-grp"></select></div>
              <div class="fg"><label class="fl">السيرفر</label><input class="fi" id="rec-guild" placeholder="Guild ID"/></div>
            </div>
            <div class="fr2">
              <div class="fg"><label class="fl">⏱️ كل كم ساعة؟</label><input class="fi" id="rec-hours" type="number" min="1" max="168" value="24" placeholder="24"/></div>
              <div class="fg"><label class="fl">السرعة</label><select class="fse" id="rec-spd"><option value="5000">🐢 بطيء</option><option value="2000" selected>🚶 متوسط</option><option value="800">🏃 سريع</option></select></div>
            </div>
            <div class="fg"><label class="fl">الرسالة</label><textarea class="fta" id="rec-msg" style="min-height:80px"></textarea></div>
            <div style="display:flex;gap:.6rem;align-items:center">
              <button class="btn bp" onclick="addRecurring()">🔁 إضافة</button>
              <span id="rec-msg-al"></span>
            </div>
          </div>
        </div>
        <div class="card"><div class="ch"><span class="ct">📋 البث المتكرر النشط</span><button class="btn bg btn-sm" onclick="loadRecurring()">🔄</button></div><div id="recurring-list"><div class="em"><div class="sp"></div></div></div></div>
      </div>

      <!-- BACKUP -->
      <div class="pg" id="pg-backup">
        <div class="pt">💾 <span>النسخ الاحتياطي</span></div>
        <div class="card" style="margin-bottom:1.2rem">
          <div class="ch"><span class="ct">💾 النسخ الاحتياطية</span></div>
          <div class="cb">
            <p style="color:var(--mu);font-size:.85rem">يتم النسخ الاحتياطي تلقائياً كل 24 ساعة. آخر 10 نسخ محفوظة.</p>
            <div style="display:flex;gap:.6rem;align-items:center;margin-bottom:1rem">
              <button class="btn bp" onclick="doBackupNow()">💾 نسخ احتياطي الآن</button>
              <span id="backup-msg"></span>
            </div>
            <div id="backup-list"><div class="em"><div class="sp"></div></div></div>
          </div>
        </div>
      </div>

      <!-- EMBED BUILDER -->
      <div class="pg" id="pg-embed">
        <div class="pt">🎨 <span>Embed Builder</span></div>
        <div class="card">
          <div class="ch"><span class="ct">🎨 بناء Embed مخصص</span></div>
          <div class="cb">
            <div class="fr2">
              <div class="fg"><label class="fl">العنوان (Title)</label><input class="fi" id="em-title" placeholder="عنوان الرسالة"/></div>
              <div class="fg"><label class="fl">اللون</label><div style="display:flex;gap:.5rem;align-items:center"><input type="color" id="em-color" value="#5865F2" style="width:48px;height:36px;border:none;border-radius:8px;cursor:pointer;background:transparent"/><input class="fi" id="em-color-hex" value="#5865F2" style="flex:1" oninput="document.getElementById('em-color').value=this.value"/></div></div>
            </div>
            <div class="fg"><label class="fl">الوصف (Description)</label><textarea class="fta" id="em-desc" style="min-height:80px" placeholder="نص الـ Embed..."></textarea></div>
            <div class="fr2">
              <div class="fg"><label class="fl">🖼️ صورة كبيرة (Image URL)</label><input class="fi" id="em-img" placeholder="https://..."/></div>
              <div class="fg"><label class="fl">🖼️ صورة صغيرة (Thumbnail URL)</label><input class="fi" id="em-thumb" placeholder="https://..."/></div>
            </div>
            <div class="fg"><label class="fl">Footer</label><input class="fi" id="em-footer" placeholder="نص أسفل الـ Embed"/></div>
            <div class="fg"><label class="fl">الرسالة النصية (فوق الـ Embed)</label><textarea class="fta" id="em-msg" style="min-height:60px" placeholder="نص عادي يُرسل مع الـ Embed..."></textarea></div>
            <div id="em-preview" style="margin:1rem 0;padding:1rem;border-radius:12px;background:var(--bg2);border-left:4px solid #5865F2">
              <div style="font-size:.75rem;color:var(--mu);margin-bottom:.5rem">👁️ معاينة (تقريبية)</div>
              <div id="em-prev-title" style="font-weight:700;font-size:.95rem;margin-bottom:.3rem;display:none"></div>
              <div id="em-prev-desc" style="font-size:.85rem;color:var(--tx2);display:none"></div>
              <div id="em-prev-footer" style="font-size:.72rem;color:var(--mu);margin-top:.5rem;display:none"></div>
            </div>
            <div style="display:flex;gap:.6rem;flex-wrap:wrap">
              <button class="btn bg" onclick="previewEmbed()">👁️ معاينة</button>
              <button class="btn bp" onclick="sendEmbedBroadcast()">📡 إرسال كبث</button>
              <span id="em-msg-al"></span>
            </div>
          </div>
        </div>
      </div>

      <!-- BOT HEALTH -->
      <div class="pg" id="pg-bothealth">
        <div class="pt">🤖 <span>صحة البوتات</span></div>
        <div class="card">
          <div class="ch"><span class="ct">🔍 حالة التوكنات</span><button class="btn bg btn-sm" onclick="refreshHealth()">🔄 فحص الآن</button></div>
          <div class="cb" id="bot-health-wrap"><div class="em"><div class="sp"></div></div></div>
        </div>
      </div>

      <!-- 2FA PAGE -->
      <div class="pg" id="pg-security">
        <div class="pt">🔐 <span>الأمان</span></div>
        <div class="card">
          <div class="ch"><span class="ct">🔐 التحقق الثنائي (2FA)</span></div>
          <div class="cb">
            <p style="color:var(--mu);font-size:.85rem;margin-bottom:1rem">عند تفعيل 2FA، سيُرسل كود عبر Discord DM عند كل تسجيل دخول.</p>
            <div id="twofa-status" style="margin-bottom:1rem"></div>
            <div id="twofa-verify-wrap" style="display:none">
              <div class="fg"><label class="fl">أدخل الكود المُرسل على Discord</label>
              <div style="display:flex;gap:.5rem"><input class="fi" id="twofa-code" placeholder="123456" maxlength="6" style="font-family:var(--mo);font-size:1.1rem;letter-spacing:.2em"/><button class="btn bp" onclick="verify2FA()">✅ تحقق</button></div></div>
              <span id="twofa-msg"></span>
            </div>
          </div>
        </div>
        <div class="card" style="margin-top:1rem">
          <div class="ch"><span class="ct">📊 جلسات تسجيل الدخول</span></div>
          <div class="cb">
            <p style="color:var(--mu);font-size:.85rem">أنت مسجل دخول كـ <strong id="sec-username"></strong> بصلاحية <span id="sec-role" class="b b-bl"></span></p>
          </div>
        </div>
      </div>

    </div>
  </div>
</div>

<!-- SHORTCUT HELP -->
<div id="shortcut-help">
  <div style="font-weight:700;margin-bottom:.5rem">⌨️ اختصارات</div>
  <div style="display:flex;flex-direction:column;gap:.3rem;color:var(--tx2)">
    <div><span class="kbd">B</span> البث</div>
    <div><span class="kbd">S</span> الجدولة</div>
    <div><span class="kbd">L</span> السجلات</div>
    <div><span class="kbd">T</span> التوكنات</div>
    <div><span class="kbd">G</span> المجموعات</div>
    <div><span class="kbd">Esc</span> إغلاق</div>
  </div>
</div>

<!-- PREVIEW MODAL -->
<div class="mo" id="prev-modal">
  <div class="md" style="max-width:520px">
    <div class="mdt">👁️ <span id="prev-title">معاينة البث</span></div>
    <div id="prev-body" style="margin-bottom:1rem"></div>
    <div class="mdf">
      <button class="btn bg" onclick="document.getElementById('prev-modal').classList.remove('open')" id="prev-cancel" data-i18n="cancel">إلغاء</button>
      <button class="btn bp" onclick="document.getElementById('prev-modal').classList.remove('open');startBroadcast(true)" id="prev-confirm">📡 <span data-i18n="prev-confirm-txt">تأكيد وإرسال</span></button>
    </div>
  </div>
</div>

<!-- CONFIRM MODAL -->
<div class="mo" id="mc"><div class="md" style="max-width:380px"><div class="mdt" id="mc-t">تأكيد</div><div id="mc-b" style="color:var(--tx2);font-size:.88rem;line-height:1.6"></div><div class="mdf"><button class="btn bg" id="mc-cancel" onclick="cModal()">إلغاء</button><button class="btn br" id="mc-ok">تأكيد</button></div></div></div>

<!-- SAVE TEMPLATE MODAL -->
<div class="mo" id="tpl-modal">
  <div class="md" style="max-width:380px">
    <div class="mdt" id="mt-title">💾 حفظ Template</div>
    <div class="fg"><label class="fl" id="mt-lbl">اسم الـ Template</label><input class="fi" id="tpl-name-in" data-i18n-ph="tpl-name-in-ph" placeholder="رسالة الترحيب"/></div>
    <div class="mdf"><button class="btn bg" onclick="cModal2()" data-i18n="cancel">إلغاء</button><button class="btn bp" onclick="saveTpl()" data-i18n="save-lbl">حفظ</button></div>
  </div>
</div>

<script>
// ══ I18N ══════════════════════════════════════════════════════════
const S={
ar:{lsub:"لوحة التحكم",lbtn:"تسجيل الدخول بـ Discord","btn-lo":"خروج",ns1:"عام",ns2:"إدارة",ns3:"بث",ns4:"أخرى","ni-overview":"الرئيسية","ni-tokens":"التوكنات","ni-groups":"المجموعات","ni-owners":"الصلاحيات","ni-blacklist":"القائمة السوداء","ni-broadcast":"بث رسالة","ni-schedule":"بث مجدول","ni-stats":"الإحصائيات","ni-logs":"السجلات","ni-bots":"إعدادات البوتات","pt-ov":"الرئيسية","pt-tok":"التوكنات","pt-grp":"المجموعات","pt-own":"الصلاحيات","pt-bl":"القائمة السوداء","pt-bc":"بث رسالة","pt-sc":"البث المجدول","pt-stats":"الإحصائيات","pt-logs":"السجلات","pt-bots":"إعدادات البوتات","sl-tok":"التوكنات","ss-tok":"بوت مسجّل","sl-grp":"المجموعات","ss-grp":"مجموعة","sl-sent":"مجموع المُرسل","ss-sent":"رسالة","sl-sch":"المجدولة","ss-sch":"بث معلّق","sl-total-sent":"إجمالي المرسل","sl-total-fail":"إجمالي الفشل","sl-total-bc":"عدد البث","sl-rate":"معدل النجاح","qa-bc":"بث رسالة","qa-bc-sub":"أرسل للأعضاء الآن","qa-sc":"جدولة بث","qa-sc-sub":"حدد وقت ويوم","qa-st":"الإحصائيات","qa-st-sub":"تحليل البث","qa-lg":"السجلات","qa-lg-sub":"تاريخ كل البث","ct-recent":"🕐 آخر النشاط","bot-st":"البوت متصل ✅","ct-tadd":"➕ إضافة توكنات","fl-tk":"التوكنات (سطر لكل توكن)","fh-tk":"كل توكن في سطر — التكرار يُتجاهل","btn-tadd":"➕ إضافة","btn-chk":"🔍 فحص الكل","ct-tlist":"قائمة التوكنات","ct-gnew":"🆕 مجموعة جديدة","fl-gn":"اسم المجموعة","fl-gc":"عدد التوكنات","btn-gc":"إنشاء","ct-glist":"المجموعات","ct-oadd":"➕ إضافة Owner","btn-oadd":"إضافة","ct-olist":"Owners","ct-bladd":"➕ إضافة للقائمة السوداء","fl-blid":"User ID","fl-blnote":"ملاحظة (اختياري)","btn-bladd":"🚫 إضافة","ct-bllist":"المحظورون","ct-bset":"⚙️ إعدادات البث","tab-all-txt":"📢 كل الأعضاء","tab-role-txt":"🏷️ حسب الرتبة","fl-bcg":"المجموعة","fl-bcgid":"السيرفر","btn-lguild":"تحميل السيرفرات","fl-bcr":"الرتبة","btn-rl":"تحميل","fl-bcm":"نوع البث","bcm-all":"📢 كل الأعضاء","bcm-obc":"🟢 الأونلاين فقط","fl-bcs":"السرعة","bcs-slow":"🐢 بطيء (5s)","bcs-norm":"🚶 متوسط (2s)","bcs-fast":"🏃 سريع (0.8s)","fl-bcmsg":"الرسالة","fh-bc":"سيُضاف منشن المستخدم تلقائياً","btn-bc-txt":"بدء البث","btn-stop-txt":"إيقاف","ct-bstat":"📊 مباشر","bc-idle-txt":"لم يبدأ البث بعد","bc-ptxt":"الإجمالي","bc-btxt":"البوتات","ct-scnew":"➕ جدولة جديدة","fl-scg":"المجموعة","fl-scgid":"السيرفر","btn-scguild":"تحميل","fl-scdt":"📅 التاريخ والوقت","fh-scdt":"توقيت جهازك","fl-scm":"نوع البث","scm-all":"📢 كل الأعضاء","scm-obc":"🟢 الأونلاين فقط","fl-scs":"السرعة","scs-slow":"🐢 بطيء (5s)","scs-norm":"🚶 متوسط (2s)","scs-fast":"🏃 سريع (0.8s)","fl-scr":"Role ID","fl-scmsg":"الرسالة","btn-scadd":"⏰ جدولة","ct-sclist":"البث المجدول","ct-chart":"📈 البث - آخر 14 يوم","ct-logs":"سجل العمليات","btn-lclr":"🗑️ مسح","ct-av":"🖼️ تغيير الأفاتار","fl-av":"رابط الصورة","btn-av":"تطبيق على الكل","ct-nm":"✏️ تغيير الأسماء","fl-nm":"الاسم الجديد (2–32)","btn-nm":"تطبيق على الكل","ct-tpl":"💾 Templates الرسائل","mt-title":"💾 حفظ Template","mt-lbl":"اسم الـ Template","mc-cancel":"إلغاء","mc-ok":"تأكيد",
noAccess:"ليس لديك صلاحية. اطلب إضافتك عبر",selGrp:"— اختر مجموعة —",selRole:"— اختر رتبة —",selGuild:"— اختر سيرفر —",loadGuildsFirst:"اضغط تحميل أولاً",tok:"توكن",bots:"بوت",members:"أعضاء",valid:"صالح",invalid:"غير صالح",pending:"معلّق",done:"اكتمل",missed:"فاته الوقت",allM:"كل الأعضاء",onlineOnly:"أونلاين فقط",byRole:"حسب الرتبة",sent:"أُرسل:",failed:"فشل:",total:"مجموع:",noLogs:"لا توجد سجلات",noScheds:"لا توجد مجدولات",noTokens:"لا توجد توكنات",noGroups:"لا توجد مجموعات",noBL:"القائمة السوداء فارغة",noTpl:"لا توجد templates",noActivity:"لا يوجد نشاط بعد",errGrp:"❌ اختر مجموعة",errGid:"❌ اختر سيرفر",errMsg:"❌ اكتب الرسالة",errDt:"❌ حدد التاريخ والوقت",errId:"❌ ID غير صحيح",applying:"جاري التطبيق…",bcComplete:"✅ اكتمل البث",stopped:"⛔ تم الإيقاف",connErr:"❌ انقطع الاتصال",remove:"إزالة",cancel:"إلغاء",confirm:"تأكيد",sure:"هل أنت متأكد؟",
"bl-note-ph":"طلب إيقاف البث","tpl-name-in-ph":"رسالة الترحيب","optional-ph":"(اختياري)","save-lbl":"حفظ","log-search-ph":"🔍 بحث في السجلات…",
"bot-lbl":"بوت","th-hash":"#","th-bot":"البوت","th-valid":"الحالة","th-groups":"المجموعات","th-name":"الاسم","th-id":"المعرف","th-count":"العدد","th-note":"الملاحظة","th-date":"التاريخ","th-msg":"الرسالة","th-actions":"",
"chk-found":"🔍 وجدنا","chk-bad":"توكن منتهية. حذفها تلقائياً؟","chk-all-ok":"✅ كل التوكنات صالحة","chk-deleted":"✅ تم حذف","chk-expired":"توكن منتهية",
"remove-btn":"إزالة","unblock-btn":"✅ رفع الحظر","tpl-saved":"تم حفظ الـ Template",
"ni-retry":"إعادة الإرسال","ni-webhooks":"الـ Webhooks","pt-retry":"إعادة الإرسال الفاشل","pt-wh":"الـ Webhooks",
"ct-retry":"جلسات البث الفاشلة","ct-whadd":"➕ إضافة Webhook","fl-whurl":"Discord Webhook URL",
"fl-whlbl":"الاسم (اختياري)","fl-whev":"الأحداث","whe-done-lbl":"✅ اكتمال البث","whe-error-lbl":"❌ خطأ في البث",
"btn-whadd":"➕ إضافة","btn-whtest":"🧪 اختبار","ct-whlist":"الـ Webhooks المضافة",
"fl-blbulk":"📋 استيراد بالجملة (ID في كل سطر)","btn-blbulk":"📥 استيراد",
"ct-grphist":"📊 إحصائيات المجموعات","btn-prev-txt":"معاينة","btn-test-txt":"اختبار",
"prev-title":"معاينة البث","prev-cancel":"إلغاء","prev-confirm":"📡 تأكيد وإرسال",
"noRetry":"لا توجد جلسات فاشلة","noWebhooks":"لا توجد webhooks","retryNow":"♻️ إعادة الإرسال",
"guildsSelected":"سيرفر محدد","selGuildsFirst":"اختر سيرفر واحد على الأقل",
"btn-selall":"☑️ الكل","previewLoading":"جاري التحميل…","previewErr":"فشل تحميل البيانات","btn-selall":"☑️ الكل","btn-lguild-txt":"تحميل","fl-blbulk":"📋 استيراد بالجملة (ID في كل سطر)","btn-blbulk":"استيراد","prev-confirm-txt":"تأكيد وإرسال","fl-whlbl":"الاسم (اختياري)","fl-whev":"الأحداث","whe-done-lbl":"✅ اكتمال البث","whe-error-lbl":"❌ خطأ في البث","btn-whadd-txt":"إضافة","btn-whtest-txt":"اختبار"},
en:{lsub:"Dashboard",lbtn:"Login with Discord","btn-lo":"Logout",ns1:"GENERAL",ns2:"MANAGE",ns3:"BROADCAST",ns4:"OTHER","ni-overview":"Overview","ni-tokens":"Tokens","ni-groups":"Groups","ni-owners":"Owners","ni-blacklist":"Blacklist","ni-broadcast":"Broadcast","ni-schedule":"Scheduled","ni-stats":"Statistics","ni-logs":"Logs","ni-bots":"Bot Settings","pt-ov":"Overview","pt-tok":"Tokens","pt-grp":"Groups","pt-own":"Owners","pt-bl":"Blacklist","pt-bc":"Broadcast","pt-sc":"Scheduled Broadcast","pt-stats":"Statistics","pt-logs":"Logs","pt-bots":"Bot Settings","sl-tok":"Tokens","ss-tok":"registered bots","sl-grp":"Groups","ss-grp":"groups","sl-sent":"Total Sent","ss-sent":"messages","sl-sch":"Scheduled","ss-sch":"pending","sl-total-sent":"Total Sent","sl-total-fail":"Total Failed","sl-total-bc":"Broadcasts","sl-rate":"Success Rate","qa-bc":"Broadcast","qa-bc-sub":"Send to members now","qa-sc":"Schedule","qa-sc-sub":"Set date and time","qa-st":"Statistics","qa-st-sub":"Broadcast analytics","qa-lg":"Logs","qa-lg-sub":"Broadcast history","ct-recent":"🕐 Recent Activity","bot-st":"Bot Online ✅","ct-tadd":"➕ Add Tokens","fl-tk":"Tokens (one per line)","fh-tk":"One token per line — duplicates ignored","btn-tadd":"➕ Add","btn-chk":"🔍 Check All","ct-tlist":"Token List","ct-gnew":"🆕 New Group","fl-gn":"Group Name","fl-gc":"Token Count","btn-gc":"Create","ct-glist":"Groups","ct-oadd":"➕ Add Owner","btn-oadd":"Add","ct-olist":"Owners","ct-bladd":"➕ Add to Blacklist","fl-blid":"User ID","fl-blnote":"Note (optional)","btn-bladd":"🚫 Add","ct-bllist":"Blacklisted Users","ct-bset":"⚙️ Broadcast Settings","tab-all-txt":"📢 All Members","tab-role-txt":"🏷️ By Role","fl-bcg":"Group","fl-bcgid":"Server","btn-lguild":"Load Servers","fl-bcr":"Role","btn-rl":"Load","fl-bcm":"Broadcast Mode","bcm-all":"📢 All Members","bcm-obc":"🟢 Online Only","fl-bcs":"Speed","bcs-slow":"🐢 Slow (5s)","bcs-norm":"🚶 Normal (2s)","bcs-fast":"🏃 Fast (0.8s)","fl-bcmsg":"Message","fh-bc":"User mention appended automatically","btn-bc-txt":"Start Broadcast","btn-stop-txt":"Stop","ct-bstat":"📊 Live Stats","bc-idle-txt":"No broadcast started","bc-ptxt":"Total","bc-btxt":"Bots","ct-scnew":"➕ New Scheduled Broadcast","fl-scg":"Group","fl-scgid":"Server","btn-scguild":"Load","fl-scdt":"📅 Date & Time","fh-scdt":"Your local timezone","fl-scm":"Broadcast Mode","scm-all":"📢 All Members","scm-obc":"🟢 Online Only","fl-scs":"Speed","scs-slow":"🐢 Slow (5s)","scs-norm":"🚶 Normal (2s)","scs-fast":"🏃 Fast (0.8s)","fl-scr":"Role ID","fl-scmsg":"Message","btn-scadd":"⏰ Schedule","ct-sclist":"Scheduled Broadcasts","ct-chart":"📈 Broadcasts — Last 14 Days","ct-logs":"Activity Log","btn-lclr":"🗑️ Clear All","ct-av":"🖼️ Change Avatar","fl-av":"Image URL","btn-av":"Apply to All","ct-nm":"✏️ Change Names","fl-nm":"New Name (2–32 chars)","btn-nm":"Apply to All","ct-tpl":"💾 Message Templates","mt-title":"💾 Save Template","mt-lbl":"Template Name","mc-cancel":"Cancel","mc-ok":"Confirm",
noAccess:"No access. Ask an owner to add you via",selGrp:"— Select Group —",selRole:"— Select Role —",selGuild:"— Select Server —",loadGuildsFirst:"Click Load first",tok:"token",bots:"bots",members:"members",valid:"Valid",invalid:"Invalid",pending:"Pending",done:"Done",missed:"Missed",allM:"All Members",onlineOnly:"Online Only",byRole:"By Role",sent:"Sent:",failed:"Failed:",total:"Total:",noLogs:"No logs yet",noScheds:"No scheduled broadcasts",noTokens:"No tokens yet",noGroups:"No groups yet",noBL:"Blacklist is empty",noTpl:"No templates yet",noActivity:"No activity yet",errGrp:"❌ Select a group",errGid:"❌ Select a server",errMsg:"❌ Enter a message",errDt:"❌ Set date and time",errId:"❌ Invalid ID",applying:"Applying…",bcComplete:"✅ Broadcast complete",stopped:"⛔ Stopped",connErr:"❌ Connection error",remove:"Remove",cancel:"Cancel",confirm:"Confirm",sure:"Are you sure? This cannot be undone.",
"bl-note-ph":"e.g. Requested opt-out","tpl-name-in-ph":"e.g. Welcome message","optional-ph":"(optional)","save-lbl":"Save","log-search-ph":"🔍 Search logs…",
"bot-lbl":"Bot","th-hash":"#","th-bot":"Bot","th-valid":"Status","th-groups":"Groups","th-name":"Name","th-id":"ID","th-count":"Count","th-note":"Note","th-date":"Date","th-msg":"Message","th-actions":"",
"chk-found":"🔍 Found","chk-bad":"expired tokens. Remove them automatically?","chk-all-ok":"✅ All tokens are valid","chk-deleted":"✅ Removed","chk-expired":"expired tokens",
"remove-btn":"Remove","unblock-btn":"✅ Unblock","tpl-saved":"Template saved",
"ni-retry":"Retry Failed","ni-webhooks":"Webhooks","pt-retry":"Retry Failed Broadcasts","pt-wh":"Webhooks",
"ct-retry":"Failed Broadcast Sessions","ct-whadd":"➕ Add Webhook","fl-whurl":"Discord Webhook URL",
"fl-whlbl":"Label (optional)","fl-whev":"Events","whe-done-lbl":"✅ Broadcast Complete","whe-error-lbl":"❌ Broadcast Error",
"btn-whadd":"➕ Add","btn-whtest":"🧪 Test","ct-whlist":"Registered Webhooks",
"fl-blbulk":"📋 Bulk Import (one ID per line)","btn-blbulk":"📥 Import",
"ct-grphist":"📊 Group Statistics","btn-prev-txt":"Preview","btn-test-txt":"Test",
"prev-title":"Broadcast Preview","prev-cancel":"Cancel","prev-confirm":"📡 Confirm & Send",
"noRetry":"No failed sessions","noWebhooks":"No webhooks added","retryNow":"♻️ Retry",
"guildsSelected":"server(s) selected","selGuildsFirst":"Select at least one server",
"btn-selall":"☑️ All","previewLoading":"Loading…","previewErr":"Failed to load preview","btn-selall":"☑️ All","btn-lguild-txt":"Load","fl-blbulk":"📋 Bulk Import (one ID per line)","btn-blbulk":"Import","prev-confirm-txt":"Confirm & Send","fl-whlbl":"Label (optional)","fl-whev":"Events","whe-done-lbl":"✅ Broadcast Complete","whe-error-lbl":"❌ Broadcast Error","btn-whadd-txt":"Add","btn-whtest-txt":"Test"}
};
let lang=localStorage.getItem('lang')||'ar',me=null,ws=null,wsOk=false,bcTabMode='all';
let allLogs=[];
let bcChart=null;

function t(k){return(S[lang]&&S[lang][k])||(S.ar[k])||k}

function setLang(l){
  lang=l;
  localStorage.setItem('lang',l);
  const root=document.getElementById('htmlroot');
  root.lang=l;
  root.dir=l==='ar'?'rtl':'ltr';
  document.getElementById('lb-ar').classList.toggle('on',l==='ar');
  document.getElementById('lb-en').classList.toggle('on',l==='en');

  // 1. All elements with an id that maps to a translation key
  document.querySelectorAll('[id]').forEach(el=>{
    const s=t(el.id);
    if(s===el.id) return; // no translation found
    const tag=el.tagName;
    if(tag==='INPUT'||tag==='TEXTAREA'){
      // translate placeholder if key exists
      const ph=S[l]&&S[l][el.id+'-ph'];
      if(ph) el.placeholder=ph;
    } else if(tag==='SELECT') {
      // handled separately via option ids
    } else if(tag==='IMG'||tag==='CANVAS') {
      // skip
    } else if(!el.children.length){
      el.textContent=s;
    } else {
      // element has children — only update if it has a direct text node (span pattern)
      if(el.firstChild&&el.firstChild.nodeType===3) el.firstChild.textContent=s;
    }
  });

  // 2. Placeholders via data-i18n-ph attribute
  document.querySelectorAll('[data-i18n-ph]').forEach(el=>{
    const k=el.getAttribute('data-i18n-ph');
    const s=t(k);
    if(s&&s!==k) el.placeholder=s;
  });

  // 3. Any element with data-i18n attribute (text content) — covers nested spans too
  document.querySelectorAll('[data-i18n]').forEach(el=>{
    const k=el.getAttribute('data-i18n');
    const s=t(k);
    if(s&&s!==k) el.textContent=s;
  });

  // 3b. data-i18n on button or element directly sets its own textContent
  // (already covered above, but also handle inputs with data-i18n as value)

  // 4. Select option textContent — they all have ids
  document.querySelectorAll('select option[id]').forEach(opt=>{
    const s=t(opt.id);
    if(s!==opt.id) opt.textContent=s;
  });

  // 5. Confirm modal buttons (rebuilt each open)
  const mok=document.getElementById('mc-ok');
  const mca=document.getElementById('mc-cancel');
  if(mok) mok.textContent=t('confirm');
  if(mca) mca.textContent=t('cancel');

  // 6. Login page subtitle & button
  const lsub=document.getElementById('lsub');
  if(lsub) lsub.textContent=t('lsub');
  const lbtn=document.getElementById('lbtn');
  if(lbtn) lbtn.textContent=t('lbtn');

  // 7. Logout button
  const blo=document.getElementById('btn-lo');
  if(blo) blo.textContent=t('btn-lo');

  // 8. Search placeholder
  const ls=document.getElementById('log-search');
  if(ls) ls.placeholder=t('log-search-ph');

  // 9. Broadcast idle text
  const bit=document.getElementById('bc-idle-txt');
  if(bit) bit.textContent=t('bc-idle-txt');

  // 10. Dynamic inline strings with data-i18n-inline (optional spans)
  document.querySelectorAll('[data-i18n-inline]').forEach(el=>{
    const k=el.getAttribute('data-i18n-inline');
    el.textContent=t(k);
  });

  // 11. Bot status text
  const bst=document.getElementById('bot-st');
  if(bst) bst.textContent=t('bot-st');

  // 12. Refresh buttons (🔄) — no text needed, emojis are universal
  // 13. Page titles are all <span id="pt-*"> covered by rule 1

  // Done — re-render active page to update any JS-generated content
  const activePg=document.querySelector('.pg.on');
  if(activePg){
    const name=activePg.id.replace('pg-','');
    // Reload data-driven content in the current page
    if(name==='overview')   loadOverview();
    if(name==='tokens')     loadTokens();
    if(name==='groups')     {loadGroups();loadGroupHistory();}
    if(name==='owners')     loadOwners();
    if(name==='blacklist')  loadBlacklist();
    if(name==='broadcast')  {loadBcGroups();loadTemplateChips();}
    if(name==='schedule')   {loadSchedGroups();loadSchedules();}
    if(name==='stats')      loadStats();
    if(name==='logs')       loadLogs();
    if(name==='bots')       loadTemplates();
    if(name==='retry')      loadRetry();
    if(name==='webhooks')   loadWebhooks();
  }
}

function toggleTheme(){document.body.classList.toggle('light');const btn=document.querySelector('.theme-btn');btn.textContent=document.body.classList.contains('light')?'🌙':'☀️';localStorage.setItem('theme',document.body.classList.contains('light')?'light':'dark');}

async function boot(){
  const savedTheme=localStorage.getItem('colorTheme')||'';if(savedTheme)setTheme(savedTheme);
  if(localStorage.getItem('theme')==='light'){document.body.classList.add('light');document.querySelector('.theme-btn').textContent='🌙';}
  setLang(lang);

  // Show login page immediately (hidden until auth confirmed)
  const lp=document.getElementById('lp');
  const app=document.getElementById('app');

  // Handle error params from OAuth redirect
  const p=new URLSearchParams(location.search);
  if(p.has('error')){
    const e=document.getElementById('lerr');
    const errMap={oauth_failed:'فشل تسجيل الدخول — تحقق من DISCORD_CLIENT_SECRET و REDIRECT_URI',no_code:'لم يُرسل كود من Discord'};
    e.textContent=errMap[p.get('error')]||p.get('error');
    e.style.display='block';
    // Clean URL
    history.replaceState({},'',location.pathname);
  }

  let r=null;
  try{ r=await fetch('/auth/me').then(x=>x.json()); }catch(err){ console.error('auth/me failed:',err); }

  // Not logged in → show login page
  if(!r||!r.user){
    lp.style.display='';
    app.classList.remove('show');
    return;
  }

  // Logged in but no access
  if(!r.hasAccess){
    const e=document.getElementById('lerr');
    e.innerHTML=t('noAccess')+' <code style="background:var(--sf2);padding:.1rem .4rem;border-radius:4px;font-size:.82rem">$add-owner '+r.user.id+'</code>';
    e.style.display='block';
    lp.style.display='';
    app.classList.remove('show');
    return;
  }

  // ✅ Authenticated — show dashboard
  me=r.user;
  lp.style.display='none';
  app.classList.add('show');
  document.getElementById('tb-av').src=me.avatar;
  document.getElementById('tb-un').textContent=me.username;
  document.getElementById('tb-ur').textContent=r.role;
  document.querySelectorAll('.ni').forEach(el=>el.addEventListener('click',()=>gp(el.dataset.p)));
  document.querySelectorAll('.mo').forEach(el=>el.addEventListener('click',e=>{if(e.target===el)el.classList.remove('open');}));
  const dt=new Date(Date.now()+3600000);dt.setSeconds(0,0);
  const scdt=document.getElementById('sc-dt');if(scdt)scdt.value=dt.toISOString().slice(0,16);
  gp('overview');
  setTimeout(async()=>{try{const d=await api('GET','/api/overview');if(d.tokenCount>0)document.getElementById('bot-status-bar').style.display='flex';}catch{}},600);
}

function gp(name){
  document.querySelectorAll('.ni').forEach(e=>e.classList.toggle('on',e.dataset.p===name));
  document.querySelectorAll('.pg').forEach(e=>e.classList.toggle('on',e.id==='pg-'+name));
  if(name==='overview')  loadOverview();
  if(name==='tokens')    loadTokens();
  if(name==='groups')    {loadGroups();loadGroupHistory();}
  if(name==='owners')    loadOwners();
  if(name==='blacklist') loadBlacklist();
  if(name==='broadcast') {loadBcGroups();loadTemplateChips();}
  if(name==='schedule')  {loadSchedGroups();loadSchedules();}
  if(name==='stats')     loadStats();
  if(name==='logs')      loadLogs();
  if(name==='bots')      loadTemplates();
  if(name==='retry')     loadRetry();
  if(name==='webhooks')  loadWebhooks();
  if(name==='audit')     loadAuditPage();
  if(name==='custbc')    loadCustGroups();
  if(name==='roles')     loadRolesPage();
  if(name==='recurring') { loadRecurring(); loadRecGrps(); }
  if(name==='backup')    loadBackups();
  if(name==='embed')     loadEmbedGroups();
  if(name==='security')  loadSecurity();
  if(name==='bothealth')  loadBotHealth();
}

async function api(method,url,body){const o={method,headers:{'Content-Type':'application/json'}};if(body)o.body=JSON.stringify(body);const r=await fetch(url,o);const j=await r.json();if(!r.ok)throw new Error(j.error||r.statusText);return j;}
function bmsg(id,cls,txt,dur=4000){const e=document.getElementById(id);if(!e)return;e.innerHTML=\`<span class="b \${cls}" style="margin:.3rem .4rem">\${txt}</span>\`;if(dur)setTimeout(()=>{if(e)e.innerHTML=''},dur);}
function bal(id,cls,html){const e=document.getElementById(id);if(e)e.innerHTML=\`<div class="al al-\${cls}">\${html}</div>\`;}
function confirm2(title,body,cb){document.getElementById('mc-t').textContent=title;document.getElementById('mc-b').textContent=body;document.getElementById('mc-ok').textContent=t('confirm');document.getElementById('mc-cancel').textContent=t('cancel');document.getElementById('mc-ok').onclick=()=>{cModal();cb();};document.getElementById('mc').classList.add('open');}
function cModal(){document.getElementById('mc').classList.remove('open');}
function cModal2(){document.getElementById('tpl-modal').classList.remove('open');}
function fmtDt(iso){return new Date(iso).toLocaleString(lang==='ar'?'ar-SA':'en-US',{dateStyle:'short',timeStyle:'short'});}
function ml(m){return m==='obc'?t('onlineOnly'):m==='rbc'?t('byRole'):t('allM');}
function countdown(iso){const diff=new Date(iso)-Date.now();if(diff<=0)return'';const h=Math.floor(diff/3600000),m=Math.floor((diff%3600000)/60000),s=Math.floor((diff%60000)/1000);return h>24?\`\${Math.floor(h/24)}d \${h%24}h\`:h>0?\`\${h}h \${m}m\`:\`\${m}m \${s}s\`;}

// ── OVERVIEW ─────────────────────────────────────────────────────────
async function loadOverview(){
  try{
    const [d,sc,logs]=await Promise.all([api('GET','/api/overview'),api('GET','/api/schedules'),api('GET','/api/logs')]);
    document.getElementById('ov-tok').textContent=d.tokenCount;document.getElementById('ov-grp').textContent=d.groupCount;
    const pending=sc.filter(s=>s.status==='pending').length;document.getElementById('ov-sch').textContent=pending;
    const totalSent=logs.reduce((a,l)=>a+(l.sent||0),0);document.getElementById('ov-sent').textContent=totalSent.toLocaleString();
    const badge=document.getElementById('sched-badge');if(pending>0){badge.style.display='';badge.textContent=pending;}else badge.style.display='none';
    const rl=document.getElementById('recent-list');
    if(!logs.length){rl.innerHTML=\`<div class="em"><div class="ei">📭</div><div class="et">\${t('noActivity')}</div></div>\`;return;}
    rl.innerHTML=logs.slice(0,5).map(l=>\`<div class="lr"><div class="li">\${l.type==='scheduled'?'⏰':'📢'}</div><div class="linf"><div style="font-weight:700;font-size:.84rem">\${l.groupName||'?'} <span style="color:var(--mu);font-weight:400">→ \${ml(l.mode)}</span></div><div style="margin-top:.2rem"><span class="b \${l.status==='done'?'b-gr':'b-rd'}">\${t('sent')} \${l.sent||0}  \${t('failed')} \${l.failed||0}</span></div></div><div class="lt">\${fmtDt(l.timestamp)}</div></div>\`).join('');
  }catch{}
}

// ── TOKENS ─────────────────────────────────────────────────────────
async function loadTokens(){
  const w=document.getElementById('tok-wrap');w.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const toks=await api('GET','/api/tokens');
    if(!toks.length){w.innerHTML=\`<div class="em"><div class="ei">🔑</div><div class="et">\${t('noTokens')}</div></div>\`;return;}
    w.innerHTML=\`<table><thead><tr><th>\${t('th-hash')}</th><th>\${t('th-bot')}</th><th>\${t('th-valid')}</th><th>\${t('th-groups')}</th><th></th></tr></thead><tbody>\${toks.map(k=>\`<tr>
      <td style="font-family:var(--mo);color:var(--mu);font-size:.78rem">\${k.index+1}</td>
      <td><div class="brow">\${k.avatar?\`<img class="bav" src="\${k.avatar}">\`:\`<div class="bavp">\${k.valid&&k.name?k.name[0].toUpperCase():'?'}</div>\`}<div><div style="font-weight:700;font-size:.86rem">\${k.valid?k.name:t('invalid')}</div><div style="font-size:.72rem;color:var(--mu);font-family:var(--mo)">\${k.snippet}</div></div></div></td>
      <td>\${k.valid?\`<span class="b b-gr">✅ \${t('valid')}</span>\`:\`<span class="b b-rd">❌ \${t('invalid')}</span>\`}</td>
      <td>\${k.groups.length?k.groups.map(g=>\`<span class="b b-bl" style="margin:.1rem">\${g}</span>\`).join(''):'<span class="b b-mu">—</span>'}</td>
      <td><button class="btn br btn-sm" onclick="delToken(\${k.index})">🗑️</button></td>
    </tr>\`).join('')}</tbody></table>\`;
  }catch(e){w.innerHTML=\`<div class="em"><div class="ei">⚠️</div><div class="et">\${e.message}</div></div>\`;}
}
async function addTokens(){const raw=document.getElementById('tok-in').value.trim();if(!raw)return;const tokens=raw.split('\\n').map(x=>x.trim()).filter(Boolean);try{const r=await api('POST','/api/tokens',{tokens});bmsg('tmsg','b-gr',\`✅ +\${r.added}\`);document.getElementById('tok-in').value='';loadTokens();loadOverview();}catch(e){bmsg('tmsg','b-rd','❌ '+e.message);}}
function delToken(i){confirm2(t('ct-tlist'),t('sure'),async()=>{try{await api('DELETE',\`/api/tokens/\${i}\`);loadTokens();loadOverview();}catch(e){alert(e.message);}});}
async function checkTokens(){
  const btn=document.getElementById('btn-chk');btn.disabled=true;btn.innerHTML='<span class="sp"></span>';
  bmsg('tmsg','b-yl',\`🔍 \${t('applying')}…\`);
  try{
    const r=await api('POST','/api/tokens/check');
    const bad=r.filter(x=>!x.valid).length;
    if(bad>0){
      confirm2(t('ct-tlist'),\`\${t('chk-found')} \${bad} \${t('chk-expired')}\`,async()=>{await api('POST','/api/tokens/check?cleanup=true');loadTokens();loadOverview();bmsg('tmsg','b-gr',\`\${t('chk-deleted')} \${bad} \${t('chk-expired')}\`);});
    } else {bmsg('tmsg','b-gr',\`\${t('chk-all-ok')} (\${r.length})\`)}
  }catch(e){bmsg('tmsg','b-rd','❌ '+e.message);}
  btn.disabled=false;btn.innerHTML=t('btn-chk');
}

// ── GROUPS ─────────────────────────────────────────────────────────
async function loadGroups(){
  const w=document.getElementById('grp-wrap');w.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{const gs=await api('GET','/api/groups');if(!gs.length){w.innerHTML=\`<div class="em"><div class="ei">📦</div><div class="et">\${t('noGroups')}</div></div>\`;return;}w.innerHTML=\`<table><thead><tr><th>\${t('fl-gn')}</th><th>ID</th><th>\${t('fl-gc')}</th><th></th></tr></thead><tbody>\${gs.map(g=>\`<tr><td style="font-weight:700">\${g.name}</td><td><span class="b b-mu" style="font-family:var(--mo)">\${g.id}</span></td><td><span class="b b-bl">\${g.tokenCount} \${t('tok')}</span></td><td><button class="btn br btn-sm" onclick="delGroup('\${g.id}','\${g.name}')">🗑️</button></td></tr>\`).join('')}</tbody></table>\`;}catch(e){w.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
async function createGroup(){const name=document.getElementById('gn').value.trim(),count=parseInt(document.getElementById('gc').value);if(!name||!count)return bmsg('gmsg','b-rd','❌');try{const g=await api('POST','/api/groups',{name,tokenCount:count});bmsg('gmsg','b-gr',\`✅ "\${g.name}"\`);document.getElementById('gn').value='';document.getElementById('gc').value='';loadGroups();loadOverview();}catch(e){bmsg('gmsg','b-rd','❌ '+e.message);}}
function delGroup(id,name){confirm2(t('ct-glist'),\`"\${name}"?\`,async()=>{try{await api('DELETE',\`/api/groups/\${id}\`);loadGroups();loadOverview();}catch(e){alert(e.message);}});}

// ── OWNERS ─────────────────────────────────────────────────────────
async function loadOwners(){
  const list=document.getElementById('own-list');list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{const {botOwner,owners}=await api('GET','/api/owners');let html='';if(botOwner)html+=owRow(botOwner,true);html+=owners.map(o=>owRow(o,false)).join('');if(!html)html=\`<div class="em"><div class="ei">👑</div></div>\`;list.innerHTML=html;}catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
function owRow(u,isBo){return \`<div class="or"><img src="\${u.avatar||'https://cdn.discordapp.com/embed/avatars/0.png'}" alt=""><div style="flex:1"><div class="orn">\${isBo?'👑 ':'🛡️ '}\${u.username}</div><div class="ori">\${u.id}</div></div>\${isBo?\`<span class="b b-yl">Bot Owner</span>\`:\`<button class="btn br btn-sm" onclick="delOwner('\${u.id}','\${u.username}')">\${t('remove')}</button>\`}</div>\`;}
async function addOwner(){const id=document.getElementById('oin').value.trim();if(!/^\\d{17,20}$/.test(id))return bmsg('omsg','b-rd',t('errId'));try{await api('POST','/api/owners',{userId:id});bmsg('omsg','b-gr','✅');document.getElementById('oin').value='';loadOwners();}catch(e){bmsg('omsg','b-rd','❌ '+e.message);}}
function delOwner(id,name){confirm2(t('ct-olist'),\`"\${name}"?\`,async()=>{try{await api('DELETE',\`/api/owners/\${id}\`);loadOwners();}catch(e){alert(e.message);}});}

// ── BLACKLIST ─────────────────────────────────────────────────────
async function loadBlacklist(){
  const w=document.getElementById('bl-wrap');w.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const bl=await api('GET','/api/blacklist');
    if(!bl.length){w.innerHTML=\`<div class="em"><div class="ei">🚫</div><div class="et">\${t('noBL')}</div></div>\`;return;}
    w.innerHTML=\`<table><thead><tr><th>\${t('fl-blid')}</th><th>\${t('fl-blnote')}</th><th>\${t('fl-scdt')}</th><th></th></tr></thead><tbody>\${bl.map(x=>\`<tr><td><span class="b b-mu" style="font-family:var(--mo)">\${x.userId}</span></td><td style="color:var(--mu);font-size:.82rem">\${x.note||'—'}</td><td style="color:var(--mu);font-size:.76rem">\${fmtDt(x.addedAt)}</td><td><button class="btn bs btn-sm" onclick="delBL('\${x.userId}')">\${t('unblock-btn')}</button></td></tr>\`).join('')}</tbody></table>\`;
  }catch(e){w.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
async function addBlacklist(){
  const userId=document.getElementById('bl-id').value.trim(),note=document.getElementById('bl-note').value.trim();
  if(!/^\\d{17,20}$/.test(userId))return bmsg('blmsg','b-rd',t('errId'));
  try{await api('POST','/api/blacklist',{userId,note});bmsg('blmsg','b-gr','✅');document.getElementById('bl-id').value='';document.getElementById('bl-note').value='';loadBlacklist();}
  catch(e){bmsg('blmsg','b-rd','❌ '+e.message);}
}
function delBL(userId){confirm2(t('ct-bllist'),t('sure'),async()=>{try{await api('DELETE',\`/api/blacklist/\${userId}\`);loadBlacklist();}catch(e){alert(e.message);}});}

// ── GUILDS PICKER ──────────────────────────────────────────────────
let guildsCache=null;
// ── Multi-guild selector (broadcast) ──────────────────────────────
async function loadGuilds(prefix){
  if(prefix==='bc'){
    const wrap=document.getElementById('bc-guilds-wrap');
    if(!wrap)return;
    wrap.innerHTML=\`<div class="em" style="padding:.5rem"><div class="sp"></div></div>\`;
    try{
      if(!guildsCache) guildsCache=await api('GET','/api/guilds');
      renderGuildCheckboxes(guildsCache);
    }catch(e){wrap.innerHTML=\`<div style="color:var(--rd);font-size:.8rem">❌ \${e.message}</div>\`;}
  } else {
    // Schedule page: keep single select
    const sel=document.getElementById(\`\${prefix}-guild-sel\`);
    if(!sel)return;
    sel.innerHTML=\`<option>\${t('applying')}</option>\`;
    try{
      if(!guildsCache) guildsCache=await api('GET','/api/guilds');
      sel.innerHTML=\`<option value="">\${t('selGuild')}</option>\`;
      guildsCache.forEach(g=>{const o=document.createElement('option');o.value=g.id;o.dataset.name=g.name;o.dataset.icon=g.icon||'';o.textContent=\`\${g.name} (\${(g.memberCount||0).toLocaleString()})\`;sel.appendChild(o);});
    }catch(e){sel.innerHTML=\`<option value="">❌ \${e.message}</option>\`;}
  }
}
function renderGuildCheckboxes(guilds){
  const wrap=document.getElementById('bc-guilds-wrap');if(!wrap)return;
  if(!guilds.length){wrap.innerHTML=\`<div style="color:var(--mu);font-size:.8rem">No servers found</div>\`;return;}
  wrap.innerHTML=guilds.map(g=>\`<label style="display:flex;align-items:center;gap:.5rem;padding:.3rem .5rem;border-radius:7px;cursor:pointer;border:1px solid var(--bd);background:var(--sf2)">
    <input type="checkbox" class="bc-guild-cb" value="\${g.id}" data-name="\${g.name}" data-count="\${g.memberCount||0}" style="accent-color:var(--bl)"/>
    \${g.icon?\`<img src="\${g.icon}" style="width:20px;height:20px;border-radius:50%;object-fit:cover"/>\`:\`\`}
    <span style="font-size:.82rem;font-weight:600">\${g.name}</span>
    <span style="font-size:.72rem;color:var(--mu);margin-inline-start:auto">\${(g.memberCount||0).toLocaleString()}</span>
  </label>\`).join('');
  wrap.querySelectorAll('.bc-guild-cb').forEach(cb=>cb.addEventListener('change',updateGuildCount));
}
function updateGuildCount(){
  const n=[...document.querySelectorAll('.bc-guild-cb:checked')].length;
  const cnt=document.getElementById('bc-guild-count');
  if(cnt)cnt.textContent=n?\`\${n} \${t('guildsSelected')}\`:'';
}
function selAllGuilds(){
  const cbs=document.querySelectorAll('.bc-guild-cb');
  const all=[...cbs].every(c=>c.checked);
  cbs.forEach(c=>c.checked=!all);
  updateGuildCount();
}
function getSelectedGuildIds(){return [...document.querySelectorAll('.bc-guild-cb:checked')].map(c=>c.value);}
function onGuildSelect(prefix){
  const sel=document.getElementById(\`\${prefix}-guild-sel\`);if(!sel)return;
  const opt=sel.options[sel.selectedIndex];
  const prev=document.getElementById(\`\${prefix}-guild-preview\`);
  if(!opt||!opt.value){if(prev)prev.style.display='none';return;}
  const icon=opt.dataset.icon;
  const ico=document.getElementById(\`\${prefix}-gprev-icon\`);
  if(ico){if(icon){ico.src=icon;ico.style.display='';}else ico.style.display='none';}
  const pn=document.getElementById(\`\${prefix}-gprev-name\`);if(pn)pn.textContent=opt.dataset.name||opt.textContent;
  const pi=document.getElementById(\`\${prefix}-gprev-id\`);if(pi)pi.textContent=opt.value;
  if(prev)prev.style.display='flex';
}
function getGuildId(prefix){const sel=document.getElementById(\`\${prefix}-guild-sel\`);return sel?sel.value:null;}

// ── ROLES ──────────────────────────────────────────────────────────
async function loadRoles(){
  const gid=getGuildId('bc');if(!gid)return bal('bc-al','err',t('errGid'));
  const sel=document.getElementById('bc-role');sel.innerHTML=\`<option>\${t('applying')}</option>\`;
  try{const roles=await api('GET',\`/api/guild/\${gid}/roles\`);sel.innerHTML=\`<option value="">\${t('selRole')}</option>\`;roles.forEach(r=>{const o=document.createElement('option');o.value=r.id;o.textContent=\`\${r.name} (\${r.memberCount} \${t('members')})\`;sel.appendChild(o);});bal('bc-al','ok',\`✅ \${roles.length} \${t('members')}\`);}
  catch(e){sel.innerHTML=\`<option value="">—</option>\`;bal('bc-al','err','❌ '+e.message);}
}

// ── TEMPLATES ──────────────────────────────────────────────────────
async function loadTemplateChips(){
  const wrap=document.getElementById('tpl-chips');wrap.innerHTML='';
  try{const tpls=await api('GET','/api/templates');tpls.forEach(tp=>{const ch=document.createElement('div');ch.className='tpl-chip';ch.textContent=tp.name;ch.title=tp.content;ch.onclick=()=>{document.getElementById('bc-msg').value=tp.content;};wrap.appendChild(ch);});}catch{}
}
async function loadTemplates(){
  const list=document.getElementById('tpl-list');list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const tpls=await api('GET','/api/templates');
    if(!tpls.length){list.innerHTML=\`<div class="em"><div class="ei">💾</div><div class="et">\${t('noTpl')}</div></div>\`;return;}
    list.innerHTML=\`<table><thead><tr><th>\${t('mt-lbl')}</th><th>\${t('fl-bcmsg')}</th><th>\${t('fl-scdt')}</th><th></th></tr></thead><tbody>\${tpls.map(tp=>\`<tr><td style="font-weight:700">\${tp.name}</td><td style="color:var(--mu);font-size:.82rem;max-width:300px">\${tp.content.slice(0,60)}\${tp.content.length>60?'…':''}</td><td style="font-size:.76rem;color:var(--mu)">\${fmtDt(tp.createdAt)}</td><td><button class="btn br btn-sm" onclick="delTpl('\${tp.id}')">🗑️</button></td></tr>\`).join('')}</tbody></table>\`;
  }catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
function saveTplModal(){document.getElementById('tpl-name-in').value='';document.getElementById('tpl-modal').classList.add('open');}
async function saveTpl(){
  const name=document.getElementById('tpl-name-in').value.trim();
  const content=document.getElementById('bc-msg').value.trim();
  if(!name)return;if(!content)return cModal2();
  try{await api('POST','/api/templates',{name,content});cModal2();loadTemplateChips();loadTemplates();bmsg('tmsg','b-gr',\`✅ \${t('tpl-saved')}\`);}
  catch(e){alert(e.message);}
}
function delTpl(id){confirm2(t('ct-tpl'),t('sure'),async()=>{try{await api('DELETE',\`/api/templates/\${id}\`);loadTemplates();loadTemplateChips();}catch(e){alert(e.message);}});}

// ── BROADCAST ─────────────────────────────────────────────────────
async function loadBcGroups(){const sel=document.getElementById('bc-grp');sel.innerHTML=\`<option value="">\${t('selGrp')}</option>\`;try{const gs=await api('GET','/api/groups');gs.forEach(g=>{const o=document.createElement('option');o.value=g.name;o.textContent=\`\${g.name} (\${g.tokenCount} \${t('tok')})\`;sel.appendChild(o);});}catch{}}
function bcTabFn(tab){bcTabMode=tab;document.getElementById('bt-all').classList.toggle('on',tab==='all');document.getElementById('bt-role').classList.toggle('on',tab==='role');document.getElementById('bc-role-sec').style.display=tab==='role'?'block':'none';}

// Preview broadcast
async function previewBroadcast(){
  const groupName=document.getElementById('bc-grp').value;
  const guildIds=getSelectedGuildIds();
  const mode=document.getElementById('bc-mode').value;
  const message=document.getElementById('bc-msg').value.trim();
  const roleId=bcTabMode==='role'?(document.getElementById('bc-role').value||null):null;
  if(!groupName)return bal('bc-al','err',t('errGrp'));
  if(!guildIds.length)return bal('bc-al','err',t('selGuildsFirst'));
  if(!message)return bal('bc-al','err',t('errMsg'));
  const btn=document.getElementById('btn-bc-prev');btn.disabled=true;btn.innerHTML=\`<span class="sp"></span>\`;
  try{
    const d=await api('POST','/api/broadcast/preview',{groupName,guildIds,mode,roleId});
    const body=document.getElementById('prev-body');
    body.innerHTML=\`
      <div class="sg" style="margin-bottom:.8rem">
        <div class="sc"><div class="sc-icon">🤖</div><div class="sv" style="font-size:1.2rem">\${d.bots}</div><div class="ss">Bots</div></div>
        <div class="sc"><div class="sc-icon">👥</div><div class="sv" style="font-size:1.2rem">\${d.totalMembers.toLocaleString()}</div><div class="ss">\${t('members')}</div></div>
        <div class="sc"><div class="sc-icon">🚫</div><div class="sv" style="font-size:1.2rem">\${d.blacklisted}</div><div class="ss">Blacklisted</div></div>
        <div class="sc"><div class="sc-icon">🌐</div><div class="sv" style="font-size:1.2rem">\${d.guilds.length}</div><div class="ss">Guilds</div></div>
      </div>
      <div style="margin-bottom:.6rem">\${d.guilds.map(g=>\`<div style="display:flex;justify-content:space-between;padding:.3rem 0;border-bottom:1px solid var(--bd);font-size:.82rem"><span>\${g.name}</span><span class="b b-bl">\${(g.memberCount||0).toLocaleString()} \${t('members')}</span></div>\`).join('')}</div>
      <div style="background:var(--sf2);border-radius:8px;padding:.6rem .8rem;font-size:.84rem;color:var(--tx2);border:1px solid var(--bd);margin-top:.6rem;white-space:pre-wrap;word-break:break-word">\${message.slice(0,200)}\${message.length>200?'…':''}</div>
    \`;
    document.getElementById('prev-modal').classList.add('open');
  }catch(e){bal('bc-al','err','❌ '+e.message);}
  btn.disabled=false;btn.innerHTML=\`👁️ <span id="btn-prev-txt">\${t('btn-prev-txt')}</span>\`;
}

// Test broadcast (send to self)
function testBroadcast(){
  const message=document.getElementById('bc-msg').value.trim();
  if(!message)return bal('bc-al','err',t('errMsg'));
  bal('bc-al','inf',\`<span class="sp"></span> Sending test DM…\`);
  const proto=location.protocol==='https:'?'wss':'ws';
  const twsObj=new WebSocket(\`\${proto}://\${location.host}\`);
  twsObj.onopen=()=>twsObj.send(JSON.stringify({action:'auth',userId:me.id}));
  twsObj.onmessage=(e)=>{
    const m=JSON.parse(e.data);
    if(m.type==='authed') twsObj.send(JSON.stringify({action:'test_broadcast',userId:me.id,message}));
    if(m.type==='done') {bal('bc-al','ok',\`✅ Test DM sent! Check your Discord.\`);twsObj.close();}
    if(m.type==='error'){bal('bc-al','err','❌ '+m.msg);twsObj.close();}
  };
  twsObj.onerror=()=>bal('bc-al','err',t('connErr'));
}

let bcStartTime=0;
function startBroadcast(confirmed=false){
  const groupName=document.getElementById('bc-grp').value;
  const guildIds=getSelectedGuildIds();
  const mode=document.getElementById('bc-mode').value;
  const delay=parseInt(document.getElementById('bc-spd').value);
  const message=document.getElementById('bc-msg').value.trim();
  const roleId=bcTabMode==='role'?(document.getElementById('bc-role').value||null):null;
  if(!groupName)return bal('bc-al','err',t('errGrp'));
  if(!guildIds.length)return bal('bc-al','err',t('selGuildsFirst'));
  if(!message)return bal('bc-al','err',t('errMsg'));
  document.getElementById('bc-al').innerHTML='';
  document.getElementById('bc-idle').style.display='none';
  document.getElementById('bc-ratelimit').style.display='none';
  document.getElementById('bc-prog').style.display='block';
  document.getElementById('btn-bc-send').disabled=true;
  document.getElementById('btn-bc-stop').style.display='block';
  document.getElementById('bc-ld').style.display='';
  bcStartTime=Date.now();wsOk=false;
  const proto=location.protocol==='https:'?'wss':'ws';ws=new WebSocket(\`\${proto}://\${location.host}\`);
  ws.onopen=()=>ws.send(JSON.stringify({action:'auth',userId:me.id}));
  ws.onmessage=(e)=>{
    const m=JSON.parse(e.data);
    if(m.type==='authed'){const _pl={action:'broadcast',groupName,guildIds,message,delay,mode,roleId,filters:getBcFilters(),dryRun:isDryRun()};if(window._pendingEmbed){_pl.embed=window._pendingEmbed;delete window._pendingEmbed;}ws.send(JSON.stringify(_pl));return;}
    if(m.type==='start'){updProg(0,m.total,0,0);buildBotRows(m.bots);initChart();}
    if(m.type==='stat'||m.type==='done'){const d=m.globalSent+m.globalFailed;updProg(d,m.total||1,m.globalSent,m.globalFailed);if(m.stats)updBotRows(m.stats);pushChart(m.globalSent,m.globalFailed);}
    if(m.type==='ratelimit'){
      const rl=document.getElementById('bc-ratelimit');rl.style.display='block';
      document.getElementById('bc-rl-msg').textContent=m.msg;
      setTimeout(()=>{rl.style.display='none';},m.waitMs+500);
    }
    if(m.type==='done'){
      document.getElementById('bc-ld').style.display='none';
      document.getElementById('bc-ratelimit').style.display='none';
      const retryBadge=document.getElementById('retry-badge');
      if(m.failedCount>0&&retryBadge){retryBadge.style.display='';retryBadge.textContent='+1';}
      if(m.dryRun){bcDone(\`🔍 Dry Run: \${m.total} members would receive this message (nothing sent)\`);}
      else{bcDone(\`\${t('bcComplete')} — \${t('sent')} \${m.globalSent}, \${t('failed')} \${m.globalFailed}\${m.failedCount?\` (<a href="#" onclick="gp('retry');return false">\${t('retryNow')}</a> \${m.failedCount})\`:\`\`}\`);}
    }
    if(m.type==='error'){document.getElementById('bc-ld').style.display='none';bcDone('❌ '+m.msg,true);}
    if(m.type==='info')bal('bc-al','inf','ℹ️ '+m.msg);
  };
  ws.onerror=()=>bcDone(t('connErr'),true);
  ws.onclose=()=>{document.getElementById('bc-ld').style.display='none';if(!wsOk)bcDone(t('connErr'),true);};
}
function stopBc(){wsOk=true;if(ws)ws.close();document.getElementById('bc-ld').style.display='none';bcDone(t('stopped'));}
function bcDone(txt,err=false){document.getElementById('btn-bc-send').disabled=false;document.getElementById('btn-bc-stop').style.display='none';bal('bc-al',err?'err':'ok',txt);wsOk=true;}
function updProg(done,total,sent,failed){
  const pct=total>0?Math.round((done/total)*100):0;
  document.getElementById('bc-pbar').style.width=pct+'%';document.getElementById('bc-pnum').textContent=\`\${done} / \${total}\`;
  document.getElementById('bc-sent').textContent=\`✅ \${sent}\`;document.getElementById('bc-fail').textContent=\`❌ \${failed}\`;
  document.getElementById('bc-rate').textContent=\`\${done>0?Math.round((sent/done)*100):0}%\`;
  if(done>0&&total>done){const el=Date.now()-bcStartTime,rate=done/el,rem=Math.round((total-done)/rate/1000),m=Math.floor(rem/60),s=rem%60;document.getElementById('bc-eta').textContent=\`~\${m>0?m+'m ':''}\${s}s\`;}else document.getElementById('bc-eta').textContent='';
}
function buildBotRows(n){document.getElementById('bc-bstats').innerHTML=Array.from({length:n},(_,i)=>\`<div class="bsr" id="bsr\${i}"><div class="bsrl">\${t('bot-lbl')||'Bot'} #\${i+1}</div><div class="bsrp"><div class="pbar-out"><div class="pbar-in" id="bbar\${i}" style="width:0%"></div></div></div><div class="bsrn" id="bnm\${i}">—</div></div>\`).join('');}
function updBotRows(stats){stats.forEach((s,i)=>{const bar=document.getElementById(\`bbar\${i}\`),nm=document.getElementById(\`bnm\${i}\`);if(!bar||!nm)return;const d=s.sent+s.failed,T=s.assignedCount||1;bar.style.width=Math.round((d/T)*100)+'%';nm.textContent=\`\${s.sent}✅ \${s.failed}❌\`;if(s.completed)bar.style.background='var(--gr)';});}

// ── SCHEDULE ─────────────────────────────────────────────────────────
async function loadSchedGroups(){const sel=document.getElementById('sc-grp');sel.innerHTML=\`<option value="">\${t('selGrp')}</option>\`;try{const gs=await api('GET','/api/groups');gs.forEach(g=>{const o=document.createElement('option');o.value=g.name;o.textContent=\`\${g.name} (\${g.tokenCount} \${t('tok')})\`;sel.appendChild(o);});}catch{}}
async function loadSchedules(){
  const list=document.getElementById('sc-list');list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const sc=await api('GET','/api/schedules');
    const pending=sc.filter(s=>s.status==='pending').length;
    const badge=document.getElementById('sched-badge');if(pending>0){badge.style.display='';badge.textContent=pending;}else badge.style.display='none';
    document.getElementById('ov-sch').textContent=pending;
    if(!sc.length){list.innerHTML=\`<div class="em"><div class="ei">⏰</div><div class="et">\${t('noScheds')}</div></div>\`;return;}
    const sb={pending:\`<span class="b b-yl">⏳ \${t('pending')}</span>\`,done:\`<span class="b b-gr">✅ \${t('done')}</span>\`,missed:\`<span class="b b-rd">❌ \${t('missed')}</span>\`};
    list.innerHTML='<div style="padding:1rem">'+sc.map(s=>{const cd=countdown(s.scheduledAt);return \`<div class="shr"><div style="font-size:1.5rem">⏰</div><div class="shi"><div style="font-weight:700">\${s.groupName} <span style="color:var(--mu);font-weight:400">→ \${ml(s.mode)}</span>\${s.roleId?\` <span class="b b-pu">Role</span>\`:''}</div><div class="sht">📅 \${fmtDt(s.scheduledAt)}\${cd?\`  <span class="countdown">\${cd}</span>\`:''} • <span style="font-family:var(--mo)">\${s.guildId}</span></div><div style="font-size:.75rem;color:var(--mu);margin-top:.12rem">\${s.message.slice(0,60)}\${s.message.length>60?'…':''}</div></div><div style="display:flex;flex-direction:column;align-items:flex-end;gap:.4rem">\${sb[s.status]||\`<span class="b b-mu">\${s.status}</span>\`}\${s.status==='pending'?\`<button class="btn br btn-sm" onclick="delSchedule('\${s.id}')">🗑️</button>\`:''}</div></div>\`;}).join('')+'</div>';
    setTimeout(loadSchedules,10000);
  }catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
async function createSchedule(){
  const groupName=document.getElementById('sc-grp').value;
  const guildId=getGuildId('sc');
  const dtVal=document.getElementById('sc-dt').value;
  const mode=document.getElementById('sc-mode').value;
  const delay=parseInt(document.getElementById('sc-spd').value);
  const message=document.getElementById('sc-msg').value.trim();
  const roleId=document.getElementById('sc-role').value.trim()||null;
  if(!groupName)return bmsg('scmsg','b-rd',t('errGrp'));
  if(!guildId)return bmsg('scmsg','b-rd',t('errGid'));
  if(!dtVal)return bmsg('scmsg','b-rd',t('errDt'));
  if(!message)return bmsg('scmsg','b-rd',t('errMsg'));
  const scheduledAt=new Date(dtVal).toISOString();
  try{const s=await api('POST','/api/schedules',{groupName,guildId,message,scheduledAt,delay,mode,roleId});bmsg('scmsg','b-gr',\`✅ ID: \${s.id}\`);document.getElementById('sc-msg').value='';loadSchedules();loadOverview();}
  catch(e){bmsg('scmsg','b-rd','❌ '+e.message);}
}
function delSchedule(id){confirm2(t('ct-sclist'),t('sure'),async()=>{try{await api('DELETE',\`/api/schedules/\${id}\`);loadSchedules();loadOverview();}catch(e){alert(e.message);}});}

// ── STATS ──────────────────────────────────────────────────────────
async function loadStats(){
  try{
    const [daily,logs]=await Promise.all([api('GET','/api/stats/daily'),api('GET','/api/logs')]);
    const totalSent=logs.reduce((a,l)=>a+(l.sent||0),0);
    const totalFail=logs.reduce((a,l)=>a+(l.failed||0),0);
    document.getElementById('st-total-sent').textContent=totalSent.toLocaleString();
    document.getElementById('st-total-fail').textContent=totalFail.toLocaleString();
    document.getElementById('st-total-bc').textContent=logs.length;
    const total=totalSent+totalFail;
    document.getElementById('st-rate').textContent=total>0?Math.round((totalSent/total)*100)+'%':'—';
    // Chart
    const labels=daily.map(d=>d.date.slice(5));
    const sentData=daily.map(d=>d.sent);
    const failData=daily.map(d=>d.failed);
    if(bcChart)bcChart.destroy();
    const ctx=document.getElementById('bc-chart').getContext('2d');
    bcChart=new Chart(ctx,{type:'bar',data:{labels,datasets:[{label:t('sent'),data:sentData,backgroundColor:'rgba(88,101,242,.7)',borderRadius:4},{label:t('failed'),data:failData,backgroundColor:'rgba(240,71,71,.5)',borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#6b7280',font:{size:11}}}},scales:{x:{ticks:{color:'#6b7280',font:{size:10}},grid:{color:'rgba(42,40,50,.4)'}},y:{ticks:{color:'#6b7280',font:{size:10}},grid:{color:'rgba(42,40,50,.4)'}}}}});
  }catch(e){console.error(e);}
}

// ── LOGS ──────────────────────────────────────────────────────────
async function loadLogs(){
  const list=document.getElementById('logs-list');list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{allLogs=await api('GET','/api/logs');renderLogs();}catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
function filterLogs(){renderLogs();}
function renderLogs(){
  const list=document.getElementById('logs-list');
  const q=(document.getElementById('log-search').value||'').toLowerCase();
  const filtered=q?allLogs.filter(l=>(l.groupName||'').toLowerCase().includes(q)||(l.guildId||'').includes(q)||(l.message||'').toLowerCase().includes(q)||(l.mode||'').includes(q)):allLogs;
  if(!filtered.length){list.innerHTML=\`<div class="em"><div class="ei">📋</div><div class="et">\${t('noLogs')}</div></div>\`;return;}
  list.innerHTML=filtered.map(l=>\`<div class="lr"><div class="li">\${l.type==='scheduled'?'⏰':'📢'}</div><div class="linf"><div style="font-weight:700;font-size:.84rem">\${l.groupName||'?'} <span style="color:var(--mu);font-weight:400">→ \${ml(l.mode)}</span>\${l.roleId?\` <span class="b b-pu" style="font-size:.68rem">Role</span>\`:''}</div><div style="margin-top:.25rem"><span class="b \${l.status==='done'?'b-gr':'b-rd'}">\${l.status==='done'?'✅':'❌'} \${t('sent')} \${l.sent||0}  \${t('failed')} \${l.failed||0}  \${t('total')} \${l.total||0}</span></div>\${l.message?\`<div style="font-size:.73rem;color:var(--mu);margin-top:.2rem">\${l.message}</div>\`:''}</div><div style="text-align:\${lang==='ar'?'left':'right'}"><div class="lt">\${fmtDt(l.timestamp)}</div><div style="font-family:var(--mo);font-size:.67rem;color:var(--mu)">\${l.id}</div></div></div>\`).join('');
}
function exportLogs(fmt){window.open(\`/api/logs/export?format=\${fmt}\`,'_blank');}
function clearLogs(){confirm2(t('btn-lclr'),t('sure'),async()=>{try{await api('DELETE','/api/logs');allLogs=[];renderLogs();}catch(e){alert(e.message);}});}

// ── BOT SETTINGS ───────────────────────────────────────────────────
async function updateAvatar(){const url=document.getElementById('av-url').value.trim();if(!url)return;bal('av-msg','inf',\`<span class="sp"></span> \${t('applying')}\`);try{const r=await api('POST','/api/bots/avatar',{url});bal('av-msg','ok',\`✅ \${r.filter(x=>x.ok).length}/\${r.length} \${t('bots')}\`);}catch(e){bal('av-msg','err','❌ '+e.message);}}
async function updateName(){const name=document.getElementById('nm-in').value.trim();if(!name)return;bal('nm-msg','inf',\`<span class="sp"></span> \${t('applying')}\`);try{const r=await api('POST','/api/bots/name',{name});bal('nm-msg','ok',\`✅ \${r.filter(x=>x.ok).length}/\${r.length} \${t('bots')}\`);}catch(e){bal('nm-msg','err','❌ '+e.message);}}

// ── RETRY FAILED ─────────────────────────────────────────────────
async function loadRetry(){
  const list=document.getElementById('retry-list');list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const d=await api('GET','/api/failed');
    const badge=document.getElementById('retry-badge');
    if(d.count>0){badge.style.display='';badge.textContent=d.count;}else badge.style.display='none';
    if(!d.sessions.length){list.innerHTML=\`<div class="em"><div class="ei">♻️</div><div class="et">\${t('noRetry')}</div></div>\`;return;}
    list.innerHTML='<div style="padding:1rem">'+d.sessions.map(s=>\`
      <div class="shr">
        <div style="font-size:1.5rem">♻️</div>
        <div class="shi">
          <div style="font-weight:700;font-size:.88rem">\${s.groupName} <span style="color:var(--mu)">\${s.guildId}</span></div>
          <div style="font-size:.76rem;color:var(--mu);margin-top:.2rem">📅 \${fmtDt(s.savedAt)} • \${s.members.length} failed members</div>
          <div style="font-size:.75rem;color:var(--mu);margin-top:.15rem;font-style:italic">\${(s.message||'').slice(0,60)}\${(s.message||'').length>60?'…':''}</div>
        </div>
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:.4rem">
          <span class="b b-rd">❌ \${s.members.length}</span>
          <button class="btn bp btn-sm" onclick="retrySession('\${s.id}')">\${t('retryNow')}</button>
          <button class="btn br btn-sm" onclick="delRetry('\${s.id}')">🗑️</button>
        </div>
      </div>\`).join('')+'</div>';
  }catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
function retrySession(sessionId){
  const list=document.getElementById('retry-list');list.innerHTML='<div class="em"><div class="sp"></div></div>';
  gp('broadcast');
  setTimeout(()=>{
    document.getElementById('bc-idle').style.display='none';
    document.getElementById('bc-prog').style.display='block';
    document.getElementById('btn-bc-send').disabled=true;
    document.getElementById('btn-bc-stop').style.display='block';
    document.getElementById('bc-ld').style.display='';
    wsOk=false;
    const proto=location.protocol==='https:'?'wss':'ws';ws=new WebSocket(\`\${proto}://\${location.host}\`);
    ws.onopen=()=>ws.send(JSON.stringify({action:'auth',userId:me.id}));
    ws.onmessage=(e)=>{
      const m=JSON.parse(e.data);
      if(m.type==='authed'){ws.send(JSON.stringify({action:'retry',sessionId}));return;}
      if(m.type==='start'){updProg(0,m.total,0,0);buildBotRows(m.bots);}
      if(m.type==='stat'||m.type==='done'){const d=m.globalSent+m.globalFailed;updProg(d,m.total||1,m.globalSent,m.globalFailed);if(m.stats)updBotRows(m.stats);}
      if(m.type==='done'){document.getElementById('bc-ld').style.display='none';bcDone(\`✅ Retry done — \${m.globalSent} sent, \${m.globalFailed} failed\`);loadRetry();}
      if(m.type==='error'){document.getElementById('bc-ld').style.display='none';bcDone('❌ '+m.msg,true);}
      if(m.type==='info')bal('bc-al','inf','ℹ️ '+m.msg);
      if(m.type==='ratelimit'){const rl=document.getElementById('bc-ratelimit');rl.style.display='block';document.getElementById('bc-rl-msg').textContent=m.msg;setTimeout(()=>rl.style.display='none',m.waitMs+500);}
    };
    ws.onerror=()=>bcDone(t('connErr'),true);
    ws.onclose=()=>{document.getElementById('bc-ld').style.display='none';if(!wsOk)bcDone(t('connErr'),true);};
  },200);
}
async function delRetry(id){confirm2(t('ct-retry'),t('sure'),async()=>{try{await api('DELETE',\`/api/failed/\${id}\`);loadRetry();}catch(e){alert(e.message);}});}

// ── WEBHOOKS ──────────────────────────────────────────────────────
async function loadWebhooks(){
  const list=document.getElementById('wh-list');if(!list)return;
  list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const whs=await api('GET','/api/webhooks');
    if(!whs.length){list.innerHTML=\`<div class="em"><div class="ei">🔔</div><div class="et">\${t('noWebhooks')}</div></div>\`;return;}
    list.innerHTML=\`<table><thead><tr><th>\${t('fl-whlbl')}</th><th>Events</th><th>URL</th><th></th></tr></thead><tbody>\${whs.map(w=>\`<tr>
      <td style="font-weight:700">\${w.label}</td>
      <td>\${w.events.map(ev=>\`<span class="b \${ev==='done'?'b-gr':'b-rd'}" style="margin:.1rem">\${ev==='done'?'✅ done':'❌ error'}</span>\`).join('')}</td>
      <td style="font-size:.72rem;color:var(--mu);font-family:var(--mo)">\${w.url.slice(0,40)}…</td>
      <td><button class="btn br btn-sm" onclick="delWebhook('\${w.id}')">🗑️</button></td>
    </tr>\`).join('')}</tbody></table>\`;
  }catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
async function addWebhook(){
  const url=document.getElementById('wh-url').value.trim();
  const label=document.getElementById('wh-label').value.trim()||'Webhook';
  const events=[];
  if(document.getElementById('whe-done').checked) events.push('done');
  if(document.getElementById('whe-error').checked) events.push('error');
  if(!url)return bmsg('whmsg','b-rd','❌ URL required');
  try{await api('POST','/api/webhooks',{url,label,events});loadWebhooks();document.getElementById('wh-url').value='';document.getElementById('wh-label').value='';bmsg('whmsg','b-gr','✅ Added');}
  catch(e){bmsg('whmsg','b-rd','❌ '+e.message);}
}
async function testWebhook(){
  const url=document.getElementById('wh-url').value.trim();
  if(!url)return bmsg('whmsg','b-rd','❌ Enter URL first');
  bmsg('whmsg','b-yl',\`<span class="sp"></span> Testing…\`);
  try{await api('POST','/api/webhooks/test',{url});bmsg('whmsg','b-gr','✅ Test sent!');}
  catch(e){bmsg('whmsg','b-rd','❌ '+e.message);}
}
async function delWebhook(id){confirm2(t('ct-whlist'),t('sure'),async()=>{try{await api('DELETE',\`/api/webhooks/\${id}\`);loadWebhooks();}catch(e){alert(e.message);}});}

// ── BULK BLACKLIST ────────────────────────────────────────────────
async function bulkAddBL(){
  const raw=document.getElementById('bl-bulk').value.trim();
  if(!raw)return;
  const userIds=[...new Set(raw.replace(/,/g,' ').split(' ').map(x=>x.trim()).filter(x=>x.length>=17&&x.length<=20&&!isNaN(Number(x))))];
  if(!userIds.length)return bmsg('bl-bulk-msg','b-rd','❌ No valid IDs found');
  bmsg('bl-bulk-msg','b-yl',\`<span class="sp"></span> Importing \${userIds.length}…\`);
  try{
    const r=await api('POST','/api/blacklist/bulk',{userIds,note:'Bulk import'});
    bmsg('bl-bulk-msg','b-gr',\`✅ Added \${r.added} / Skipped \${r.skipped}\`);
    document.getElementById('bl-bulk').value='';
    loadBlacklist();
  }catch(e){bmsg('bl-bulk-msg','b-rd','❌ '+e.message);}
}

// ── GROUP HISTORY ─────────────────────────────────────────────────
async function loadGroupHistory(){
  const wrap=document.getElementById('grp-hist-wrap');if(!wrap)return;
  wrap.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const [gh,gs]=await Promise.all([api('GET','/api/groups/history'),api('GET','/api/groups')]);
    if(!gs.length){wrap.innerHTML=\`<div class="em"><div class="et">\${t('noGroups')}</div></div>\`;return;}
    wrap.innerHTML=\`<table><thead><tr><th>\${t('fl-gn')}</th><th>✅ Sent</th><th>❌ Failed</th><th>Broadcasts</th><th>Last</th></tr></thead><tbody>
    \${gs.map(g=>{const h=gh[g.name]||{totalSent:0,totalFailed:0,broadcastCount:0,lastAt:null};const rate=h.totalSent+h.totalFailed>0?Math.round(h.totalSent/(h.totalSent+h.totalFailed)*100):0;return \`<tr>
      <td style="font-weight:700">\${g.name}</td>
      <td><span class="b b-gr">\${(h.totalSent||0).toLocaleString()}</span></td>
      <td><span class="b b-rd">\${(h.totalFailed||0).toLocaleString()}</span></td>
      <td><span class="b b-bl">\${h.broadcastCount||0} <span style="color:var(--mu);font-size:.72rem">(\${rate}%)</span></span></td>
      <td style="font-size:.75rem;color:var(--mu)">\${h.lastAt?fmtDt(h.lastAt):'—'}</td>
    </tr>\`;}).join('')}
    </tbody></table>\`;
  }catch(e){wrap.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}

// ── COLOR THEMES ───────────────────────────────────
function setTheme(name){
  ['green','red','orange','pink','cyan','yellow'].forEach(function(t){document.body.classList.remove('theme-'+t);});
  if(name) document.body.classList.add('theme-'+name);
  try{localStorage.setItem('colorTheme',name||'');}catch{}
  var dd=document.getElementById('theme-dd');if(dd)dd.classList.remove('open');
}
function toggleShortcutHelp(){
  var el=document.getElementById('shortcut-help');
  if(el) el.style.display=el.style.display==='block'?'none':'block';
}
document.addEventListener('click',function(e){
  var dd=document.getElementById('theme-dd');
  var wrap=document.getElementById('theme-picker-wrap');
  if(dd&&wrap&&!wrap.contains(e.target)) dd.classList.remove('open');
});
document.addEventListener('keydown',function(e){
  if(e.target.tagName==='INPUT'||e.target.tagName==='TEXTAREA'||e.target.tagName==='SELECT'||e.ctrlKey||e.metaKey||e.altKey) return;
  if(e.key==='Escape'){var sh=document.getElementById('shortcut-help');if(sh)sh.style.display='none';return;}
  var map={b:'broadcast',s:'schedule',l:'logs',t:'tokens',g:'groups',r:'retry'};
  if(map[e.key.toLowerCase()]) gp(map[e.key.toLowerCase()]);
});

// ── LIVE CHART ─────────────────────────────────────────────────
var _cD={s:[],f:[]};
function initChart(){
  var c=document.getElementById('bc-chart');if(!c)return;
  c.width=c.parentElement.offsetWidth||300;c.height=110;
  _cD.s=[];_cD.f=[];
  var w=document.getElementById('bc-chart-wrap');if(w)w.style.display='block';
}
function pushChart(sent,failed){
  _cD.s.push(sent);_cD.f.push(failed);
  if(_cD.s.length>50){_cD.s.shift();_cD.f.shift();}
  drawChart();
}
function drawChart(){
  var c=document.getElementById('bc-chart');if(!c)return;
  var ctx=c.getContext('2d'),w=c.width,h=c.height,p=6;
  ctx.clearRect(0,0,w,h);
  var maxV=Math.max.apply(null,_cD.s.concat(_cD.f).concat([1]));
  var n=_cD.s.length;if(!n)return;
  var bw=(w-p*2)/n;
  ctx.fillStyle='rgba(35,209,139,.45)';
  _cD.s.forEach(function(v,i){var bh=Math.max(2,((v/maxV)*(h-p*2)));ctx.fillRect(p+i*bw,h-p-bh,Math.max(1,bw-1),bh);});
  ctx.strokeStyle='rgba(240,71,71,.8)';ctx.lineWidth=1.5;ctx.beginPath();
  _cD.f.forEach(function(v,i){var y=h-p-((v/maxV)*(h-p*2));if(i===0)ctx.moveTo(p+i*bw+bw/2,y);else ctx.lineTo(p+i*bw+bw/2,y);});
  ctx.stroke();
}

// ── MEMBER FILTER HELPERS ────────────────────────────────────────────
function getBcFilters(){
  return {
    noAvatar:!!(document.getElementById('f-noavatar')&&document.getElementById('f-noavatar').checked),
    hasAvatar:!!(document.getElementById('f-hasavatar')&&document.getElementById('f-hasavatar').checked),
    minAge:parseInt((document.getElementById('f-minage')||{}).value||'0')||0,
  };
}
function isDryRun(){return !!(document.getElementById('f-dryrun')&&document.getElementById('f-dryrun').checked);}

// ── BOT HEALTH ───────────────────────────────────────────────────
async function loadBotHealth(){
  var wrap=document.getElementById('bot-health-wrap');if(!wrap)return;
  wrap.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    var hd=await api('GET','/api/tokens/health');
    var lb=await api('GET','/api/bot-labels');
    if(!hd.results.length){wrap.innerHTML='<div class="em"><div class="et">\u0644\u0627 \u062a\u0648\u062c\u062f \u062a\u0648\u0643\u0646\u0627\u062a</div></div>';return;}
    var rows=hd.results.map(function(r){
      var name=lb[r.index]||('Bot #'+(r.index+1));
      return '<div class="bsr"><span>'+(r.valid?'\u2705':'\u274c')+'</span>'+
        '<div style="flex:1"><div style="font-weight:700;font-size:.82rem">'+name+'</div>'+
        '<div style="font-size:.72rem;color:var(--mu)">'+(r.valid?('@'+r.username):'Invalid')+'</div></div>'+
        '<div style="display:flex;gap:.4rem;align-items:center">'+
        (r.valid?'<span class="b b-gr">\u2705</span>':'<span class="b b-rd">\u274c</span>')+
        '<input class="fi" value="'+(lb[r.index]||'')+'" placeholder="\u0627\u0633\u0645" style="width:80px;padding:.2rem .4rem;font-size:.75rem" onchange="saveBotLabel('+r.index+',this.value)"/>'+
        '</div></div>';
    }).join('');
    wrap.innerHTML='<div style="font-size:.72rem;color:var(--mu);margin-bottom:.5rem">'+(hd.checkedAt?'\u0622\u062e\u0631 \u0641\u062d\u0635: '+fmtDt(hd.checkedAt):'\u0644\u0645 \u064a\u062a\u0645 \u0641\u062d\u0635 \u0628\u0639\u062f')+
      ' <button class="btn bg btn-sm" onclick="refreshHealth()" style="margin-right:.5rem">\ud83d\udd04</button></div>'+
      '<div style="display:flex;flex-direction:column;gap:.4rem">'+rows+'</div>';
  }catch(e){wrap.innerHTML='<div class="em">'+e.message+'</div>';}
}
async function saveBotLabel(index,label){try{await api('POST','/api/bot-labels',{index:index,label:label});}catch{}}
async function refreshHealth(){
  var wrap=document.getElementById('bot-health-wrap');
  if(wrap)wrap.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{await api('POST','/api/tokens/health/refresh',{});loadBotHealth();}
  catch(e){if(wrap)wrap.innerHTML='<div class="em">'+e.message+'</div>';}
}

// ── PAUSE / RESUME ────────────────────────────────────────────────
let _bcPaused = false;
function pauseBc(){
  if(!ws||ws.readyState!==1)return;
  _bcPaused=!_bcPaused;
  ws.send(JSON.stringify({action:_bcPaused?'pause':'resume'}));
  const btn=document.getElementById('btn-bc-pause');
  const txt=document.getElementById('btn-pause-txt');
  if(_bcPaused){
    btn.classList.remove('bg');btn.classList.add('yl');
    if(txt)txt.textContent=t('resumeBc')||'استئناف';
    bal('bc-al','wrn','⏸️ '+(t('bcPaused')||'البث في وضع الإيقاف المؤقت'));
  } else {
    btn.classList.remove('yl');btn.classList.add('bg');
    if(txt)txt.textContent=t('pauseBc')||'إيقاف مؤقت';
    bal('bc-al','inf','▶️ '+(t('bcResumed')||'تم استئناف البث'));
  }
}

// ── CUSTOM BROADCAST ─────────────────────────────────────────────
async function loadCustGroups(){
  const sel=document.getElementById('cust-grp');if(!sel)return;
  sel.innerHTML=\`<option value="">\${t('selGrp')}</option>\`;
  try{const gs=await api('GET','/api/groups');gs.forEach(g=>{const o=document.createElement('option');o.value=g.name;o.textContent=\`\${g.name} (\${g.tokenCount} \${t('tok')})\`;sel.appendChild(o);});}catch{}
}
function startCustomBroadcast(){
  const groupName=document.getElementById('cust-grp').value;
  const rawIds=document.getElementById('cust-ids').value.trim();
  const message=document.getElementById('cust-msg').value.trim();
  const imageUrl=document.getElementById('cust-img').value.trim();
  const delay=parseInt(document.getElementById('cust-spd').value)||2000;
  if(!groupName)return bal('cust-al','err','❌ '+(t('errGrp')||'اختر مجموعة'));
  if(!rawIds)return bal('cust-al','err','❌ أدخل User IDs');
  if(!message)return bal('cust-al','err','❌ '+(t('errMsg')||'أدخل الرسالة'));
  const userIds=[...new Set(rawIds.replace(/,/g,' ').split(' ').map(x=>x.trim()).filter(x=>x.length>=17&&x.length<=20&&!isNaN(Number(x))))];
  if(!userIds.length)return bal('cust-al','err','❌ لا يوجد IDs صحيحة');
  const btn=document.getElementById('btn-cust-send');
  const stopBtn=document.getElementById('btn-cust-stop');
  btn.disabled=true;if(stopBtn)stopBtn.style.display='flex';
  bal('cust-al','inf',\`<span class="sp"></span> جاري البث لـ \${userIds.length} مستخدم…\`);
  wsOk=false;
  const proto=location.protocol==='https:'?'wss':'ws';
  ws=new WebSocket(\`\${proto}://\${location.host}\`);
  ws.onopen=()=>ws.send(JSON.stringify({action:'auth',userId:me.id}));
  ws.onmessage=(e)=>{
    const m=JSON.parse(e.data);
    if(m.type==='authed'){ws.send(JSON.stringify({action:'broadcast_custom',groupName,message,userIds,delay,imageUrl}));return;}
    if(m.type==='start'){bal('cust-al','inf',\`<span class="sp"></span> بدأ البث — \${m.total} مستخدم\`);}
    if(m.type==='stat'||m.type==='done'){const d=m.globalSent+m.globalFailed;bal('cust-al','inf',\`📊 \${d}/\${m.total} — ✅\${m.globalSent} ❌\${m.globalFailed}\`);}
    if(m.type==='done'){wsOk=true;btn.disabled=false;if(stopBtn)stopBtn.style.display='none';bal('cust-al','ok',\`✅ اكتمل — \${m.globalSent} ✅ \${m.globalFailed} ❌\`);}
    if(m.type==='error'){wsOk=true;btn.disabled=false;if(stopBtn)stopBtn.style.display='none';bal('cust-al','err','❌ '+m.msg);}
    if(m.type==='ratelimit'){bal('cust-al','wrn',\`⚠️ Rate limit — ينتظر \${Math.round(m.waitMs/1000)}s…\`);}
  };
  ws.onerror=()=>{btn.disabled=false;if(stopBtn)stopBtn.style.display='none';bal('cust-al','err','❌ '+(t('connErr')||'خطأ في الاتصال'));};
  ws.onclose=()=>{if(!wsOk){btn.disabled=false;if(stopBtn)stopBtn.style.display='none';}};
}

// ── AUDIT LOG (frontend) ──────────────────────────────────────────
async function loadAuditPage(){
  const list=document.getElementById('audit-list');if(!list)return;
  list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const logs=await api('GET','/api/audit');
    if(!logs.length){list.innerHTML=\`<div class="em"><div class="ei">🔍</div><div class="et">لا توجد أحداث بعد</div></div>\`;return;}
    const icons={add_token:'🔑',del_token:'🗑️',add_owner:'👑',del_owner:'🚫',add_blacklist:'⛔',del_blacklist:'✅',broadcast:'📢',login:'🔓',add_template:'📝',del_template:'🗑️'};
    list.innerHTML='<div style="padding:1rem">'+logs.map(l=>\`
      <div class="shr">
        <div style="font-size:1.4rem">\${icons[l.action]||'📌'}</div>
        <div class="shi">
          <div style="font-weight:700;font-size:.85rem">\${l.action.replace(/_/g,' ')}</div>
          <div style="font-size:.76rem;color:var(--mu);margin-top:.15rem">👤 \${l.username||l.userId} • 📅 \${fmtDt(l.timestamp)}</div>
          \${l.details&&Object.keys(l.details).length?\`<div style="font-size:.72rem;color:var(--mu);margin-top:.1rem;font-family:var(--mo)">\${JSON.stringify(l.details).slice(0,80)}</div>\`:''}
        </div>
        <span class="b b-bl" style="font-size:.7rem;white-space:nowrap">#\${l.id}</span>
      </div>\`).join('')+'</div>';
  }catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}


// ── ROLES PAGE ────────────────────────────────────────────────────
async function loadRolesPage(){
  const list=document.getElementById('roles-list');if(!list)return;
  list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const d=await api('GET','/api/roles');
    const roleColors={owner:'b-pu',admin:'b-bl',mod:'b-gr',viewer:'b-mu'};
    const roleLabels={owner:'👑 Owner',admin:'👑 Admin',mod:'🛠️ Mod',viewer:'👁️ Viewer'};
    const entries=Object.entries(d.roles||{});
    if(!entries.length){list.innerHTML='<div class="em"><div class="et">لا توجد صلاحيات مُعيَّنة بعد</div></div>';return;}
    list.innerHTML='<div style="padding:1rem">'+entries.map(([uid,role])=>\`
      <div class="shr">
        <div style="font-size:1.3rem">🛡️</div>
        <div class="shi"><div style="font-weight:700;font-family:var(--mo);font-size:.85rem">\${uid}</div></div>
        <span class="b \${roleColors[role]||'b-mu'}">\${roleLabels[role]||role}</span>
        <button class="btn br btn-sm" onclick="revokeRole('\${uid}')">🗑️</button>
      </div>\`).join('')+'</div>';
  }catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
async function grantRole(){
  const uid=document.getElementById('role-uid').value.trim();
  const role=document.getElementById('role-sel').value;
  if(!uid)return bmsg('role-msg','b-rd','❌ أدخل User ID');
  try{await api('POST','/api/roles',{userId:uid,role});bmsg('role-msg','b-gr','✅ تم');loadRolesPage();}
  catch(e){bmsg('role-msg','b-rd','❌ '+e.message);}
}
async function revokeRole(uid){
  confirm2('إزالة صلاحية','هل تريد إزالة صلاحية هذا المستخدم؟',async()=>{
    try{await api('DELETE',\`/api/roles/\${uid}\`);loadRolesPage();}catch(e){alert(e.message);}
  });
}

// ── RECURRING BROADCASTS ──────────────────────────────────────────
async function loadRecGrps(){
  const sel=document.getElementById('rec-grp');if(!sel)return;
  sel.innerHTML='<option value="">اختر مجموعة</option>';
  try{const gs=await api('GET','/api/groups');gs.forEach(g=>{const o=document.createElement('option');o.value=g.name;o.textContent=g.name;sel.appendChild(o);});}catch{}
}
async function loadRecurring(){
  const list=document.getElementById('recurring-list');if(!list)return;
  list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const recs=await api('GET','/api/recurring');
    const badge=document.getElementById('rec-badge');
    if(recs.length){badge.style.display='';badge.textContent=recs.filter(r=>r.enabled).length;}else badge.style.display='none';
    if(!recs.length){list.innerHTML='<div class="em"><div class="ei">🔁</div><div class="et">لا يوجد بث متكرر</div></div>';return;}
    list.innerHTML='<div style="padding:1rem">'+recs.map(r=>\`
      <div class="shr">
        <div style="font-size:1.4rem">\${r.enabled?'🔁':'⏸️'}</div>
        <div class="shi">
          <div style="font-weight:700">\${r.groupName} <span class="b b-mu">كل \${r.intervalHours}h</span></div>
          <div style="font-size:.76rem;color:var(--mu);margin-top:.15rem">⏭️ التالي: \${r.nextRun?fmtDt(r.nextRun):'—'} \${r.lastRun?'• آخر تشغيل: '+fmtDt(r.lastRun):''}</div>
          <div style="font-size:.74rem;color:var(--mu);margin-top:.1rem">\${(r.message||'').slice(0,60)}\${(r.message||'').length>60?'…':''}</div>
        </div>
        <div style="display:flex;flex-direction:column;gap:.4rem;align-items:flex-end">
          <span class="b \${r.enabled?'b-gr':'b-mu'}">\${r.enabled?'✅ نشط':'⏸️ موقوف'}</span>
          <div style="display:flex;gap:.3rem">
            <button class="btn bg btn-sm" onclick="toggleRecurring('\${r.id}')">\${r.enabled?'⏸️':'▶️'}</button>
            <button class="btn br btn-sm" onclick="delRecurring('\${r.id}')">🗑️</button>
          </div>
        </div>
      </div>\`).join('')+'</div>';
  }catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
async function addRecurring(){
  const groupName=document.getElementById('rec-grp').value;
  const guildId=document.getElementById('rec-guild').value.trim();
  const message=document.getElementById('rec-msg').value.trim();
  const intervalHours=parseInt(document.getElementById('rec-hours').value)||24;
  const delay=parseInt(document.getElementById('rec-spd').value)||2000;
  if(!groupName)return bmsg('rec-msg-al','b-rd','❌ اختر مجموعة');
  if(!guildId)return bmsg('rec-msg-al','b-rd','❌ أدخل Guild ID');
  if(!message)return bmsg('rec-msg-al','b-rd','❌ أدخل الرسالة');
  try{await api('POST','/api/recurring',{groupName,guildId,message,intervalHours,delay,mode:'bc'});bmsg('rec-msg-al','b-gr','✅ تم الإضافة');loadRecurring();}
  catch(e){bmsg('rec-msg-al','b-rd','❌ '+e.message);}
}
async function toggleRecurring(id){try{await api('PATCH',\`/api/recurring/\${id}/toggle\`,{});loadRecurring();}catch(e){alert(e.message);}}
async function delRecurring(id){confirm2('حذف بث متكرر','هل أنت متأكد؟',async()=>{try{await api('DELETE',\`/api/recurring/\${id}\`);loadRecurring();}catch(e){alert(e.message);}});}

// ── BACKUP ────────────────────────────────────────────────────────
async function loadBackups(){
  const list=document.getElementById('backup-list');if(!list)return;
  list.innerHTML='<div class="em"><div class="sp"></div></div>';
  try{
    const bk=await api('GET','/api/backups');
    if(!bk.length){list.innerHTML='<div class="em"><div class="et">لا توجد نسخ احتياطية بعد</div></div>';return;}
    list.innerHTML='<table><thead><tr><th>الاسم</th><th>الحجم</th><th></th></tr></thead><tbody>'+
      bk.map(b=>\`<tr><td style="font-family:var(--mo);font-size:.78rem">\${b.name}</td><td style="font-size:.78rem;color:var(--mu)">\${(b.size/1024).toFixed(1)} KB</td><td><a href="/api/backups/\${b.name}" download class="btn bg btn-sm">⬇️</a></td></tr>\`).join('')+
      '</tbody></table>';
  }catch(e){list.innerHTML=\`<div class="em">\${e.message}</div>\`;}
}
async function doBackupNow(){
  bmsg('backup-msg','b-yl','<span class="sp"></span> جاري النسخ…');
  try{await api('POST','/api/backups/now',{});bmsg('backup-msg','b-gr','✅ تم');loadBackups();}
  catch(e){bmsg('backup-msg','b-rd','❌ '+e.message);}
}

// ── EMBED BUILDER ─────────────────────────────────────────────────
async function loadEmbedGroups(){
  // reuse bc groups selector for embed broadcast
}
function previewEmbed(){
  const title=document.getElementById('em-title').value;
  const desc=document.getElementById('em-desc').value;
  const footer=document.getElementById('em-footer').value;
  const color=document.getElementById('em-color').value;
  document.getElementById('em-preview').style.borderLeftColor=color;
  const titleEl=document.getElementById('em-prev-title');
  const descEl=document.getElementById('em-prev-desc');
  const footerEl=document.getElementById('em-prev-footer');
  titleEl.textContent=title;titleEl.style.display=title?'':'none';
  descEl.textContent=desc;descEl.style.display=desc?'':'none';
  footerEl.textContent=footer;footerEl.style.display=footer?'':'none';
  document.getElementById('em-color-hex').value=color;
}
document.addEventListener('input',e=>{if(e.target.id==='em-color'){document.getElementById('em-color-hex').value=e.target.value;previewEmbed();}});
async function sendEmbedBroadcast(){
  const msg=document.getElementById('em-msg').value.trim();
  const embed={
    title:document.getElementById('em-title').value.trim()||null,
    description:document.getElementById('em-desc').value.trim()||null,
    color:document.getElementById('em-color').value||null,
    footer:document.getElementById('em-footer').value.trim()||null,
    imageUrl:document.getElementById('em-img').value.trim()||null,
    thumbnail:document.getElementById('em-thumb').value.trim()||null,
  };
  if(!msg&&!embed.title&&!embed.description)return bmsg('em-msg-al','b-rd','❌ أدخل رسالة أو Embed');
  // Navigate to broadcast page with embed prefilled
  gp('broadcast');
  setTimeout(()=>{
    if(msg)document.getElementById('bc-msg').value=msg;
    window._pendingEmbed=embed;
    bmsg('bc-al','inf','🎨 Embed جاهز — اختر المجموعة والسيرفر وابدأ البث');
  },200);
}

// ── SECURITY / 2FA ────────────────────────────────────────────────
async function loadSecurity(){
  if(me){
    const el=document.getElementById('sec-username');if(el)el.textContent=me.username||me.id;
    const rel=document.getElementById('sec-role');if(rel){rel.textContent=window._myRole||'—';}
  }
  document.getElementById('twofa-status').innerHTML='<span class="b b-mu">2FA: كل تسجيل دخول يتطلب كود Discord DM</span>';
}
async function request2FA(){
  bmsg('twofa-msg','b-yl','<span class="sp"></span> جاري إرسال الكود…');
  try{
    await api('POST','/api/2fa/request',{});
    document.getElementById('twofa-verify-wrap').style.display='block';
    bmsg('twofa-msg','b-gr','✅ تم إرسال الكود على Discord DM');
  }catch(e){bmsg('twofa-msg','b-rd','❌ '+e.message);}
}
async function verify2FA(){
  const code=document.getElementById('twofa-code').value.trim();
  if(!code)return bmsg('twofa-msg','b-rd','❌ أدخل الكود');
  try{
    await api('POST','/api/2fa/verify',{code});
    bmsg('twofa-msg','b-gr','✅ تم التحقق!');
    document.getElementById('twofa-verify-wrap').style.display='none';
    boot();
  }catch(e){bmsg('twofa-msg','b-rd','❌ '+e.message);}
}

// ── MULTI-MESSAGE BROADCAST ───────────────────────────────────────
// Extend startBroadcast to pass embed if pending
const _origStartBc = typeof startBroadcast !== 'undefined' ? startBroadcast : null;

boot();
</script>
</body>
</html>
`;


app.get("/", (req, res) => {
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(DASHBOARD_HTML);
});

// ─── Start ─────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`🌐  Dashboard → http://localhost:${PORT}`);
  console.log(`🔑  Discord OAuth2 login`);
});
