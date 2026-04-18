import html


def render_admin_page(admin_username: str = "admin") -> str:
    return (r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>goodsmall 后台</title>
  <style>
    :root{
      --bg:#f3f6fb;
      --panel:#ffffff;
      --panel-soft:#f8fbff;
      --text:#111827;
      --muted:#6b7280;
      --line:#e5e7eb;
      --primary:#0f172a;
      --primary-2:#1e293b;
      --accent:#2563eb;
      --green:#16a34a;
      --orange:#f59e0b;
      --red:#ef4444;
      --shadow:0 10px 30px rgba(15,23,42,.08);
      --radius:18px;
    }
    *{box-sizing:border-box}
    html,body{margin:0;padding:0}
    body{font-family:Inter,"PingFang SC","Microsoft YaHei",Arial,sans-serif;background:linear-gradient(180deg,#eef4ff 0,#f6f8fc 180px,var(--bg) 180px);color:var(--text)}
    a{text-decoration:none;color:inherit}
    .wrap{max-width:1680px;margin:0 auto;padding:28px 22px 36px}
    .hero{background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 100%);color:#fff;border-radius:26px;padding:28px 28px 22px;box-shadow:0 18px 40px rgba(29,78,216,.20);margin-bottom:22px;position:relative;overflow:hidden}
    .hero:before,.hero:after{content:"";position:absolute;border-radius:999px;background:rgba(255,255,255,.08)}
    .hero:before{width:240px;height:240px;right:-60px;top:-100px}
    .hero:after{width:180px;height:180px;right:160px;bottom:-120px}
    .hero-head{display:flex;justify-content:space-between;gap:24px;align-items:flex-start;position:relative;z-index:1}
    .hero-title{font-size:28px;font-weight:800;letter-spacing:.2px;margin:0 0 8px}
    .hero-sub{font-size:14px;line-height:1.7;color:rgba(255,255,255,.82);max-width:820px}
    .hero-badge{display:inline-flex;align-items:center;gap:8px;padding:10px 14px;background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.12);border-radius:999px;font-size:13px;color:#fff;backdrop-filter:blur(8px)}
    .topbar{display:flex;gap:10px;flex-wrap:wrap;margin-top:22px;position:relative;z-index:1}
    .tab-btn{border:none;background:rgba(255,255,255,.16);color:rgba(255,255,255,.88);border-radius:14px;padding:12px 18px;cursor:pointer;font-weight:700;font-size:14px;letter-spacing:.2px;transition:.18s ease;backdrop-filter:blur(12px);border:1px solid rgba(255,255,255,.10)}
    .tab-btn:hover{transform:translateY(-1px);background:rgba(255,255,255,.22)}
    .tab-btn.active{background:#fff;color:var(--primary);box-shadow:0 8px 20px rgba(15,23,42,.16)}
    .section{display:none}.section.active{display:block;animation:fadeIn .18s ease}
    @keyframes fadeIn{from{opacity:.6;transform:translateY(4px)}to{opacity:1;transform:none}}
    .grid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:18px;align-items:start}
    .col-12{grid-column:span 12}.col-8{grid-column:span 8}.col-7{grid-column:span 7}.col-6{grid-column:span 6}.col-5{grid-column:span 5}.col-4{grid-column:span 4}.col-3{grid-column:span 3}.grid > [class^="col-"]{min-width:0}
    .card{background:var(--panel);border:1px solid rgba(15,23,42,.05);border-radius:var(--radius);padding:20px;box-shadow:var(--shadow);margin-bottom:18px}
    .card.soft{background:var(--panel-soft)}
    .card-head{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px}
    h2,h3{margin:0 0 12px}.title{font-size:20px;font-weight:800}.subtitle{font-size:13px;color:var(--muted)}
    label{display:block;font-size:13px;color:var(--muted);margin-bottom:7px;font-weight:600}
    input,select,textarea{width:100%;padding:12px 14px;border:1px solid var(--line);border-radius:14px;font-size:14px;background:#fff;color:var(--text);outline:none;transition:border-color .15s ease,box-shadow .15s ease}
    input:focus,select:focus,textarea:focus{border-color:#93c5fd;box-shadow:0 0 0 4px rgba(37,99,235,.10)}
    textarea{min-height:110px;resize:vertical}
    button{border:none;border-radius:14px;padding:12px 16px;cursor:pointer;background:var(--primary);color:#fff;font-weight:700;letter-spacing:.1px;transition:transform .15s ease,opacity .15s ease,box-shadow .15s ease;box-shadow:0 10px 20px rgba(15,23,42,.12)}
    button:hover{transform:translateY(-1px)}
    button.secondary{background:#64748b}
    button.orange{background:var(--orange)}
    button.red{background:var(--red)}
    button.green{background:var(--green)}
    button.small{padding:8px 12px;font-size:12px;border-radius:10px;box-shadow:none}
    .actions{display:flex;gap:8px;flex-wrap:wrap}
    .actions input[type=file]{flex:1;min-width:220px;background:#fff}
    .table-wrap{overflow:auto;border:1px solid var(--line);border-radius:16px;background:#fff}
    table{width:100%;border-collapse:separate;border-spacing:0;font-size:14px;min-width:860px}
    th,td{padding:12px 10px;border-bottom:1px solid #eef2f7;text-align:left;vertical-align:top}
    th{background:#f8fafc;font-weight:800;color:#334155;position:sticky;top:0;z-index:1}
    tbody tr:hover td{background:#fbfdff}
    tr:last-child td{border-bottom:none}
    .status{margin-top:12px;padding:12px 14px;border-radius:14px;background:#f8fafc;color:#334155;font-size:13px;border:1px solid var(--line)}
    .tag{display:inline-flex;align-items:center;padding:5px 10px;border-radius:999px;font-size:12px;font-weight:700}
    .tag.ok{background:#e8f7ee;color:#0a8f4d}
    .tag.off{background:#f1f5f9;color:#64748b}
    .tag.warn{background:#fff7e6;color:#b45309}
    .tag.orange{background:#fff7e6;color:#c2410c}
    .tag.red{background:#fee2e2;color:#b91c1c}
    .thumb{width:58px;height:58px;border-radius:14px;object-fit:cover;background:#f8fafc;border:1px solid var(--line)}
    .preview{width:108px;height:108px;border-radius:16px;object-fit:cover;background:#f8fafc;border:1px solid var(--line)}
    .preview-wrap{display:flex;align-items:center;gap:14px;padding:12px 0}
    .muted{color:var(--muted);font-size:12px;line-height:1.6}.kpi{font-size:36px;font-weight:900;margin-top:10px;color:var(--primary)}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;word-break:break-all}
    .space-top{margin-top:14px}.divider{height:1px;background:#eef2f7;margin:16px 0}
    .metric-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:18px}
    .metric-card{background:linear-gradient(180deg,#ffffff,#f8fbff);border:1px solid rgba(15,23,42,.06);border-radius:20px;padding:18px 18px 14px;box-shadow:var(--shadow)}
    .metric-label{font-size:13px;color:var(--muted);font-weight:700}
    .metric-tip{font-size:12px;color:#94a3b8;margin-top:8px}
    .toolbar-inline{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
    .chip{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;background:#eff6ff;color:#1d4ed8;font-size:12px;font-weight:700}
    .form-sticky{position:sticky;top:20px}
    .empty-box{display:flex;align-items:center;justify-content:center;min-height:220px;border:1px dashed #cbd5e1;border-radius:18px;background:#fafcff;color:#94a3b8;font-size:14px}
    .upload-progress-card{margin-top:12px;padding:12px 14px;border-radius:14px;background:#f8fafc;border:1px solid var(--line)}
    .upload-progress-bar{height:12px;border-radius:999px;background:#e5e7eb;overflow:hidden;position:relative}
    .upload-progress-fill{height:100%;width:0%;background:linear-gradient(90deg,#2563eb,#60a5fa);transition:width .15s ease}
    .upload-progress-meta{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-top:8px;font-size:12px;color:#64748b}
    .upload-progress-tip{margin-top:8px;font-size:12px;color:#64748b;line-height:1.6}
    .task-log-box{min-height:280px;max-height:360px;overflow:auto;border:1px dashed #cbd5e1;border-radius:18px;background:#fafcff;padding:12px}
    .task-log-item{padding:10px 12px;border-radius:12px;background:#fff;border:1px solid #e5e7eb;margin-bottom:10px;font-size:13px;line-height:1.65;color:#334155}
    .task-log-item:last-child{margin-bottom:0}
    .task-log-time{font-size:12px;color:#94a3b8;margin-right:8px}
.bot-save-toast{position:fixed;right:24px;bottom:24px;z-index:9999;min-width:320px;max-width:480px;padding:14px 16px;border-radius:16px;box-shadow:0 14px 40px rgba(15,23,42,.18);font-size:14px;line-height:1.6;display:none}
.bot-save-toast.success{background:#ecfdf5;border:1px solid #86efac;color:#166534}
.bot-save-toast.error{background:#fef2f2;border:1px solid #fca5a5;color:#991b1b}
.bot-save-toast .title{font-weight:800;margin-bottom:4px}
    
    /* 商品页布局修复 */
    #section-products .grid{align-items:start}
    #section-products .col-5,#section-products .col-7{min-width:0}
    #section-products .products-left-col,#section-products .products-right-col{display:flex;flex-direction:column;gap:18px;align-self:start;min-width:0}
    #section-products .products-left-col>.card,#section-products .products-right-col>.card{margin-bottom:0}
    #section-products .card{min-width:0;position:relative;z-index:0}
    #section-products .table-wrap{width:100%;overflow-x:auto;overflow-y:hidden}
    #section-products table{min-width:1100px}
    #section-products th,#section-products td{white-space:nowrap}
    #section-products td:nth-child(2){white-space:normal;min-width:260px}
    #section-products .actions{flex-wrap:wrap}
    #section-products .preview-wrap{padding-bottom:4px}
    #section-products .form-sticky{position:static;top:auto}
    #section-products .product-form-card{max-height:none;overflow:visible;padding-right:20px;padding-bottom:20px}
    #section-products .product-import-card{scroll-margin-top:20px;clear:both}
    #section-products .product-import-card .status,#section-products .product-form-card .status{position:relative;z-index:0}
    #productsTable .muted{white-space:normal}
    @media (max-width: 1200px){ .metric-grid{grid-template-columns:repeat(2,minmax(0,1fr));} .col-8,.col-5,.col-4,.col-3{grid-column:span 12} .form-sticky{position:static} .hero-head{flex-direction:column} }
    @media (max-width: 720px){ .wrap{padding:16px} .hero{padding:20px 18px} .hero-title{font-size:22px} .metric-grid{grid-template-columns:1fr} table{min-width:720px} }
  
    /* SKU form layout fix */
    #section-products .sku-row .grid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:12px;align-items:end}
    #section-products .sku-row .col-4{grid-column:span 4;min-width:0}
    #section-products .sku-row .col-2{grid-column:span 2;min-width:0}
    #section-products .sku-row input,
    #section-products .sku-row select{width:100%;min-width:0;box-sizing:border-box}
    #section-products .sku-row label{display:block;margin-bottom:6px}
    #section-products .sku-row .actions{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
    @media (max-width:1200px){
      #section-products .sku-row .col-4,
      #section-products .sku-row .col-2{grid-column:span 6}
    }
    @media (max-width:720px){
      #section-products .sku-row .col-4,
      #section-products .sku-row .col-2{grid-column:span 12}
    }

  </style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <div class="hero-head">
      <div>
        <div class="hero-title">Goodsmall 实货商城后台</div>
        <div class="hero-sub">当前版本聚焦于分类、商品、订单和支付地址管理。先把后台做顺手，再继续推进 100 Bot 架构与物流查询链路。</div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:flex-end"><div class="hero-badge" id="adminIdentityBadge">已登录 · __ADMIN_USERNAME__</div><button class="small secondary" onclick="logoutAdmin()" style="background:rgba(255,255,255,.18);color:#fff;border:1px solid rgba(255,255,255,.18);box-shadow:none">退出登录</button></div>
    </div>
    <div class="topbar">
      <button class="tab-btn" data-tab="dashboard" onclick="showTab('dashboard',this)">概览总览</button>
      <button class="tab-btn" data-tab="bots" onclick="showTab('bots',this)">机器人管理</button>
      <button class="tab-btn" data-tab="sessions" onclick="showTab('sessions',this)">会话中心</button>
      <button class="tab-btn" data-tab="announcements" onclick="showTab('announcements',this)">商城公告</button>
      <button class="tab-btn" data-tab="folder-link" onclick="showTab('folder-link',this)">共享文件夹</button>
      <button class="tab-btn" data-tab="admins" onclick="showTab('admins',this)">管理员</button>
      <button class="tab-btn" data-tab="categories" onclick="showTab('categories',this)">分类管理</button>
      <button class="tab-btn" data-tab="products" onclick="showTab('products',this)">商品管理</button>
      <button class="tab-btn" data-tab="orders" onclick="showTab('orders',this)">订单管理</button>
      <button class="tab-btn" data-tab="logistics" onclick="showTab('logistics',this)">物流中心</button>
      <button class="tab-btn" data-tab="shipping" onclick="showTab('shipping',this)">发货中心</button>
      <button class="tab-btn" data-tab="suppliers" onclick="showTab('suppliers',this)">供应链中心</button>
      <button class="tab-btn" data-tab="data-center" onclick="showTab('data-center',this)">数据中心</button>
      <button class="tab-btn" data-tab="payment" onclick="showTab('payment',this)">支付地址</button>
    </div>
  </div>

  <div id="section-dashboard" class="section">
    <div class="metric-grid">
      <div class="metric-card"><div class="metric-label">分类数量</div><div class="kpi" id="kpiCategories">0</div><div class="metric-tip">用于商品浏览入口与前台分层展示</div></div>
      <div class="metric-card"><div class="metric-label">商品数量</div><div class="kpi" id="kpiProducts">0</div><div class="metric-tip">只统计后台当前可管理的商品记录</div></div>
      <div class="metric-card"><div class="metric-label">订单数量</div><div class="kpi" id="kpiOrders">0</div><div class="metric-tip">后续会继续接入支付与物流状态联动</div></div>
      <div class="metric-card"><div class="metric-label">支付地址</div><div class="kpi" id="kpiPaymentAddresses">0</div><div class="metric-tip">USDT 收款地址池与二维码配置入口</div></div>
      <div class="metric-card"><div class="metric-label">启用机器人</div><div class="kpi" id="kpiBotsEnabled">0</div><div class="metric-tip">当前配置为启用状态的机器人数量</div></div>
      <div class="metric-card"><div class="metric-label">运行中机器人</div><div class="kpi" id="kpiBotsRunning">0</div><div class="metric-tip">最近心跳显示为运行中的机器人数量</div></div>
    </div>
    <div class="grid space-top">
      <div class="col-8">
        <div class="card">
          <div class="card-head"><div><div class="title">当前说明</div><div class="subtitle">后台视觉已切到更适合运营的桌面版布局</div></div><div class="chip">后台美化合并版</div></div>
          <div class="muted">这一版把后台美化、100 Bot 架构和 v5 物流能力合并到同一份管理页。你可以直接在这里管理 Bot、分类、商品、订单、支付地址，并联动物流同步。</div>
        </div>
      </div>
      <div class="col-4">
        <div class="card soft">
          <div class="card-head"><div><div class="title">下一步建议</div><div class="subtitle">按风险从低到高推进</div></div></div>
          <div class="muted">1. 批量绑定 Buyer / Shipping Bot<br>2. 接着补发货中心与导入已发货<br>3. 再补物流统计与面单对账</div>
        </div>
      </div>
    </div>
  </div>


  <div id="section-bots" class="section">
    <div class="grid">
      <div class="col-4">
        <div class="card form-sticky">
          <div class="card-head"><div><div class="title">机器人表单</div><div class="subtitle">用于绑定商城机器人与供应链机器人</div></div></div>
          <input id="botId" type="hidden" />
          <label>Bot 编码</label><input id="botCode" placeholder="buyer001" />
          <label class="space-top">Bot Token</label><input id="botToken" placeholder="123456:ABC..." />
          <label class="space-top">机器人类型</label><select id="botType"><option value="buyer">商城机器人</option><option value="shipping">供应链机器人</option><option value="session">聚合聊天机器人</option></select>
          <label class="space-top">机器人名称（同步到 Telegram）</label><input id="botName" placeholder="商城助手 / 发货助手" maxlength="64" />
          <label class="space-top">机器人别称（后台管理用）</label><input id="botAlias" placeholder="海001 / 发货A / 聚合客服" maxlength="128" />
          <label class="space-top">短简介（同步到 Telegram）</label><input id="botShortDescription" placeholder="展示在机器人资料页，最多 120 字" maxlength="120" />
          <label class="space-top">机器人简介（同步到 Telegram）</label><textarea id="botDescription" placeholder="展示在空聊天页，最多 512 字"></textarea>
          <label class="space-top">启动欢迎词（/start 首句，独立字段）</label><textarea id="botStartWelcomeText" placeholder="例如：欢迎来到实货商城。\n请选择下方功能进入。最多 512 字"></textarea>
          <div class="muted">说明：正式独立字段版。此字段只控制 /start 首句，不会再占用 Telegram 机器人简介。</div>
          <label class="space-top">机器人头像（JPG/JPEG）</label>
          <div style="display:flex;gap:10px;align-items:flex-start;flex-wrap:wrap">
            <div style="width:88px;height:88px;border:1px dashed var(--line);border-radius:18px;display:flex;align-items:center;justify-content:center;overflow:hidden;background:#f8fafc">
              <img id="botAvatarPreview" src="" alt="bot avatar" style="width:100%;height:100%;object-fit:cover;display:none" />
              <div id="botAvatarPlaceholder" class="muted" style="font-size:12px;text-align:center;padding:8px">未上传头像</div>
            </div>
            <div style="flex:1;min-width:220px">
              <input id="botAvatarImage" placeholder="上传后自动填充头像地址" />
              <div class="actions space-top"><input id="botAvatarFile" type="file" accept=".jpg,.jpeg,image/jpeg" /><button class="secondary" type="button" onclick="uploadBotAvatar()">上传头像</button><button class="secondary" type="button" onclick="clearBotAvatar()">清空头像</button></div>
              <div class="muted space-top">Telegram 机器人头像同步目前按官方接口要求使用 JPG/JPEG。</div>
            </div>
          </div>
          <label class="space-top">绑定供应链编码</label><input id="botSupplierCode" placeholder="A / B，仅供应链机器人需要" />
          <label class="space-top">状态</label><select id="botIsEnabled"><option value="true">启用</option><option value="false">停用</option></select>
          <div class="actions space-top"><button class="green" onclick="saveBot()">保存 Bot</button><button class="secondary" onclick="syncSelectedBotProfile()">重新同步资料</button><button class="secondary" onclick="clearBotForm()">清空</button></div>
          <div id="botsStatus" class="status">-</div>
        </div>
      </div>
      <div class="col-8">
        <div class="card">
          <div class="card-head"><div><div class="title">机器人列表</div><div class="subtitle">启用状态由后台统一控制；运行状态由 Runner 心跳回写</div><div id="botAutoSyncInfo" class="muted">自动同步：加载中...</div></div><div class="actions"><button class="secondary" onclick="manualRunBotProfileAutoSync()">按配置立即同步</button><button class="secondary" onclick="batchSyncBotProfiles('enabled')">批量同步启用机器人</button><button class="secondary" onclick="batchSyncBotProfiles('all')">批量同步全部机器人</button></div></div>
          <div id="botsTable" class="table-wrap"></div>
        </div>
        <div class="card">
          <div class="card-head"><div><div class="title">运行状态</div><div class="subtitle">用于观察 100 Bot 架构下的收敛情况</div></div></div>
          <div id="botRuntimeTable" class="table-wrap"></div>
        </div>
      </div>
    </div>
  </div>

<div id="section-sessions" class="section">
  <div class="metric-grid">
    <div class="metric-card"><div class="metric-label">总会话数</div><div class="kpi" id="chatSessionCount">0</div><div class="metric-tip">所有商城机器人聚合后的客户会话</div></div>
    <div class="metric-card"><div class="metric-label">开放会话</div><div class="kpi" id="chatOpenCount">0</div><div class="metric-tip">当前仍在跟进中的客户会话</div></div>
    <div class="metric-card"><div class="metric-label">已关闭会话</div><div class="kpi" id="chatClosedCount">0</div><div class="metric-tip">人工处理完成后可关闭</div></div>
    <div class="metric-card"><div class="metric-label">未读消息</div><div class="kpi" id="chatUnreadCount">0</div><div class="metric-tip">来自所有 Bot 的待处理客户消息</div></div>
  </div>
  <div class="card space-top">
    <div class="card-head"><div><div class="title">聚合机器人订阅状态</div><div class="subtitle">会话中心自动推送运行状态</div></div><div class="actions"><button class="secondary" onclick="loadSessionRuntimeStatus()">刷新状态</button></div></div>
    <div id="sessionRuntimeInfo" class="status">-</div>
    <div class="muted space-top">首次使用请先到聚合机器人窗口发送 /start 开启自动推送；发送 /mute 可关闭。</div>
  </div>
  <div class="grid space-top">
    <div class="col-4">
      <div class="card form-sticky">
        <div class="card-head"><div><div class="title">会话筛选</div><div class="subtitle">聚合 100 Bot 客户消息</div></div></div>
        <label>Bot 编码</label><input id="chatFilterBotCode" placeholder="buyer001，可留空" oninput="debounceChatRefresh()" />
        <label class="space-top">状态</label><select id="chatFilterStatus" onchange="loadChatCenter(true)"><option value="open">开放</option><option value="closed">已关闭</option><option value="all">全部</option></select>
        <label class="space-top">关键字</label><input id="chatFilterQ" placeholder="客户昵称 / 用户ID / 订单号 / 手机号 / 单号" oninput="debounceChatRefresh()" />
        <label class="space-top">只看未读</label><select id="chatFilterUnread" onchange="loadChatCenter(true)"><option value="true">是</option><option value="false">否</option></select>
        <div class="actions space-top"><button class="green" onclick="loadChatCenter(true)">刷新会话</button><button class="secondary" onclick="clearChatFilters()">清空</button></div>
        <div id="chatStatus" class="status">-</div>
      </div>
      <div class="card space-top">
        <div class="card-head"><div><div class="title">关键词屏蔽</div><div class="subtitle">屏蔽命令词和客服不想聚合的文本</div></div></div>
        <input id="chatKeywordBlockId" type="hidden" />
        <label>关键词</label><input id="chatKeywordValue" placeholder="商品分类 / 在吗 / 售后" />
        <label class="space-top">匹配方式</label><select id="chatKeywordMatchType"><option value="exact">完全匹配</option><option value="contains">包含匹配</option></select>
        <label class="space-top">备注</label><input id="chatKeywordRemark" placeholder="例如：系统命令词 / 无效闲聊" />
        <label class="space-top">状态</label><select id="chatKeywordIsActive"><option value="true">启用</option><option value="false">停用</option></select>
        <div class="actions space-top"><button class="green" onclick="saveChatKeywordBlock()">保存屏蔽词</button><button class="secondary" onclick="clearChatKeywordForm()">清空</button></div>
        <div id="chatKeywordStatus" class="status">-</div>
        <div class="space-top" id="chatKeywordBlocksTable"></div>
      </div>
    </div>
    <div class="col-8">
      <div class="card">
        <div class="card-head"><div><div class="title">聚合会话列表</div><div class="subtitle">每个客户给任意商城机器人发消息，都会在这里聚合</div></div></div>
        <div id="chatSessionsTable" class="table-wrap"></div>
      </div>
      <div class="card">
        <div class="card-head"><div><div class="title">会话详情 / 回复客户</div><div class="subtitle">直接回客户时，会通过原始商城机器人发出</div></div></div>
        <div id="chatSessionDetail" class="empty-box">请先从左侧打开一个会话。</div>
        <div class="space-top">
          <label>回复内容</label>
          <textarea id="chatReplyText" placeholder="在这里输入回复内容，将通过原始商城机器人发送给客户"></textarea>
          <div class="actions space-top"><button class="green" onclick="sendChatReply()">发送回复</button><button class="secondary" onclick="markCurrentChatRead()">标记已读</button><button class="orange" onclick="closeCurrentChat()">关闭会话</button><button class="secondary" onclick="reopenCurrentChat()">重新打开</button></div>
        </div>
      </div>
    </div>
  </div>
</div>

<div id="section-announcements" class="section">
  <div class="grid">
    <div class="col-5">
      <div class="card form-sticky">
        <div class="card-head"><div><div class="title">首次启动公告</div><div class="subtitle">首次 /start 时可按相册组一次发送 2 到 4 个视频，短文案挂在第 1 个视频下方</div></div></div>
        <label>适用机器人类型</label>
        <select id="annTargetBotTypes" multiple size="3">
          <option value="buyer">商城机器人</option>
          <option value="shipping">供应链机器人</option>
          <option value="session">聚合聊天机器人</option>
        </select>
        <label class="space-top">公告标题</label><input id="annTitle" placeholder="例如：商城上新通知" />
        <label class="space-top">短文案（挂在第 1 个视频下方）</label><textarea id="annContent" placeholder="建议控制在 1024 字以内"></textarea>
        <label class="space-top">发送模式</label>
        <select id="annMediaMode">
          <option value="video_album">多视频相册组</option>
          <option value="single_video">单视频</option>
          <option value="none">纯文字</option>
        </select>
        <label class="space-top">文案模式</label>
        <select id="annTextMode">
          <option value="caption_first">挂第 1 个视频</option>
          <option value="album_then_text">相册后补文字（当前先按第 1 个视频处理）</option>
        </select>
        <label class="space-top">/start 欢迎词处理</label>
        <select id="annReplaceStartWelcome">
          <option value="true">替代旧欢迎词</option>
          <option value="false">保留旧欢迎词</option>
        </select>
        <label class="space-top">发送失败回退</label>
        <select id="annFallbackMode">
          <option value="text_only">只发文字</option>
          <option value="welcome_only">只发欢迎词</option>
          <option value="single_video_first_item">退化成第 1 个视频</option>
          <option value="none">不发送</option>
        </select>
        <label class="space-top">视频组（最多 4 个）</label>
        <div class="muted small">相册组模式至少需要 2 个视频。菜单会通过下一条短消息显示。</div>
        <div class="stack space-top">
          <div>
            <label><input id="annClearCache1" type="checkbox" /> 视频 1</label><input id="annMediaUrl1" placeholder="https://.../video1.mp4 或上传后自动回填" />
            <div class="actions space-top"><input id="annVideoFile1" type="file" accept="video/mp4,video/quicktime,video/x-m4v" /><button id="annUploadBtn1" class="secondary" onclick="uploadAnnouncementVideo(1)">上传视频 1</button></div>
            <div id="annCacheInfo1" class="muted small">缓存状态：待加载</div>
          </div>
          <div>
            <label><input id="annClearCache2" type="checkbox" /> 视频 2</label><input id="annMediaUrl2" placeholder="https://.../video2.mp4 或上传后自动回填" />
            <div class="actions space-top"><input id="annVideoFile2" type="file" accept="video/mp4,video/quicktime,video/x-m4v" /><button id="annUploadBtn2" class="secondary" onclick="uploadAnnouncementVideo(2)">上传视频 2</button></div>
            <div id="annCacheInfo2" class="muted small">缓存状态：待加载</div>
          </div>
          <div>
            <label><input id="annClearCache3" type="checkbox" /> 视频 3</label><input id="annMediaUrl3" placeholder="https://.../video3.mp4 或上传后自动回填" />
            <div class="actions space-top"><input id="annVideoFile3" type="file" accept="video/mp4,video/quicktime,video/x-m4v" /><button id="annUploadBtn3" class="secondary" onclick="uploadAnnouncementVideo(3)">上传视频 3</button></div>
            <div id="annCacheInfo3" class="muted small">缓存状态：待加载</div>
          </div>
          <div>
            <label><input id="annClearCache4" type="checkbox" /> 视频 4</label><input id="annMediaUrl4" placeholder="https://.../video4.mp4 或上传后自动回填" />
            <div class="actions space-top"><input id="annVideoFile4" type="file" accept="video/mp4,video/quicktime,video/x-m4v" /><button id="annUploadBtn4" class="secondary" onclick="uploadAnnouncementVideo(4)">上传视频 4</button></div>
            <div id="annCacheInfo4" class="muted small">缓存状态：待加载</div>
          </div>
        </div>
        <div id="announcementUploadBox" class="upload-progress-card" style="display:none">
          <div class="upload-progress-bar"><div id="announcementUploadFill" class="upload-progress-fill"></div></div>
          <div class="upload-progress-meta">
            <div id="announcementUploadPercent">0%</div>
            <div id="announcementUploadSize">0 B / 0 B</div>
          </div>
          <div id="announcementUploadTip" class="upload-progress-tip">等待上传…</div>
          <div id="announcementTelegramHint" class="upload-progress-tip">Telegram 预计：等待选择视频文件</div>
        </div>
        <label class="space-top">状态</label><select id="annEnabled"><option value="true">启用</option><option value="false">停用</option></select>
        <div class="actions space-top"><button class="green" onclick="saveAnnouncementConfig()">保存首次启动公告</button><button class="orange" onclick="runAnnouncementBroadcast()">机器人群发公告</button><button class="secondary" onclick="clearAnnouncementCache(false)">清除全部缓存</button><button class="secondary" onclick="clearAnnouncementCache(true)">清除选中缓存</button></div>
        <div id="announcementStatus" class="status">-</div>
      </div>
    </div>
    <div class="col-7">
      <div class="card">
        <div class="card-head"><div><div class="title">任务日志</div><div class="subtitle">记录公告加载、上传、保存、群发等任务状态，方便排查是否执行成功</div></div></div>
        <div class="stack">
          <div id="announcementCurrentPreview" class="status">当前公告摘要：未设置</div>
          <div id="announcementTaskLog" class="task-log-box"><div class="task-log-item"><span class="task-log-time">等待中</span>商城公告任务日志将在这里显示。</div></div>
        </div>
      </div>
    </div>
  </div>
</div>

  

<div id="section-folder-link" class="section">
  <div class="grid">
    <div class="col-5">
      <div class="card form-sticky">
        <div class="card-head"><div><div class="title">共享文件夹设置</div><div class="subtitle">为所有机器人统一展示“添加到文件夹”按钮，并检测共享文件夹链接是否失效</div></div></div>
        <label>启用状态</label>
        <select id="folderLinkEnabled"><option value="true">启用</option><option value="false">关闭</option></select>
        <label class="space-top">主按钮文案</label><input id="folderLinkPrimaryText" placeholder="添加到商城文件夹" />
        <label class="space-top">共享文件夹链接</label><input id="folderLinkUrl" placeholder="https://t.me/addlist/xxxxx" />
        <label class="space-top">适用机器人类型</label>
        <select id="folderLinkBotTypes" multiple size="3">
          <option value="buyer">商城机器人</option>
          <option value="shipping">供应链机器人</option>
          <option value="session">聚合聊天机器人</option>
        </select>
        <label class="space-top"><input id="folderLinkApplyAll" type="checkbox" checked /> 应用于所有机器人</label>
        <label class="space-top"><input id="folderLinkShowSettings" type="checkbox" checked /> 显示“打开文件夹设置”按钮</label>
        <label class="space-top">设置按钮文案</label><input id="folderSettingsText" placeholder="打开文件夹设置" />
        <label class="space-top">设置按钮链接</label><input id="folderSettingsUrl" placeholder="tg://settings/folders" />
        <label class="space-top"><input id="folderLinkShowManual" type="checkbox" checked /> 显示“如何手动加入机器人”按钮</label>
        <label class="space-top">手动提示按钮文案</label><input id="folderManualText" placeholder="如何手动加入机器人" />
        <label class="space-top">手动提示内容</label><textarea id="folderManualHint" placeholder="机器人私聊请在 Telegram 内手动加入文件夹或手动置顶。"></textarea>
        <label class="space-top">检测模式</label>
        <select id="folderCheckMode">
          <option value="weak">弱检测（默认可直接运行）</option>
          <option value="telegram_user_api">Telegram 用户 API 检测</option>
          <option value="none">不检测</option>
        </select>
        <label class="space-top">检测频率（分钟）</label><input id="folderCheckInterval" type="number" min="5" value="60" />
        <div class="actions space-top"><button class="green" onclick="saveFolderLinkConfig()">保存共享文件夹配置</button><button class="secondary" onclick="checkFolderLinkNow()">立即检测链接</button></div>
        <div id="folderLinkStatus" class="status">未加载共享文件夹配置。</div>
      </div>
    </div>
    <div class="col-7">
      <div class="card">
        <div class="card-head"><div><div class="title">运行态预览</div><div class="subtitle">下面展示运行时会下发给三个机器人的按钮配置和检测状态</div></div></div>
        <div class="stack">
          <div id="folderLinkRuntimePreview" class="status">未加载运行态预览。</div>
          <div id="folderLinkExplain" class="muted small">说明：共享文件夹可导入群组/频道，机器人私聊仍需用户手动加入文件夹或手动置顶。</div>
        </div>
      </div>
    </div>
  </div>
</div>

  <div id="section-admins" class="section">
    <div class="grid">
      <div class="col-4">
        <div class="card form-sticky">
          <div class="card-head"><div><div class="title">管理员表单</div><div class="subtitle">支持多账号、角色和启停管理</div></div><div class="chip" id="adminManageMode">仅读</div></div>
          <input id="adminUserId" type="hidden" />
          <label>账号</label><input id="adminUsername" placeholder="operator01" />
          <label class="space-top">显示名称</label><input id="adminDisplayName" placeholder="客服一号 / 运营主管" />
          <label class="space-top">角色</label><select id="adminRole"><option value="operator">普通管理员</option><option value="superadmin">超级管理员</option></select>
          <label class="space-top">状态</label><select id="adminIsActive"><option value="true">启用</option><option value="false">停用</option></select>
          <label class="space-top">登录密码</label><input id="adminPassword" type="password" placeholder="新增账号时必填；编辑留空则不改密码" />
          <div class="actions space-top"><button class="green" onclick="saveAdminUser()">保存管理员</button><button class="secondary" onclick="clearAdminUserForm()">清空</button></div>
          <div id="adminsStatus" class="status">只有超级管理员可以新增、停用、删除后台账号。</div>
        </div>
        <div class="card space-top">
          <div class="card-head"><div><div class="title">我的密码</div><div class="subtitle">当前登录账号可在这里修改自己的密码</div></div></div>
          <label>当前密码</label><input id="myCurrentPassword" type="password" placeholder="请输入当前密码" />
          <label class="space-top">新密码</label><input id="myNewPassword" type="password" placeholder="至少 6 位" />
          <div class="actions space-top"><button class="green" onclick="changeMyPassword()">修改我的密码</button></div>
          <div id="myPasswordStatus" class="status">修改完成后，下次登录请使用新密码。</div>
        </div>
      </div>
      <div class="col-8">
        <div class="card">
          <div class="card-head"><div><div class="title">管理员列表</div><div class="subtitle">推荐至少保留 1 个超级管理员 + 1 个日常运营账号</div></div><div class="actions"><button class="small secondary" onclick="loadAdminUsers()">刷新</button></div></div>
          <div id="adminsTable" class="table-wrap"></div>
        </div>
      </div>
    </div>
  </div>


  <div id="section-categories" class="section">
    <div class="grid">
      <div class="col-4">
        <div class="card form-sticky">
          <div class="card-head"><div><div class="title">分类表单</div><div class="subtitle">用于前台商品分区与导航入口</div></div></div>
          <input id="categoryId" type="hidden" />
          <label>分类名称</label><input id="categoryName" />
          <label class="space-top">封面图</label><input id="categoryCoverImage" />
          <label class="space-top">排序</label><input id="categorySortOrder" type="number" value="100" />
          <label class="space-top">状态</label><select id="categoryIsActive"><option value="true">启用</option><option value="false">停用</option></select>
          <div class="actions space-top"><button class="green" onclick="saveCategory()">保存分类</button><button class="secondary" onclick="clearCategoryForm()">清空</button></div>
          <div id="categoriesStatus" class="status">-</div>
        </div>
      </div>
      <div class="col-8"><div class="card"><div class="card-head"><div><div class="title">分类列表</div><div class="subtitle">启用中的分类会出现在商城机器人前台</div></div></div><div id="categoriesTable" class="table-wrap"></div></div></div>
    </div>
  </div>

  <div id="section-products" class="section">
    <div class="grid">
      <div class="col-5 products-left-col">
        <div class="card form-sticky product-form-card">
          <div class="card-head"><div><div class="title">商品表单</div><div class="subtitle">建议先录主图、价格、库存，再补详情内容</div></div></div>
          <input id="productId" type="hidden" />
          <label>分类</label><select id="productCategoryId"></select>
          <label class="space-top">商品名称</label><input id="productName" />
          <label class="space-top">副标题</label><input id="productSubtitle" />
          <label class="space-top">SKU</label><input id="productSkuCode" />
          <label class="space-top">封面图地址</label><input id="productCoverImage" />
          <div class="actions space-top"><input id="productImageFile" type="file" accept="image/*" /><button class="secondary" onclick="uploadProductImage()">上传图片</button></div>
          <div class="preview-wrap"><img id="productImagePreview" class="preview" src="" style="display:none" /><div id="productImagePlaceholder" class="muted">未上传商品图，建议尺寸 800×800。</div></div>
          <div class="grid space-top">
            <div class="col-6"><label>售价</label><input id="productPrice" type="number" step="0.01" value="0" /></div>
            <div class="col-6"><label>原价</label><input id="productOriginalPrice" type="number" step="0.01" value="0" /></div>
            <div class="col-6"><label>库存</label><input id="productStockQty" type="number" value="0" /></div>
            <div class="col-6"><label>重量(g)</label><input id="productWeightGram" type="number" value="0" /></div>
            <div class="col-6"><label>单位</label><input id="productUnitText" value="件" /></div>
            <div class="col-6"><label>排序</label><input id="productSortOrder" type="number" value="100" /></div>
          </div>
          <label class="space-top">描述</label><textarea id="productDescription"></textarea>
          <label class="space-top">详情</label><textarea id="productDetailHtml"></textarea>
          <label class="space-top">上架状态</label><select id="productIsActive"><option value="true">上架</option><option value="false">下架</option></select>
          <div class="card space-top">
            <div class="card-head"><div><div class="title">SKU 列表</div><div class="subtitle">一个商品可配置多个规格</div></div><div class="chip">多SKU</div></div>
            <div class="actions"><button type="button" class="secondary" onclick="addSkuRow()">新增 SKU</button></div>
            <div id="productSkuList" class="space-top"></div>
          </div>
          <div class="actions space-top"><button id="saveProductBtn" type="button" class="green" onclick="saveProduct()">保存商品</button><button type="button" class="secondary" onclick="clearProductForm()">清空</button></div>
          <div id="productsStatus" class="status">-</div>
        </div>
        <div class="card product-import-card">
          <div class="card-head"><div><div class="title">表格模板上传商品</div><div class="subtitle">支持下载模板、预览校验、正式导入、失败行二次导入</div></div><div class="chip">放大页面也不丢</div></div>
          <label>操作人</label><input id="productImportOperatorName" placeholder="admin_ui" value="admin_ui" />
          <label class="space-top">选择文件</label><input id="productImportFile" type="file" accept=".xlsx,.xls" />
          <label class="space-top">图片链接可达性检测</label><select id="productImportCheckImages"><option value="false">否</option><option value="true">是</option></select>
          <div class="actions space-top"><button class="secondary" onclick="downloadProductImportTemplate()">下载模板</button><button onclick="previewProductImportFile()">预览校验</button><button class="green" onclick="uploadProductImportFile()">正式导入</button></div>
          <div id="productImportStatus" class="status">-</div>
          <div class="space-top" id="productImportPreviewTable"></div>
        </div>
      </div>
      <div class="col-7 products-right-col">
        <div class="card">
          <div class="card-head"><div><div class="title">商品列表</div><div class="subtitle">支持按搜索词、分类、状态筛选</div></div><div class="chip">更适合桌面运营</div></div>
          <div class="grid">
            <div class="col-4"><label>搜索</label><input id="productSearchKeyword" placeholder="名称 / 副标题 / SKU" oninput="renderProductsTable()" /></div>
            <div class="col-4"><label>分类</label><select id="productSearchCategory" onchange="renderProductsTable()"></select></div>
            <div class="col-4"><label>上架状态</label><select id="productSearchStatus" onchange="renderProductsTable()"><option value="all">全部</option><option value="active">已上架</option><option value="inactive">已下架</option></select></div>
          </div>
          <div id="productsTable" class="space-top table-wrap"></div>
        </div>
        <div class="card space-top">
          <div class="card-head"><div><div class="title">商品导入历史</div><div class="subtitle">查看批次、失败行、失败行二次导入</div></div></div>
          <div class="grid">
            <div class="col-6"><label>关键字</label><input id="productImportBatchKeyword" placeholder="批次号 / 文件名 / 操作人" /></div>
            <div class="col-3"><label>结果</label><select id="productImportBatchStatus"><option value="all">全部</option><option value="success">仅成功</option><option value="failed">有失败</option></select></div>
            <div class="col-3"><label>&nbsp;</label><button class="secondary" onclick="loadProductImportBatches()">刷新批次</button></div>
          </div>
          <div id="productImportBatchTable" class="space-top table-wrap"></div>
          <div class="space-top" id="productImportErrorsTable"></div>
        </div>
      </div>
    </div>
  </div>

  <div id="section-orders" class="section">
    <div class="grid">
      <div class="col-12">
        <div class="card">
          <div class="card-head"><div><div class="title">订单列表</div><div class="subtitle">按订单号、收件人、手机号、单号快速检索</div></div></div>
          <div class="grid">
            <div class="col-4"><label>搜索</label><input id="orderSearchKeyword" placeholder="订单号 / 收件人 / 手机 / 单号" oninput="debounceOrdersRefresh()" /></div>
            <div class="col-3"><label>支付状态</label><select id="orderSearchPayStatus" onchange="refreshOrders(true)"><option value="all">全部</option><option value="pending">待支付</option><option value="paid">已支付</option><option value="failed">支付失败</option></select></div>
            <div class="col-3"><label>发货状态</label><select id="orderSearchDeliveryStatus" onchange="refreshOrders(true)"><option value="all">全部</option><option value="not_shipped">待发货</option><option value="shipped">已发货</option><option value="signed">已签收</option></select></div>
            <div class="col-2"><label>供应链</label><select id="orderSearchSupplier" onchange="refreshOrders(true)"><option value="all">全部</option></select></div>
            <div class="col-1"><label>&nbsp;</label><button class="secondary" onclick="refreshOrders(true)">刷新</button></div>
          </div>
          <div id="ordersTable" class="space-top table-wrap"></div>
          <div id="ordersStatus" class="status">-</div>
        </div>
      </div>
      <div class="col-12">
        <div class="card">
          <div class="card-head"><div><div class="title">订单详情</div><div class="subtitle">查看支付单信息、商品明细、录入发货与备注</div></div></div>
          <div id="orderDetailEmpty" class="muted">请在上方列表点击“详情”。</div>
          <div id="orderDetailBox" style="display:none">
            <div class="grid">
              <div class="col-6"><div id="orderDetailBasic"></div></div>
              <div class="col-6"><div id="orderDetailPayment"></div></div>
            </div>
            <div class="divider"></div>
            <div id="orderDetailItems"></div>
            <div id="orderDetailShipment" class="space-top"></div>
            <div class="divider"></div>
            <div class="grid">
              <div class="col-6"><label>供应链指派</label><div class="actions"><select id="orderAssignSupplier" style="min-width:220px"></select><button class="secondary" onclick="assignOrderSupplier()">手动指派</button><button class="secondary" onclick="autoAssignOrderSupplier()">自动重算</button></div></div>
              <div class="col-6"><div id="orderFulfillmentInfo" class="muted">未分配供应链</div></div>
            </div>
            <div class="grid">
              <div class="col-4"><label>快递公司</label><input id="shipCourierCompany" /></div>
              <div class="col-4"><label>快递编码</label><input id="shipCourierCode" /></div>
              <div class="col-4"><label>快递单号</label><input id="shipTrackingNo" /></div>
            </div>
            <label class="space-top">卖家备注</label><textarea id="orderSellerRemark"></textarea>
            <div class="actions space-top">
              <button class="green" onclick="markOrderPaid()">标记已支付</button>
              <button class="secondary" onclick="simulateOrderPaid()">模拟支付成功</button>
              <button class="orange" onclick="shipOrder()">录入发货</button>
              <button class="secondary" onclick="saveOrderRemark()">保存备注</button>
              <button class="secondary" onclick="syncOrderLogistics()">同步物流</button>
              <button class="secondary" onclick="pushOrderToSupplier()">推送供应链</button>
              <button class="secondary" onclick="pullSupplierStatus()">拉取履约状态</button>
              <button class="secondary" onclick="previewSupplierPayload()">查看推单载荷</button>
              <button class="secondary" onclick="completeOrder()">标记完成</button>
              <button class="red" onclick="cancelOrder()">取消订单</button>
            </div>
            <div id="orderDetailStatus" class="status">-</div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div id="section-payment" class="section">
    <div class="grid">
      <div class="col-5">
        <div class="card form-sticky">
          <div class="card-head"><div><div class="title">支付地址表单</div><div class="subtitle">建议先上传二维码，再保存地址与标签</div></div></div>
          <input id="paymentAddressId" type="hidden" />
          <label>标签</label><input id="paymentAddressLabel" />
          <label class="space-top">收款地址</label><textarea id="paymentAddressValue"></textarea>
          <label class="space-top">二维码地址</label><input id="paymentQrImage" />
          <div class="actions space-top"><input id="paymentQrFile" type="file" accept="image/*" /><button class="secondary" onclick="uploadPaymentQr()">上传二维码</button></div>
          <div class="preview-wrap"><img id="paymentQrPreview" class="preview" src="" style="display:none" /><div id="paymentQrPlaceholder" class="muted">未上传二维码图片，建议使用方形 PNG。</div></div>
          <div class="grid space-top">
            <div class="col-6"><label>排序</label><input id="paymentSortOrder" type="number" value="100" /></div>
            <div class="col-6"><label>状态</label><select id="paymentIsActive"><option value="true">启用</option><option value="false">停用</option></select></div>
          </div>
          <div class="actions space-top"><button class="green" onclick="savePaymentAddress()">保存地址</button><button class="secondary" onclick="clearPaymentAddressForm()">清空</button></div>
          <div id="paymentStatus" class="status">-</div>
        </div>
      </div>
      <div class="col-7"><div class="card"><div class="card-head"><div><div class="title">支付地址列表</div><div class="subtitle">仅启用中的地址会用于新订单分配</div></div></div><div id="paymentAddressesTable" class="table-wrap"></div></div></div>
    </div>
  </div>


  <div id="section-logistics" class="section">
    <div class="card">
      <div class="card-head"><div><div class="title">物流中心</div><div class="subtitle">查询物流、同步物流、查看最近轨迹</div></div></div>
      <div class="grid">
        <div class="col-4"><label>搜索</label><input id="logisticsSearchKeyword" placeholder="订单号 / 单号 / 快递公司" oninput="renderLogisticsTable()" /></div>
        <div class="col-3"><label>供应链</label><select id="logisticsSearchSupplier" onchange="onLogisticsSupplierChange()"></select></div>
        <div class="col-3"><label>物流状态</label><select id="logisticsSearchStatus" onchange="renderLogisticsTable()"><option value="all">全部</option><option value="pending">待同步</option><option value="shipped">运输中</option><option value="signed">已签收</option><option value="error">异常</option></select></div>
        <div class="col-2"><label>&nbsp;</label><button class="secondary" onclick="loadLogisticsCenter()">刷新物流</button></div>
      </div>
      <div id="logisticsTable" class="space-top table-wrap"></div>
      <div id="logisticsStatus" class="status">-</div>
    </div>
    <div class="card space-top">
      <div class="card-head"><div><div class="title">物流预警中心</div><div class="subtitle">找回之前的预警区：已支付未发货超时、首轨迹超时、轨迹停滞、同步异常、异常件</div></div><div class="actions"><button class="secondary small" onclick="exportLogisticsAlertsTemplate()">导出核对表</button><button class="secondary small" onclick="loadLogisticsAlertsCenter()">刷新预警</button></div></div>
      <div class="metric-grid">
        <div class="metric-card"><div class="metric-label">预警总数</div><div class="kpi" id="logisticsAlertTotal">0</div><div class="metric-tip">当前筛选条件下的预警条数</div></div>
        <div class="metric-card"><div class="metric-label">红色预警</div><div class="kpi" id="logisticsAlertRed">0</div><div class="metric-tip">异常件 / 严重停滞 / 超时严重</div></div>
        <div class="metric-card"><div class="metric-label">橙色预警</div><div class="kpi" id="logisticsAlertOrange">0</div><div class="metric-tip">物流同步异常、48h 停滞等</div></div>
        <div class="metric-card"><div class="metric-label">黄色预警</div><div class="kpi" id="logisticsAlertYellow">0</div><div class="metric-tip">已支付未发货 6h+、首轨迹超时</div></div>
      </div>
      <div class="grid space-top">
        <div class="col-3"><label>预警等级</label><select id="logisticsAlertLevel" onchange="loadLogisticsAlertsCenter()"><option value="all">全部</option><option value="red">红色</option><option value="orange">橙色</option><option value="yellow">黄色</option></select></div>
        <div class="col-9"><label>说明</label><div class="status" style="margin-top:0">供应链筛选与上方物流中心联动。切换供应链后，会同时刷新物流表和预警中心。</div></div>
      </div>
      <div id="logisticsAlertsTable" class="space-top table-wrap"></div>
      <div id="logisticsAlertsStatus" class="status">-</div>
    </div>
    <div class="card space-top">
      <div class="card-head"><div><div class="title">物流 / API 接口对接项</div><div class="subtitle">查看当前物流 provider 与各供应链 API 对接配置</div></div></div>
      <div id="logisticsApiOverview" class="table-wrap"></div>
      <div id="logisticsApiStatus" class="status">-</div>
    </div>
  </div>

  <div id="section-shipping" class="section">
    <div class="grid">
      <div class="col-12">
        <div class="metric-grid">
          <div class="metric-card"><div class="metric-label">待发货总数</div><div class="kpi" id="pendingShipCount">0</div><div class="metric-tip">已支付且未发货</div></div>
          <div class="metric-card"><div class="metric-label">已发货总数</div><div class="kpi" id="shippedCount">0</div><div class="metric-tip">已录入快递单号</div></div>
          <div class="metric-card"><div class="metric-label">导入批次数</div><div class="kpi" id="importBatchCount">0</div><div class="metric-tip">已发货模板导入批次</div></div>
          <div class="metric-card"><div class="metric-label">导入失败行</div><div class="kpi" id="importFailedCount">0</div><div class="metric-tip">便于客服补录单号</div></div>
        </div>
      </div>
      <div class="col-5">
        <div class="card form-sticky">
          <div class="card-head"><div><div class="title">已发货模板导入</div><div class="subtitle">按模板批量回录昨日已发货信息</div></div></div>
          <label>供应链编码（可空）</label><input id="shipmentImportSupplierCode" placeholder="A / B" />
          <label class="space-top">业务日期（已发货导出）</label><input id="shipmentBizDate" type="date" />
          <label class="space-top">选择文件</label><input id="shipmentImportFile" type="file" accept=".xlsx,.xls" />
          <div class="actions space-top">
            <button class="secondary" onclick="downloadShipmentTemplate()">下载模板</button>
            <button class="secondary" onclick="downloadCurrentSupplierTemplateSample()">下载当前供应链样例</button>
            <button class="green" onclick="uploadShipmentImport()">上传导入</button>
            <button onclick="exportPendingShipments()">导出待发货</button>
            <button class="orange" onclick="exportShippedShipments()">导出已发货</button>
          </div>
          <div id="shippingStatus" class="status">-</div>
        </div>
      </div>
      <div class="col-7">
        <div class="card">
          <div class="card-head"><div><div class="title">最近导入批次</div><div class="subtitle">查看每批成功/失败情况</div></div></div>
          <div id="shipmentImportBatchesTable" class="table-wrap"></div>
        </div>
      </div>
      <div class="col-12">
        <div class="card">
          <div class="card-head"><div><div class="title">待发货 / 已发货预览</div><div class="subtitle">便于仓库和客服快速核对</div></div></div>
          <div class="grid">
            <div class="col-6"><div id="pendingShippingPreview" class="table-wrap"></div></div>
            <div class="col-6"><div id="shippedPreview" class="table-wrap"></div></div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div id="section-suppliers" class="section">
    <div class="grid">
      <div class="col-4">
        <div class="card form-sticky">
          <div class="card-head"><div><div class="title">供应链表单</div><div class="subtitle">A / B 供应链配置与供应链机器人绑定</div></div></div>
          <input id="supplierId" type="hidden" />
          <label>供应链编码</label><input id="supplierCode" placeholder="A" />
          <label class="space-top">供应链名称</label><input id="supplierName" placeholder="供应链 A" />
          <label class="space-top">类型</label><select id="supplierType"><option value="manual">人工</option><option value="api">API</option></select>
          <label class="space-top">API 地址</label><input id="supplierApiBase" placeholder="https://api.supplier-a.com" />
          <label class="space-top">API Key</label><input id="supplierApiKey" />
          <label class="space-top">API Secret</label><input id="supplierApiSecret" />
          <label class="space-top">供应链机器人编码</label><input id="supplierShippingBotCode" placeholder="soso001" />
          <label class="space-top">模板类型</label><select id="supplierTemplateType"><option value="standard">standard</option><option value="supplier_a">supplier_a</option><option value="supplier_b">supplier_b</option></select>
          <label class="space-top">联系人</label><input id="supplierContactName" />
          <label class="space-top">联系电话</label><input id="supplierContactPhone" />
          <label class="space-top">联系 TG</label><input id="supplierContactTg" />
          <label class="space-top">备注</label><textarea id="supplierRemark"></textarea>
          <label class="space-top">状态</label><select id="supplierIsActive"><option value="true">启用</option><option value="false">停用</option></select>
          <div class="actions space-top"><button class="green" onclick="saveSupplier()">保存供应链</button><button class="secondary" onclick="clearSupplierForm()">清空</button></div>
          <div id="suppliersStatus" class="status">-</div>
        </div>
      </div>
      <div class="col-8">
        <div class="card">
          <div class="card-head"><div><div class="title">供应链列表</div><div class="subtitle">每个供应链可绑定一个供应链机器人</div></div></div>
          <div id="suppliersTable" class="table-wrap"></div>
        </div>
        <div class="card">
          <div class="card-head"><div><div class="title">商品绑定供应链</div><div class="subtitle">决定订单默认分配给哪个供应链</div></div></div>
          <div class="grid">
            <div class="col-3"><label>商品</label><select id="psmProductId"></select></div>
            <div class="col-3"><label>供应链</label><select id="psmSupplierId"></select></div>
            <div class="col-2"><label>供应链 SKU</label><input id="psmSupplierSku" /></div>
            <div class="col-2"><label>优先级</label><input id="psmPriority" type="number" value="100" /></div>
            <div class="col-2"><label>默认</label><select id="psmIsDefault"><option value="true">是</option><option value="false">否</option></select></div>
          </div>
          <div class="actions space-top"><button class="green" onclick="saveProductSupplierMap()">保存绑定</button></div>
          <div id="productSupplierMapStatus" class="status">-</div>
          <div id="productSupplierMapTable" class="space-top table-wrap"></div>
        </div>
        <div class="card">
          <div class="card-head"><div><div class="title">订单履约分配</div><div class="subtitle">查看订单已分配到哪个供应链</div></div></div>
          <div id="orderFulfillmentsTable" class="table-wrap"></div>
        </div>
      </div>
    </div>
  </div>
  <div id="section-data-center" class="section">
    <div class="grid">
      <div class="col-12">
        <div class="card">
          <div class="card-head"><div><div class="title">数据中心</div><div class="subtitle">结合供应链查看 GMV、发货、签收、异常与商品销量</div></div></div>
          <div class="grid">
            <div class="col-3"><label>时间窗口</label><select id="dcDays"><option value="7">近 7 天</option><option value="15">近 15 天</option><option value="30" selected>近 30 天</option><option value="60">近 60 天</option><option value="90">近 90 天</option></select></div>
            <div class="col-4"><label>供应链</label><select id="dcSupplierCode"><option value="">全部供应链</option></select></div>
            <div class="col-5"><label>操作</label><div class="actions"><button class="green" onclick="loadDataCenter()">刷新数据中心</button><button class="secondary" onclick="exportDataCenterSupplierBoard()">导出供应链排行</button><button class="secondary" onclick="exportDataCenterProductRanking()">导出商品销量榜</button></div></div>
          </div>
          <div id="dataCenterStatus" class="status">-</div>
        </div>
      </div>
    </div>

    <div class="metric-grid space-top">
      <div class="metric-card"><div class="metric-label">支付 GMV</div><div class="kpi" id="dcPaidGmv">¥0</div><div class="metric-tip" id="dcPaidGmvTip">-</div></div>
      <div class="metric-card"><div class="metric-label">已支付订单</div><div class="kpi" id="dcPaidOrders">0</div><div class="metric-tip" id="dcPaidOrdersTip">-</div></div>
      <div class="metric-card"><div class="metric-label">已发货订单</div><div class="kpi" id="dcShippedOrders">0</div><div class="metric-tip" id="dcShippedOrdersTip">-</div></div>
      <div class="metric-card"><div class="metric-label">签收率</div><div class="kpi" id="dcSignRate">0%</div><div class="metric-tip" id="dcSignRateTip">-</div></div>
      <div class="metric-card"><div class="metric-label">待发货订单</div><div class="kpi" id="dcPendingShip">0</div><div class="metric-tip" id="dcPendingShipTip">-</div></div>
      <div class="metric-card"><div class="metric-label">平均发货时效</div><div class="kpi" id="dcAvgShipHours">0h</div><div class="metric-tip" id="dcAvgShipHoursTip">-</div></div>
      <div class="metric-card"><div class="metric-label">同步异常</div><div class="kpi" id="dcSyncAbnormal">0</div><div class="metric-tip" id="dcSyncAbnormalTip">-</div></div>
      <div class="metric-card"><div class="metric-label">停滞物流</div><div class="kpi" id="dcStagnantCount">0</div><div class="metric-tip" id="dcStagnantCountTip">-</div></div>
    </div>

    <div class="grid space-top">
      <div class="col-12">
        <div class="card soft">
          <div class="card-head"><div><div class="title">本期 vs 上期</div><div class="subtitle">同样天数窗口对比，帮助看出供应链是在变好还是变差</div></div></div>
          <div id="dcCompareSummary" class="muted">-</div>
        </div>
      </div>
      <div class="col-12">
        <div class="card">
          <div class="card-head"><div><div class="title">供应链排行榜</div><div class="subtitle">按 GMV、发货率、签收率、异常与停滞物流综合查看</div></div></div>
          <div id="dcSupplierBoard" class="table-wrap"></div>
        </div>
      </div>
      <div class="col-6">
        <div class="card">
          <div class="card-head"><div><div class="title">订单漏斗</div><div class="subtitle">创建、支付、待发货、发货、签收的转化链路</div></div></div>
          <div id="dcFunnel" class="table-wrap"></div>
        </div>
      </div>
      <div class="col-6">
        <div class="card">
          <div class="card-head"><div><div class="title">商品销量榜</div><div class="subtitle">按 GMV 与销量查看当前窗口最值得关注的商品</div></div></div>
          <div id="dcProductRanking" class="table-wrap"></div>
        </div>
      </div>
      <div class="col-6">
        <div class="card">
          <div class="card-head"><div><div class="title">每日趋势</div><div class="subtitle">订单、支付、GMV、发货、签收的日趋势</div></div></div>
          <div id="dcTrend" class="table-wrap"></div>
        </div>
      </div>
      <div class="col-6">
        <div class="card">
          <div class="card-head"><div><div class="title">供应链异常趋势</div><div class="subtitle">同步异常、停滞物流、异常件按日汇总</div></div></div>
          <div id="dcAlertsTrend" class="table-wrap"></div>
        </div>
      </div>
      <div class="col-12">
        <div class="card">
          <div class="card-head"><div><div class="title">分类 × 供应链</div><div class="subtitle">看哪个分类主要由哪条供应链承接，以及对应 GMV 与销量</div></div></div>
          <div id="dcCategorySupplierBoard" class="table-wrap"></div>
        </div>
      </div>
    </div>
  </div>

</div>
<div id="botSaveToast" class="bot-save-toast success">
  <div id="botSaveToastTitle" class="title">保存成功</div>
  <div id="botSaveToastBody">Bot 配置已保存。</div>
</div>
<script>
  const API_BASE = '';
  let BOTS = []; let BOT_RUNTIME = []; let BOT_AUTO_SYNC_STATUS = null; let ANNOUNCEMENT_CONFIG = null; let ADMIN_USERS = []; let ADMIN_AUTH = {configured:false, authorized:true, username:'', display_name:'', role:'', is_superadmin:false}; let CATEGORIES = []; let PRODUCTS = []; let ORDERS = []; let ORDER_PAGE = {page:1,page_size:20,total:0,total_pages:1,has_prev:false,has_next:false}; let PAYMENT_ADDRESSES = []; let CURRENT_ORDER = null; let SUPPLIERS = []; let PRODUCT_SUPPLIER_MAPS = []; let ORDER_FULFILLMENTS = []; let LOGISTICS_ROWS = []; let LOGISTICS_ALERTS = {overview:{total:0,by_level:{yellow:0,orange:0,red:0},by_type:{}}, rows:[]}; let SHIPMENT_IMPORT_BATCHES = []; let SHIPPING_PENDING = []; let SHIPPING_SHIPPED = []; let CHAT_OVERVIEW = null; let CHAT_SESSIONS = []; let CHAT_SESSION_PAGE = {page:1,page_size:30,total:0,total_pages:1,has_prev:false,has_next:false}; let CURRENT_CHAT_SESSION = null; let DATA_CENTER_OVERVIEW = null; let DATA_CENTER_SUPPLIER_BOARD = []; let DATA_CENTER_TREND = []; let DATA_CENTER_CATEGORY_BOARD = []; let DATA_CENTER_PRODUCT_RANKING = []; let DATA_CENTER_FUNNEL = []; let DATA_CENTER_ALERTS_TREND = []; let CHAT_KEYWORD_BLOCKS = []; let CHAT_KEYWORD_EFFECTIVE = null; let PRODUCT_IMPORT_PREVIEW = null; let PRODUCT_IMPORT_BATCHES = []; let PRODUCT_IMPORT_ERRORS = []; let LOGISTICS_API_OVERVIEW = null; let ORDER_REFRESH_TIMER = null; let CHAT_REFRESH_TIMER = null;
  const $ = id => document.getElementById(id);
  const ADMIN_UI_TABS = ['dashboard','bots','sessions','announcements','folder-link','admins','categories','products','orders','logistics','shipping','suppliers','data-center','payment'];
  const ADMIN_TAB_STORAGE_KEY = 'goodsmall_admin_active_tab';
  function getApiBase(){ return API_BASE || ''; }
  function normalizeAdminTab(name){ return ADMIN_UI_TABS.includes(String(name || '').trim()) ? String(name || '').trim() : 'dashboard'; }
  function findTabButton(name){ return document.querySelector('.tab-btn[data-tab="'+normalizeAdminTab(name)+'"]'); }
  function readRequestedTab(){
    const hash = String(window.location.hash || '').replace(/^#/, '').trim();
    if(ADMIN_UI_TABS.includes(hash)){ return hash; }
    const params = new URLSearchParams(window.location.search || '');
    const queryTab = String(params.get('tab') || '').trim();
    if(ADMIN_UI_TABS.includes(queryTab)){ return queryTab; }
    try{
      const saved = String(localStorage.getItem(ADMIN_TAB_STORAGE_KEY) || '').trim();
      if(ADMIN_UI_TABS.includes(saved)){ return saved; }
    }catch(_){ }
    return 'dashboard';
  }
  function persistAdminTab(name){
    const tab = normalizeAdminTab(name);
    try{ localStorage.setItem(ADMIN_TAB_STORAGE_KEY, tab); }catch(_){ }
    const next = window.location.pathname + window.location.search + '#' + tab;
    if((window.location.pathname + window.location.search + window.location.hash) !== next){
      history.pushState({tab}, '', next);
    }
  }
  function activateTabUi(name, btn){
    const tab = normalizeAdminTab(name);
    document.querySelectorAll('.section').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
    const section = $('section-'+tab);
    if(section){ section.classList.add('active'); }
    const tabBtn = btn || findTabButton(tab);
    if(tabBtn){ tabBtn.classList.add('active'); }
  }
  function showTab(name, btn, options){
    const tab = normalizeAdminTab(name);
    activateTabUi(tab, btn);
    if(!(options && options.persist===false)){ persistAdminTab(tab); }
    if(tab==='bots'){ loadBots(); }
    if(tab==='sessions'){ loadChatCenter(); }
    if(tab==='announcements'){ loadAnnouncements(); }
    if(tab==='admins'){ loadAdminUsers(); }
    if(tab==='orders'){ refreshOrders(); }
    if(tab==='payment'){ loadPaymentAddresses(); }
    if(tab==='logistics'){ loadLogisticsCenter(); }
    if(tab==='shipping'){ loadShippingCenter(); }
    if(tab==='suppliers'){ loadSuppliersCenter(); }
    if(tab==='data-center'){ loadDataCenter(); }
  }
  function setStatus(id, text){ $(id).textContent = text; }
  function encodeRowPayload(row){
    try{
      return btoa(unescape(encodeURIComponent(JSON.stringify(row || {}))));
    }catch(e){
      console.error('encodeRowPayload failed', e, row);
      return '';
    }
  }
  function decodeRowPayload(raw){
    try{
      return JSON.parse(decodeURIComponent(escape(atob(String(raw || '')))));
    }catch(e){
      console.error('decodeRowPayload failed', e, raw);
      return {};
    }
  }
  function encodeTextPayload(value){
    try{
      return btoa(unescape(encodeURIComponent(String(value || ''))));
    }catch(e){
      console.error('encodeTextPayload failed', e, value);
      return '';
    }
  }
  function decodeTextPayload(raw){
    try{
      return decodeURIComponent(escape(atob(String(raw || ''))));
    }catch(e){
      console.error('decodeTextPayload failed', e, raw);
      return '';
    }
  }
  let BOT_SAVE_TOAST_TIMER = null;
  function showBotSaveToast(title, message, isError){
    const box = $('botSaveToast');
    const titleEl = $('botSaveToastTitle');
    const bodyEl = $('botSaveToastBody');
    if(!box || !titleEl || !bodyEl) return;
    box.className = 'bot-save-toast ' + (isError ? 'error' : 'success');
    titleEl.textContent = String(title || (isError ? '保存失败' : '保存成功'));
    bodyEl.textContent = String(message || '');
    box.style.display = 'block';
    clearTimeout(BOT_SAVE_TOAST_TIMER);
    BOT_SAVE_TOAST_TIMER = setTimeout(function(){ box.style.display = 'none'; }, 4200);
  }
  function escapeHtml(v){ return String(v ?? '').replace(/[&<>"']/g, s => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[s])); }
  function boolTag(v){ return v ? '<span class="tag ok">启用</span>' : '<span class="tag off">停用</span>'; }
  function payTag(v){ if(v==='paid') return '<span class="tag ok">已支付</span>'; if(v==='failed') return '<span class="tag off">支付失败</span>'; return '<span class="tag warn">待支付</span>'; }
  function deliveryTag(v){ if(v==='signed') return '<span class="tag ok">已签收</span>'; if(v==='shipped') return '<span class="tag warn">已发货</span>'; return '<span class="tag off">待发货</span>'; }
  function formatTime(v){ if(!v) return '-'; try{ const s=String(v).replace(' ','T'); const d=new Date(s.endsWith('Z')||s.includes('+')?s:(s+'Z')); return isNaN(d.getTime())?v:d.toLocaleString(); }catch(_){ return v; } }
  function todayText(offsetDays=0){ const d=new Date(); d.setDate(d.getDate()+Number(offsetDays||0)); const y=d.getFullYear(); const m=String(d.getMonth()+1).padStart(2,'0'); const day=String(d.getDate()).padStart(2,'0'); return `${y}-${m}-${day}`; }
  function buildAdminHeaders(extra){ return Object.assign({}, extra||{}); }
  function openAdminPath(path, target='_blank'){ const url = getApiBase()+path; if(target === '_self'){ window.location.href = url; return; } window.open(url, target); }
  async function logoutAdmin(){ try{ await fetch(getApiBase()+'/admin/logout', {method:'POST', headers:{'Accept':'application/json'}}); }catch(_){ } window.location.href = getApiBase() + '/admin/login'; }
  async function ensureAdminAuth(_forcePrompt=false){ const configuredRes = await fetch(getApiBase()+'/admin/auth/check', {headers:{'Accept':'application/json'}}); let data = {configured:false, authorized:true, username:'', display_name:'', role:'', is_superadmin:false}; try{ data = await configuredRes.json(); }catch(_){ } ADMIN_AUTH = Object.assign({configured:false, authorized:true, username:'', display_name:'', role:'', is_superadmin:false}, data||{}); if(!ADMIN_AUTH.configured){ if($('adminIdentityBadge')) $('adminIdentityBadge').textContent = '开放模式'; return true; } if(ADMIN_AUTH.authorized){ const badgeName = ADMIN_AUTH.display_name || ADMIN_AUTH.username || ''; const roleText = ADMIN_AUTH.is_superadmin ? '超级管理员' : '普通管理员'; if(badgeName && $('adminIdentityBadge')) $('adminIdentityBadge').textContent = '已登录 · ' + badgeName + (ADMIN_AUTH.username && ADMIN_AUTH.display_name && ADMIN_AUTH.display_name!==ADMIN_AUTH.username ? ' @'+ADMIN_AUTH.username : '') + ' · ' + roleText; return true; } window.location.href = getApiBase() + '/admin/login?next=' + encodeURIComponent('/admin/ui'); throw new Error('后台未登录'); }
  async function parseRes(res, path){ if(!res.ok){ let msg = path + ' -> ' + res.status; try{ const data = await res.json(); if(Array.isArray(data?.detail)){ msg = data.detail.map(x=>x.msg||JSON.stringify(x)).join('；'); } else if(data?.detail){ msg = data.detail; } }catch(_){ } if(res.status===401){ window.location.href = getApiBase() + '/admin/login?next=' + encodeURIComponent('/admin/ui'); } throw new Error(msg); } const contentType = String(res.headers.get('content-type') || ''); if(contentType.includes('application/json')) return res.json(); return {}; }
  async function apiGet(path, params){ await ensureAdminAuth(false); const url = new URL(getApiBase()+path, window.location.origin); Object.entries(params||{}).forEach(([k,v]) => { if(v!==undefined && v!==null && String(v)!=='') url.searchParams.set(k, String(v)); }); const res = await fetch(url.toString(), {headers:buildAdminHeaders({'Accept':'application/json'})}); return parseRes(res, path); }
  async function apiPost(path, data){ await ensureAdminAuth(false); const res = await fetch(getApiBase()+path, {method:'POST', headers:buildAdminHeaders({'Content-Type':'application/json','Accept':'application/json'}), body: JSON.stringify(data||{})}); return parseRes(res, path); }
  async function apiDelete(path){ await ensureAdminAuth(false); const res = await fetch(getApiBase()+path, {method:'DELETE', headers:buildAdminHeaders({'Accept':'application/json'})}); return parseRes(res, path); }
  async function apiUpload(path, file){ await ensureAdminAuth(false); const fd = new FormData(); fd.append('file', file); const res = await fetch(getApiBase()+path, {method:'POST', headers:buildAdminHeaders({}), body: fd}); return parseRes(res, path); }
  async function apiFormPost(path, formData){ await ensureAdminAuth(false); const res = await fetch(getApiBase()+path, {method:'POST', headers:buildAdminHeaders({}), body: formData}); return parseRes(res, path); }
  function normalizeMediaUrl(url){
    const raw = String(url || '').trim();
    if(!raw) return '';
    if(raw.startsWith('/')) return getApiBase() + raw;
    try{
      const parsed = new URL(raw, window.location.origin);
      if((parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1') && parsed.pathname.startsWith('/static/uploads/')){
        return getApiBase() + parsed.pathname;
      }
      return parsed.href;
    }catch(_){
      return raw;
    }
  }
  function previewImage(url, imgId='productImagePreview', placeholderId='productImagePlaceholder'){ const img=$(imgId), ph=$(placeholderId); const finalUrl = normalizeMediaUrl(url); if(finalUrl){ img.src=finalUrl; img.style.display='block'; ph.style.display='none'; } else { img.style.display='none'; ph.style.display='block'; } }
  function refreshOverview(){ $('kpiCategories').textContent = CATEGORIES.length; $('kpiProducts').textContent = PRODUCTS.length; $('kpiOrders').textContent = ORDERS.length; $('kpiPaymentAddresses').textContent = PAYMENT_ADDRESSES.length; if($('kpiBotsEnabled')) $('kpiBotsEnabled').textContent = BOTS.filter(x=>!!x.is_enabled).length; if($('kpiBotsRunning')) $('kpiBotsRunning').textContent = BOT_RUNTIME.filter(x=>x.run_status==='running').length; }


// Announcements
function selectedAnnouncementBotTypes(){
  const el = $('annTargetBotTypes');
  return el ? Array.from(el.options).filter(x => x.selected).map(x => x.value) : [];
}

function announcementMediaItems(){
  const rows = [];
  for(let i=1;i<=4;i++){
    const input = $(`annMediaUrl${i}`);
    const value = String(input?.value || '').trim();
    if(!value) continue;
    rows.push({
      type:'video',
      url:value,
      source_url:String(input?.dataset?.sourceUrl || value).trim(),
      normalized_url:String(input?.dataset?.normalizedUrl || value).trim(),
      sort:i,
      enabled:true
    });
  }
  return rows;
}

function announcementCacheMap(){
  const map = new Map();
  const cacheRows = Array.isArray(ANNOUNCEMENT_CONFIG?.media_cache) ? ANNOUNCEMENT_CONFIG.media_cache : [];
  cacheRows.forEach(row => {
    const sort = Number(row?.sort || 0);
    if(sort > 0) map.set(sort, row || {});
  });
  return map;
}

function renderAnnouncementCacheInfo(){
  const cacheMap = announcementCacheMap();
  for(let i=1;i<=4;i++){
    const el = $(`annCacheInfo${i}`);
    if(!el) continue;
    const row = cacheMap.get(i) || {};
    const normalized = String(row.normalized_url || '').trim();
    const fileId = String(row.telegram_file_id || '').trim();
    const status = String(row.status || '').trim() || 'pending';
    const parts = [];
    parts.push(`标准化：${normalized ? '已生成' : '未生成'}`);
    parts.push(`缓存：${fileId ? '已缓存' : '未缓存'}`);
    parts.push(`状态：${status}`);
    if(row.error){ parts.push(`错误：${String(row.error).slice(0,80)}`); }
    el.textContent = parts.join('；');
  }
}

function selectedAnnouncementCacheSorts(){
  const rows = [];
  for(let i=1;i<=4;i++){
    const el = $(`annClearCache${i}`);
    if(el && el.checked) rows.push(i);
  }
  return rows;
}

async function clearAnnouncementCache(selectedOnly){
  try{
    const scene = 'startup';
    const sorts = selectedOnly ? selectedAnnouncementCacheSorts() : [];
    if(selectedOnly && !sorts.length){
      setStatus('announcementStatus', '请先勾选要清除缓存的视频。');
      return;
    }
    const ok = selectedOnly
      ? confirm(`确认清除已选视频缓存？清除后下次发送时会重建这些视频的 Telegram 缓存。\n已选视频：${sorts.join('、')}`)
      : confirm('确认清除当前首次公告的全部视频缓存？清除后下次发送会重新生成 Telegram 缓存，速度会暂时变慢。');
    if(!ok) return;
    const path = selectedOnly ? '/admin/announcements/cache/clear-selected' : '/admin/announcements/cache/clear';
    const payload = selectedOnly ? {scene, sorts} : {scene};
    const res = await apiPost(path, payload);
    ANNOUNCEMENT_CONFIG = (res && res.data) ? res.data : ANNOUNCEMENT_CONFIG;
    fillAnnouncementForm(ANNOUNCEMENT_CONFIG || {});
    const cleared = Array.isArray(res?.cleared_sorts) ? res.cleared_sorts : [];
    const msg = selectedOnly
      ? `已清除选中缓存：${cleared.join('、') || '无'}。下次发送会重建这些视频。`
      : `已清除全部缓存：${Number(res?.cleared_count || 0)} 个视频。下次发送会重新生成 Telegram 缓存。`;
    appendAnnouncementTaskLog(msg);
    setStatus('announcementStatus', msg);
  }catch(e){
    setStatus('announcementStatus', '清除公告缓存失败：' + e.message);
  }
}


function fillAnnouncementForm(data){
  const row = data || {};
  ANNOUNCEMENT_CONFIG = row;
  $('annTitle').value = row.title || '';
  $('annContent').value = row.content_text || '';
  $('annMediaMode').value = row.media_mode || (row.media_url ? 'single_video' : 'none');
  $('annTextMode').value = row.text_mode || 'caption_first';
  $('annReplaceStartWelcome').value = String(row.replace_start_welcome !== false);
  $('annFallbackMode').value = row.fallback_mode || 'text_only';
  $('annEnabled').value = String(!!row.is_enabled);

  for(let i=1;i<=4;i++){
    const input = $(`annMediaUrl${i}`);
    if(input){ input.value = ''; input.dataset.sourceUrl=''; input.dataset.normalizedUrl=''; }
    const ck = $(`annClearCache${i}`);
    if(ck) ck.checked = false;
  }
  const mediaItems = Array.isArray(row.media_items) ? row.media_items : [];
  const mediaCache = Array.isArray(row.media_cache) ? row.media_cache : [];
  mediaItems.forEach((item, idx) => {
    const input = $(`annMediaUrl${idx+1}`);
    const cache = mediaCache[idx] || {};
    if(input){
      input.value = item.url || cache.normalized_url || cache.source_url || '';
      input.dataset.sourceUrl = cache.source_url || item.source_url || item.url || '';
      input.dataset.normalizedUrl = cache.normalized_url || item.normalized_url || item.url || '';
    }
  });
  if(!mediaItems.length && row.media_url && $('annMediaUrl1')) { $('annMediaUrl1').value = row.media_url || ''; $('annMediaUrl1').dataset.sourceUrl = row.media_url || ''; $('annMediaUrl1').dataset.normalizedUrl = row.media_url || ''; }

  const selected = new Set((row.target_bot_types || ['buyer']).map(x => String(x)));
  const typeEl = $('annTargetBotTypes');
  if(typeEl){
    Array.from(typeEl.options).forEach(opt => {
      opt.selected = selected.has(opt.value);
    });
  }

  renderAnnouncementPreview();
  renderAnnouncementCacheInfo();
}

function renderAnnouncementPreview(){
  const el = $('announcementCurrentPreview');
  if(!el) return;
  const title = escapeHtml($('annTitle').value || '未设置标题');
  const rawText = $('annContent').value || '未设置公告内容';
  const mediaMode = $('annMediaMode').value || 'none';
  const count = announcementMediaItems().length;
  const types = selectedAnnouncementBotTypes().map(botTypeText).join('、') || '商城机器人';
  const replaceText = $('annReplaceStartWelcome').value === 'true' ? '替代欢迎词' : '保留欢迎词';
  const cacheRows = Array.isArray(ANNOUNCEMENT_CONFIG?.media_cache) ? ANNOUNCEMENT_CONFIG.media_cache : [];
  const cachedCount = cacheRows.filter(row => String(row?.telegram_file_id || '').trim()).length;
  el.innerHTML = `当前公告摘要：<strong>${title}</strong>；适用类型：${escapeHtml(types)}；模式：${escapeHtml(mediaMode)}；视频数：${count}；已缓存：${cachedCount}/${cacheRows.length || count || 0}；内容长度：${rawText.length} 字；/start：${escapeHtml(replaceText)}`;
}

function taskLogTimeText(){
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${y}-${m}-${day} ${hh}:${mm}:${ss}`;
}

function appendAnnouncementTaskLog(message){
  const box = $('announcementTaskLog');
  if(!box) return;
  const item = document.createElement('div');
  item.className = 'task-log-item';
  item.innerHTML = `<span class="task-log-time">${escapeHtml(taskLogTimeText())}</span>${escapeHtml(String(message || '-'))}`;
  if(box.firstElementChild && box.firstElementChild.textContent && box.firstElementChild.textContent.includes('商城公告任务日志将在这里显示')){
    box.innerHTML = '';
  }
  box.prepend(item);
}

async function loadAnnouncements(){
  try{
    const data = await apiGet('/admin/announcements/config', {scene:'startup'});
    fillAnnouncementForm(data || {});
    resetAnnouncementUploadProgress();
    appendAnnouncementTaskLog('商城公告配置已加载。');
    setStatus('announcementStatus', '商城公告已加载。');
  }catch(e){
    setStatus('announcementStatus', '加载商城公告失败：' + e.message);
  }
}

const TELEGRAM_BOT_VIDEO_LIMIT_BYTES = 50 * 1000 * 1000;
const TELEGRAM_BOT_VIDEO_WARN_BYTES = 45 * 1000 * 1000;

function formatFileSize(bytes){
  const value = Number(bytes || 0);
  if(value < 1024) return `${value} B`;
  if(value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if(value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function telegramVideoLimitHint(bytes){
  const value = Number(bytes || 0);
  if(!value) return 'Telegram 预计：等待选择视频文件';
  if(value > TELEGRAM_BOT_VIDEO_LIMIT_BYTES){
    return `Telegram 预计：可能超限（当前 ${formatFileSize(value)}，建议压到 45 MB 以内）`;
  }
  if(value > TELEGRAM_BOT_VIDEO_WARN_BYTES){
    return `Telegram 预计：接近上限（当前 ${formatFileSize(value)}，建议压小后再发）`;
  }
  return `Telegram 预计：可发送（当前 ${formatFileSize(value)}）`;
}

function resetAnnouncementUploadProgress(){
  const box = $('announcementUploadBox');
  const fill = $('announcementUploadFill');
  const percent = $('announcementUploadPercent');
  const size = $('announcementUploadSize');
  const tip = $('announcementUploadTip');
  const tgHint = $('announcementTelegramHint');
  if(box) box.style.display = 'none';
  if(fill) fill.style.width = '0%';
  if(percent) percent.textContent = '0%';
  if(size) size.textContent = '0 B / 0 B';
  if(tip) tip.textContent = '等待上传…';
  if(tgHint) tgHint.textContent = 'Telegram 预计：等待选择视频文件';
}

function updateAnnouncementUploadProgress(loaded, total, tipText){
  const box = $('announcementUploadBox');
  const fill = $('announcementUploadFill');
  const percent = $('announcementUploadPercent');
  const size = $('announcementUploadSize');
  const tip = $('announcementUploadTip');
  const tgHint = $('announcementTelegramHint');
  const safeTotal = Number(total || 0);
  const safeLoaded = Number(loaded || 0);
  const ratio = safeTotal > 0 ? Math.min(100, Math.max(0, Math.round((safeLoaded / safeTotal) * 100))) : 0;
  if(box) box.style.display = 'block';
  if(fill) fill.style.width = `${ratio}%`;
  if(percent) percent.textContent = `${ratio}%`;
  if(size) size.textContent = `${formatFileSize(safeLoaded)} / ${formatFileSize(safeTotal)}`;
  if(tip && tipText) tip.textContent = tipText;
  if(tgHint) tgHint.textContent = telegramVideoLimitHint(safeTotal || safeLoaded);
}

document.addEventListener('change', function(evt){
  const target = evt.target;
  if(!target || !String(target.id || '').startsWith('annVideoFile')) return;
  const file = target.files && target.files[0];
  if(!file){
    resetAnnouncementUploadProgress();
    return;
  }
  updateAnnouncementUploadProgress(0, file.size || 0, `已选择：${file.name}（${formatFileSize(file.size || 0)}）`);
});

async function uploadAnnouncementVideo(slotIndex){
  try{
    const fileInput = $(`annVideoFile${slotIndex}`);
    const file = fileInput && fileInput.files && fileInput.files[0];
    if(!file){
      setStatus('announcementStatus', `请先选择视频 ${slotIndex}。`);
      return;
    }

    const uploadBtn = $(`annUploadBtn${slotIndex}`);
    if(uploadBtn) uploadBtn.disabled = true;
    updateAnnouncementUploadProgress(0, file.size || 0, `准备上传视频 ${slotIndex}：${file.name}（${formatFileSize(file.size || 0)}）`);

    const formData = new FormData();
    formData.append('file', file);

    await ensureAdminAuth(false);

    const data = await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', getApiBase() + '/admin/announcements/upload-video', true);
      xhr.setRequestHeader('Accept', 'application/json');

      xhr.upload.onprogress = function(evt){
        if(evt.lengthComputable){
          updateAnnouncementUploadProgress(evt.loaded, evt.total, `视频 ${slotIndex} 上传中，请等待…`);
        }else{
          updateAnnouncementUploadProgress(0, file.size || 0, `视频 ${slotIndex} 上传中，请等待…`);
        }
      };

      xhr.onload = function(){
        if(uploadBtn) uploadBtn.disabled = false;
        if(xhr.status >= 200 && xhr.status < 300){
          try{
            const payload = JSON.parse(xhr.responseText || '{}');
            updateAnnouncementUploadProgress(file.size || 0, file.size || 0, `视频 ${slotIndex} 上传完成，正在回填地址…`);
            resolve(payload);
          }catch(err){
            reject(new Error('上传成功，但返回结果解析失败'));
          }
          return;
        }
        try{
          const payload = JSON.parse(xhr.responseText || '{}');
          reject(new Error(payload.detail || ('HTTP ' + xhr.status)));
        }catch(_){
          reject(new Error('HTTP ' + xhr.status));
        }
      };

      xhr.onerror = function(){
        if(uploadBtn) uploadBtn.disabled = false;
        reject(new Error('网络异常，上传中断'));
      };

      xhr.onabort = function(){
        if(uploadBtn) uploadBtn.disabled = false;
        reject(new Error('上传已取消'));
      };

      xhr.send(formData);
    });

    const targetInput = $(`annMediaUrl${slotIndex}`);
    if(targetInput){
      targetInput.value = data.normalized_url || data.url || '';
      targetInput.dataset.sourceUrl = data.source_url || data.url || '';
      targetInput.dataset.normalizedUrl = data.normalized_url || data.url || '';
    }
    renderAnnouncementPreview();
    updateAnnouncementUploadProgress(file.size || 0, file.size || 0, `视频 ${slotIndex} 上传成功：${file.name}（${formatFileSize(file.size || 0)}）`);
    const normalizeTip = data.normalize_status === 'ready' ? '服务器已自动转为稳定 MP4。' : ('自动转码失败，将回退原始文件。' + (data.normalize_error ? (' ' + data.normalize_error) : ''));
    setStatus('announcementStatus', `视频 ${slotIndex} 已上传成功，地址已自动回填。${normalizeTip}`);
    appendAnnouncementTaskLog(`视频 ${slotIndex} 上传成功。`);
  }catch(e){
    const selectedFile = $(`annVideoFile${slotIndex}`).files && $(`annVideoFile${slotIndex}`).files[0];
    updateAnnouncementUploadProgress(0, selectedFile ? (selectedFile.size || 0) : 0, '上传失败，请检查网络或文件格式后重试。');
    setStatus('announcementStatus', '上传公告视频失败：' + e.message);
  }finally{
    const uploadBtn = $(`annUploadBtn${slotIndex}`);
    if(uploadBtn) uploadBtn.disabled = false;
  }
}

async function saveAnnouncementConfig(){
  try{
    const mediaMode = $('annMediaMode').value || 'none';
    const mediaItems = announcementMediaItems();
    if(mediaMode === 'video_album' && mediaItems.length < 2){
      setStatus('announcementStatus', '相册组模式至少需要 2 个视频。');
      return;
    }
    const payload = {
      scene:'startup',
      title:$('annTitle').value.trim(),
      content_text:$('annContent').value.trim(),
      media_mode:mediaMode,
      media_items:mediaItems,
      media_type:(mediaMode === 'none' || !mediaItems.length) ? 'none' : 'video',
      media_url:mediaItems.length ? mediaItems[0].url : '',
      text_mode:$('annTextMode').value || 'caption_first',
      replace_start_welcome:$('annReplaceStartWelcome').value === 'true',
      fallback_mode:$('annFallbackMode').value || 'text_only',
      target_bot_types:selectedAnnouncementBotTypes(),
      is_enabled:$('annEnabled').value === 'true'
    };
    await apiPost('/admin/announcements/config', payload);
    renderAnnouncementPreview();
    appendAnnouncementTaskLog('首次启动公告已保存。');
    setStatus('announcementStatus', '首次启动公告已保存。');
  }catch(e){
    setStatus('announcementStatus', '保存商城公告失败：' + e.message);
  }
}

async function runAnnouncementBroadcast(){
  try{
    const target_bot_types = selectedAnnouncementBotTypes();
    if(!target_bot_types.length){
      setStatus('announcementStatus', '请至少选择一种机器人类型。');
      return;
    }
    if(($('annMediaMode').value || 'none') === 'video_album' && announcementMediaItems().length < 2){
      setStatus('announcementStatus', '相册组模式至少需要 2 个视频。');
      return;
    }
    if(!confirm('确定对选中类型机器人执行群发公告吗？')) return;

    const res = await apiPost('/admin/announcements/broadcast', {
      scene:'startup',
      target_bot_types
    });

    let msg = `群发完成：共 ${Number(res.total || 0)} 个，成功 ${Number(res.success_count || 0)} 个`;
    if(Number(res.failed_count || 0) > 0){
      const first = (res.results || []).find(x => x.status === 'failed');
      msg += `，失败 ${Number(res.failed_count || 0)} 个`;
      if(first && first.error){
        msg += `。首个失败：${first.bot_code} / ${first.error}`;
      }
    }
    appendAnnouncementTaskLog(msg);
    setStatus('announcementStatus', msg);
  }catch(e){
    setStatus('announcementStatus', '群发公告失败：' + e.message);
  }
}



  // Folder link
  function selectedFolderLinkBotTypes(){
    const el = $('folderLinkBotTypes');
    if(!el) return ['buyer','session','shipping'];
    return Array.from(el.selectedOptions || []).map(x=>String(x.value||'').trim()).filter(Boolean);
  }
  function fillFolderLinkBotTypes(values){
    const rows = Array.isArray(values) ? values : [];
    const el = $('folderLinkBotTypes');
    if(!el) return;
    Array.from(el.options || []).forEach(opt=>{ opt.selected = rows.includes(opt.value); });
  }
  function renderFolderLinkRuntimePreview(data){
    if(!$('folderLinkRuntimePreview')) return;
    const rows = [];
    rows.push(`状态：${escapeHtml(data.status || 'unknown')}`);
    rows.push(`启用：${data.is_enabled ? '是' : '否'}`);
    rows.push(`适用机器人：${(data.apply_to_all_bots ? ['全部'] : (data.apply_to_bot_types || [])).join('、')}`);
    rows.push(`主按钮：${escapeHtml(data.primary_button_text || '-')}`);
    rows.push(`共享文件夹链接：${escapeHtml(data.folder_link_url || '-')}`);
    rows.push(`最近检测：${escapeHtml(formatTime(data.last_checked_at) || '-')}`);
    if(data.last_check_error){ rows.push(`最近错误：${escapeHtml(data.last_check_error)}`); }
    $('folderLinkRuntimePreview').innerHTML = rows.map(x=>`<div>${x}</div>`).join('');
  }
  async function loadFolderLinkConfig(){
    try{
      const data = await apiGet('/admin/folder-link/config');
      $('folderLinkEnabled').value = data.is_enabled ? 'true' : 'false';
      $('folderLinkPrimaryText').value = data.primary_button_text || '添加到商城文件夹';
      $('folderLinkUrl').value = data.folder_link_url || '';
      $('folderLinkApplyAll').checked = !!data.apply_to_all_bots;
      fillFolderLinkBotTypes(data.apply_to_bot_types || ['buyer','session','shipping']);
      $('folderLinkShowSettings').checked = !!data.show_settings_button;
      $('folderSettingsText').value = data.settings_button_text || '打开文件夹设置';
      $('folderSettingsUrl').value = data.settings_button_url || 'tg://settings/folders';
      $('folderLinkShowManual').checked = !!data.show_manual_hint_button;
      $('folderManualText').value = data.manual_hint_button_text || '如何手动加入机器人';
      $('folderManualHint').value = data.manual_hint_text || '';
      $('folderCheckMode').value = data.check_mode || 'weak';
      $('folderCheckInterval').value = data.check_interval_minutes || 60;
      renderFolderLinkRuntimePreview(data);
      setStatus('folderLinkStatus', `共享文件夹配置已加载。当前状态：${data.status || 'unknown'}`);
    }catch(e){
      setStatus('folderLinkStatus', '加载共享文件夹配置失败：' + e.message);
    }
  }
  async function saveFolderLinkConfig(){
    try{
      const payload = {
        is_enabled: $('folderLinkEnabled').value === 'true',
        primary_button_text: $('folderLinkPrimaryText').value.trim(),
        folder_link_url: $('folderLinkUrl').value.trim(),
        apply_to_all_bots: !!$('folderLinkApplyAll').checked,
        apply_to_bot_types: selectedFolderLinkBotTypes(),
        show_settings_button: !!$('folderLinkShowSettings').checked,
        settings_button_text: $('folderSettingsText').value.trim(),
        settings_button_url: $('folderSettingsUrl').value.trim(),
        show_manual_hint_button: !!$('folderLinkShowManual').checked,
        manual_hint_button_text: $('folderManualText').value.trim(),
        manual_hint_text: $('folderManualHint').value.trim(),
        check_mode: $('folderCheckMode').value || 'weak',
        check_interval_minutes: Number($('folderCheckInterval').value || 60)
      };
      const data = await apiPost('/admin/folder-link/config', payload);
      renderFolderLinkRuntimePreview(data);
      setStatus('folderLinkStatus', '共享文件夹配置已保存。');
    }catch(e){
      setStatus('folderLinkStatus', '保存共享文件夹配置失败：' + e.message);
    }
  }
  async function checkFolderLinkNow(){
    try{
      const data = await apiPost('/admin/folder-link/check', {});
      renderFolderLinkRuntimePreview(data);
      setStatus('folderLinkStatus', `检测完成：${data.status || 'unknown'}${data.last_check_error ? ' / ' + data.last_check_error : ''}`);
    }catch(e){
      setStatus('folderLinkStatus', '检测共享文件夹链接失败：' + e.message);
    }
  }

  // Admin users
  function roleTag(v){ return v==='superadmin' ? '<span class="tag red">超级管理员</span>' : '<span class="tag off">普通管理员</span>'; }
  function clearAdminUserForm(){ $('adminUserId').value=''; $('adminUsername').value=''; $('adminUsername').disabled=false; $('adminDisplayName').value=''; $('adminRole').value='operator'; $('adminIsActive').value='true'; $('adminPassword').value=''; setStatus('adminsStatus', ADMIN_AUTH.is_superadmin ? '可新增管理员、分配角色、停用和删除账号。' : '当前账号不是超级管理员，只能查看管理员列表。'); }
  function editAdminUser(raw){ const row = decodeRowPayload(raw); $('adminUserId').value=row.id||''; $('adminUsername').value=row.username||''; $('adminUsername').disabled=true; $('adminDisplayName').value=row.display_name||row.username||''; $('adminRole').value=row.role||'operator'; $('adminIsActive').value=String(!!row.is_active); $('adminPassword').value=''; setStatus('adminsStatus','已回填管理员，可修改显示名、角色、状态；密码请用表格里的“重置密码”。'); window.scrollTo({top:0,behavior:'smooth'}); }
  function renderAdminsTable(){ const el=$('adminsTable'); if(!el) return; if(!ADMIN_USERS.length){ el.innerHTML='<div class="muted">暂无后台管理员</div>'; return; } let html='<table><thead><tr><th>ID</th><th>账号</th><th>显示名称</th><th>角色</th><th>状态</th><th>最后登录</th><th>创建时间</th><th>操作</th></tr></thead><tbody>'; for(const row of ADMIN_USERS){ const canEdit = !!ADMIN_AUTH.is_superadmin; html += `<tr><td>${row.id}</td><td class="mono">${escapeHtml(row.username)}</td><td>${escapeHtml(row.display_name || row.username)}</td><td>${roleTag(row.role)}</td><td>${boolTag(row.is_active)}</td><td>${escapeHtml(formatTime(row.last_login_at))}</td><td>${escapeHtml(formatTime(row.created_at))}</td><td><div class="actions">${canEdit?`<button class="small secondary" onclick="editAdminUser('${encodeRowPayload(row)}')">编辑</button><button class="small orange" onclick="toggleAdminUser(${row.id})">${row.is_active?'停用':'启用'}</button><button class="small" onclick="resetAdminPassword(${row.id}, '${encodeTextPayload(row.username)}')">重置密码</button><button class="small red" onclick="deleteAdminUser(${row.id}, '${encodeTextPayload(row.username)}')">删除</button>`:'<span class="muted">仅超级管理员可管理</span>'}</div></td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
  async function loadAdminUsers(){ try{ await ensureAdminAuth(false); if($('adminManageMode')) $('adminManageMode').textContent = ADMIN_AUTH.is_superadmin ? '超级管理员模式' : '只读模式'; if(!ADMIN_AUTH.is_superadmin){ ADMIN_USERS = []; $('adminsTable').innerHTML = '<div class="muted">当前账号不是超级管理员，只能查看自己的登录状态，不能管理其他后台账号。</div>'; clearAdminUserForm(); return; } ADMIN_USERS = await apiGet('/admin/users'); renderAdminsTable(); clearAdminUserForm(); }catch(e){ setStatus('adminsStatus','加载管理员失败：'+e.message); } }
  async function saveAdminUser(){ try{ await ensureAdminAuth(false); if(!ADMIN_AUTH.is_superadmin){ setStatus('adminsStatus','只有超级管理员可以保存后台账号。'); return; } const isEdit = !!$('adminUserId').value; const payload = {id:$('adminUserId').value||null, username:$('adminUsername').value.trim(), display_name:$('adminDisplayName').value.trim(), role:$('adminRole').value, is_active:$('adminIsActive').value==='true'}; if(!isEdit){ payload.password = $('adminPassword').value.trim(); if(!payload.username){ setStatus('adminsStatus','请先填写管理员账号。'); return; } if(!payload.password){ setStatus('adminsStatus','新增管理员时必须填写登录密码。'); return; } } await apiPost('/admin/users', payload); setStatus('adminsStatus', isEdit ? '管理员已更新。' : '管理员已创建。'); await loadAdminUsers(); }catch(e){ setStatus('adminsStatus','保存管理员失败：'+e.message); } }
  async function toggleAdminUser(id){ try{ if(!confirm('确定切换这个后台账号的启停状态吗？')) return; await apiPost('/admin/users/'+id+'/toggle', {}); setStatus('adminsStatus','管理员状态已切换。'); await loadAdminUsers(); }catch(e){ setStatus('adminsStatus','切换管理员状态失败：'+e.message); } }
  async function resetAdminPassword(id, username){ try{ username = decodeTextPayload(username); const newPassword = prompt('给管理员 '+username+' 设置新密码（至少 6 位）'); if(newPassword===null) return; if(String(newPassword).trim().length < 6){ setStatus('adminsStatus','新密码至少需要 6 位。'); return; } await apiPost('/admin/users/'+id+'/password', {new_password:String(newPassword).trim()}); setStatus('adminsStatus','管理员密码已重置。'); }catch(e){ setStatus('adminsStatus','重置管理员密码失败：'+e.message); } }
  async function deleteAdminUser(id, username){ try{ username = decodeTextPayload(username); if(!confirm('确定删除后台管理员 '+username+' 吗？此操作不能撤销。')) return; await apiDelete('/admin/users/'+id); setStatus('adminsStatus','管理员账号已删除。'); await loadAdminUsers(); }catch(e){ setStatus('adminsStatus','删除管理员失败：'+e.message); } }
  async function changeMyPassword(){ try{ const current_password = $('myCurrentPassword').value.trim(); const new_password = $('myNewPassword').value.trim(); if(!current_password){ setStatus('myPasswordStatus','请先填写当前密码。'); return; } if(new_password.length < 6){ setStatus('myPasswordStatus','新密码至少需要 6 位。'); return; } await apiPost('/admin/users/me/password', {current_password, new_password}); $('myCurrentPassword').value=''; $('myNewPassword').value=''; setStatus('myPasswordStatus','我的密码已更新，下次登录请使用新密码。'); }catch(e){ setStatus('myPasswordStatus','修改我的密码失败：'+e.message); } }


  // Bots
  function botTypeText(v){ if(v==='shipping') return '供应链机器人'; if(v==='session') return '聚合聊天机器人'; return '商城机器人'; }
  function runtimeTag(v){ if(v==='running') return '<span class="tag ok">运行中</span>'; if(v==='starting') return '<span class="tag warn">启动中</span>'; if(v==='stopping') return '<span class="tag warn">停止中</span>'; return '<span class="tag off">已停止</span>'; }
  function clearBotForm(){ $('botId').value=''; $('botCode').value=''; $('botToken').value=''; $('botToken').placeholder='123456:ABC...'; $('botType').value='buyer'; $('botName').value=''; $('botAlias').value=''; $('botShortDescription').value=''; $('botDescription').value=''; $('botStartWelcomeText').value=''; $('botAvatarImage').value=''; $('botAvatarFile').value=''; clearBotAvatar(false); $('botSupplierCode').value=''; $('botIsEnabled').value='true'; }
  function clearBotAvatar(clearInput=true){ if(clearInput){ $('botAvatarImage').value=''; $('botAvatarFile').value=''; } previewImage('', 'botAvatarPreview', 'botAvatarPlaceholder'); }
  async function uploadBotAvatar(){ try{ const file=$('botAvatarFile').files && $('botAvatarFile').files[0]; if(!file){ setStatus('botsStatus','请先选择 JPG/JPEG 头像。'); return; } const data = await apiUpload('/admin/bots/upload-avatar', file); $('botAvatarImage').value = data.url || ''; previewImage(data.url || '', 'botAvatarPreview', 'botAvatarPlaceholder'); setStatus('botsStatus','机器人头像已上传，保存 Bot 后会同步到 Telegram。'); }catch(e){ setStatus('botsStatus','上传机器人头像失败：'+e.message); } }
  function editBot(raw){ const row = decodeRowPayload(raw); $('botId').value=row.id||''; $('botCode').value=row.bot_code||''; $('botToken').value=''; $('botToken').placeholder=row.bot_token_masked||row.bot_token||'留空则保持原 Token'; $('botType').value=row.bot_type||'buyer'; $('botName').value=row.bot_name||''; $('botAlias').value=row.bot_alias||''; $('botShortDescription').value=row.bot_short_description||''; $('botDescription').value=row.bot_description||''; $('botStartWelcomeText').value=row.start_welcome_text||''; $('botAvatarImage').value=row.avatar_image||''; previewImage(row.avatar_image||'', 'botAvatarPreview', 'botAvatarPlaceholder'); $('botSupplierCode').value=row.supplier_code||''; $('botIsEnabled').value=String(!!row.is_enabled); const syncText = row.last_profile_sync_error ? ('上次同步失败：'+row.last_profile_sync_error) : ('上次同步：'+(row.last_profile_sync_at||'未同步')); setStatus('botsStatus','已回填 Bot；Token 留空则保持原值。'+syncText); window.scrollTo({top:0,behavior:'smooth'}); }
  async function saveBot(){ try{ const payload={id:$('botId').value||null,bot_code:$('botCode').value.trim(),bot_token:$('botToken').value.trim(),bot_type:$('botType').value,bot_name:$('botName').value.trim(),bot_alias:$('botAlias').value.trim(),bot_short_description:$('botShortDescription').value.trim(),bot_description:$('botDescription').value.trim(),start_welcome_text:$('botStartWelcomeText').value.trim(),avatar_image:$('botAvatarImage').value.trim(),supplier_code:$('botSupplierCode').value.trim(),is_enabled:$('botIsEnabled').value==='true'}; if(!payload.bot_code){ setStatus('botsStatus','请先填写 Bot 编码。'); showBotSaveToast('保存失败','请先填写 Bot 编码。', true); return; } if(!payload.id && !payload.bot_token){ setStatus('botsStatus','请先填写 Bot Token。'); showBotSaveToast('保存失败','请先填写 Bot Token。', true); return; } if(payload.bot_type==='shipping' && !payload.supplier_code){ setStatus('botsStatus','供应链机器人必须绑定供应链编码。'); showBotSaveToast('保存失败','供应链机器人必须绑定供应链编码。', true); return; } const res = await apiPost('/admin/bots', payload); let statusText='Bot 已保存。'; if(res && res.profile_sync && res.profile_sync.ok===false){ statusText += ' 但同步 Telegram 资料失败：'+(res.profile_sync.error||'未知错误'); } else if(res && res.profile_sync && res.profile_sync.ok){ statusText += ' Telegram 资料已同步。'; } setStatus('botsStatus', statusText); showBotSaveToast('保存成功','启动欢迎词及 Bot 配置已保存。建议等待 5-10 秒后再用 /start 测试。', false); if(res && res.data){ editBot(encodeRowPayload(res.data)); } await loadBots(); await loadSuppliersCenter(); }catch(e){ setStatus('botsStatus','保存 Bot 失败：'+e.message); showBotSaveToast('保存失败','Bot 配置未保存成功：'+e.message, true); } }
  async function syncSelectedBotProfile(){ try{ const id = $('botId').value || ''; if(!id){ setStatus('botsStatus','请先从右侧列表选择一个 Bot。'); return; } const res = await apiPost('/admin/bots/'+id+'/sync-profile', {}); if(res.ok===false){ setStatus('botsStatus','同步 Telegram 资料失败：'+(res.error||'未知错误')); } else { setStatus('botsStatus','Telegram 资料已重新同步。'); } await loadBots(); if(res && res.data){ editBot(encodeRowPayload(res.data)); } }catch(e){ setStatus('botsStatus','同步资料失败：'+e.message); } }
  function renderBotAutoSyncInfo(){ const el=$('botAutoSyncInfo'); if(!el) return; const s=BOT_AUTO_SYNC_STATUS||{}; const enabled = !!s.enabled; const scopeText = s.scope==='all' ? '全部机器人' : '启用机器人'; const typeText = s.bot_type && s.bot_type!=='all' ? (' / '+botTypeText(s.bot_type)) : ''; let text = `自动同步：${enabled?'已开启':'未开启'}；范围：${scopeText}${typeText}；间隔：${Number(s.interval_seconds||0)} 秒`; if(s.last_finished_at){ text += `；上次完成：${formatTime(s.last_finished_at)}`; if(Number(s.last_total||0)>0){ text += `（成功 ${Number(s.last_success_count||0)} / 失败 ${Number(s.last_failed_count||0)}）`; } } if(s.last_first_error){ text += `；最近失败：${s.last_first_error}`; } el.textContent = text; }
  async function batchSyncBotProfiles(scope='enabled'){ try{ const res = await apiPost('/admin/bots/sync-profile-batch', {scope: scope, bot_type: 'all'}); const ok = Number(res.success_count||0); const failed = Number(res.failed_count||0); const total = Number(res.total||0); let msg = `批量同步完成：共 ${total} 个，成功 ${ok} 个`; if(failed>0){ const firstError = (res.results||[]).find(x=>x.status==='failed'); msg += `，失败 ${failed} 个`; if(firstError && firstError.error){ msg += `。首个失败：${firstError.bot_code} / ${firstError.error}`; } } else { msg += '，全部成功。'; } setStatus('botsStatus', msg); await loadBots(); }catch(e){ setStatus('botsStatus','批量同步资料失败：'+e.message); } }
  async function manualRunBotProfileAutoSync(){ try{ const res = await apiPost('/admin/bots/profile-auto-sync-run', {}); const ok = Number(res.success_count||0); const failed = Number(res.failed_count||0); const total = Number(res.total||0); let msg = `按配置立即同步完成：共 ${total} 个，成功 ${ok} 个`; if(failed>0){ const firstError = (res.results||[]).find(x=>x.status==='failed'); msg += `，失败 ${failed} 个`; if(firstError && firstError.error){ msg += `。首个失败：${firstError.bot_code} / ${firstError.error}`; } } else { msg += '，全部成功。'; } setStatus('botsStatus', msg); await loadBots(); }catch(e){ setStatus('botsStatus','按配置立即同步失败：'+e.message); } }
  async function enableBot(id){ try{ await apiPost('/admin/bots/'+id+'/enable', {}); setStatus('botsStatus','已发送启用请求。'); await loadBots(); }catch(e){ setStatus('botsStatus','启用失败：'+e.message); } }
  async function disableBot(id){ try{ await apiPost('/admin/bots/'+id+'/disable', {}); setStatus('botsStatus','已发送停用请求。'); await loadBots(); }catch(e){ setStatus('botsStatus','停用失败：'+e.message); } }
  function renderBotSyncTag(row){ if(row.last_profile_sync_error){ return `<span class="tag warn" title="${escapeHtml(row.last_profile_sync_error)}">同步失败</span>`; } if(row.last_profile_sync_at){ return `<span class="tag ok">已同步</span>`; } return '<span class="tag off">未同步</span>'; }
  function renderBotsTable(){ const el=$('botsTable'); if(!BOTS.length){ el.innerHTML='<div class="muted">暂无 Bot</div>'; return; } let html='<table><thead><tr><th>ID</th><th>Bot</th><th>类型</th><th>供应链</th><th>Telegram 资料</th><th>Token</th><th>配置状态</th><th>操作</th></tr></thead><tbody>'; for(const row of BOTS){ const tokenView=(row.bot_token||'').slice(0,12)+'...'; const avatar = normalizeMediaUrl(row.avatar_image||''); const avatarHtml = avatar ? `<img src="${escapeHtml(avatar)}" style="width:40px;height:40px;border-radius:12px;object-fit:cover;border:1px solid var(--line)" />` : `<div style="width:40px;height:40px;border-radius:12px;border:1px dashed var(--line);display:flex;align-items:center;justify-content:center;font-size:12px;color:#94a3b8">无图</div>`; const title = row.bot_alias || row.bot_name || row.bot_code; const username = row.telegram_username ? ('@'+row.telegram_username) : '-'; html += `<tr><td>${row.id}</td><td><div style="display:flex;align-items:center;gap:10px">${avatarHtml}<div><div><strong>${escapeHtml(title)}</strong></div><div class="muted mono">${escapeHtml(row.bot_code)}</div><div class="muted">${escapeHtml(username)}</div></div></div></td><td>${escapeHtml(botTypeText(row.bot_type))}</td><td class="mono">${escapeHtml(row.supplier_code || '-')}</td><td><div>${renderBotSyncTag(row)}</div><div class="muted">名称：${escapeHtml(row.bot_name||'-')}</div><div class="muted">短简介：${escapeHtml((row.bot_short_description||'-').slice(0,32))}</div></td><td class="mono">${escapeHtml(tokenView)}</td><td>${boolTag(row.is_enabled)}</td><td><div class="actions"><button class="small secondary" onclick="editBot('${encodeRowPayload(row)}')">编辑</button><button class="small secondary" onclick="apiPost('/admin/bots/${row.id}/sync-profile', {}).then(()=>loadBots()).catch(e=>setStatus('botsStatus','同步失败：'+e.message))">同步资料</button>${row.is_enabled?`<button class="small orange" onclick="disableBot(${row.id})">停用</button>`:`<button class="small green" onclick="enableBot(${row.id})">启用</button>`}</div></td></tr>`; } html+='</tbody></table>'; el.innerHTML=html; }
  function renderBotRuntimeTable(){ const el=$('botRuntimeTable'); if(!el) return; if(!BOT_RUNTIME.length){ el.innerHTML='<div class="muted">暂无运行状态</div>'; return; } let html='<table><thead><tr><th>Bot 编码</th><th>类型</th><th>运行状态</th><th>状态说明</th><th>实例 ID</th><th>最后心跳</th><th>最后错误</th></tr></thead><tbody>'; for(const row of BOT_RUNTIME){ html += `<tr><td class="mono">${escapeHtml(row.bot_code||'-')}</td><td>${escapeHtml(botTypeText(row.bot_type))}</td><td>${runtimeTag(row.run_status)}</td><td>${escapeHtml(row.status_text||'-')}</td><td class="mono">${escapeHtml(row.instance_id||'-')}</td><td>${escapeHtml(formatTime(row.last_heartbeat_at))}</td><td>${escapeHtml(row.last_error||'-')}</td></tr>`; } html += '</tbody></table>'; el.innerHTML = html; }
  async function loadBots(){
    try{
      BOTS = await apiGet('/admin/bots');
      BOT_RUNTIME = await apiGet('/admin/bots/runtime-state');
      try{ BOT_AUTO_SYNC_STATUS = await apiGet('/admin/bots/profile-auto-sync-status'); }catch(_){ BOT_AUTO_SYNC_STATUS = null; }
      renderBotsTable();
      renderBotRuntimeTable();
      renderBotAutoSyncInfo();
      refreshOverview();
      setStatus('botsStatus', '机器人列表已加载。');
    }catch(e){
      console.error('loadBots failed', e);
      const table = $('botsTable'); if(table) table.innerHTML = '<div class="muted">机器人列表加载失败：'+escapeHtml(e.message || e)+'</div>';
      const runtime = $('botRuntimeTable'); if(runtime) runtime.innerHTML = '<div class="muted">运行状态加载失败：'+escapeHtml(e.message || e)+'</div>';
      setStatus('botsStatus', '机器人列表加载失败：' + (e.message || e));
    }
  }


// Chat center
function clearChatFilters(){ $('chatFilterBotCode').value=''; $('chatFilterStatus').value='open'; $('chatFilterQ').value=''; $('chatFilterUnread').value='true'; CHAT_SESSION_PAGE.page = 1; }
function debounceChatRefresh(){ if(CHAT_REFRESH_TIMER){ clearTimeout(CHAT_REFRESH_TIMER); } CHAT_REFRESH_TIMER = setTimeout(()=>loadChatCenter(true), 260); }
function chatStatusTag(v){ if(v==='closed') return '<span class="tag off">已关闭</span>'; return '<span class="tag ok">开放</span>'; }
function chatDirectionTag(v){ return v==='operator' ? '<span class="tag warn">客服</span>' : '<span class="tag ok">客户</span>'; }
function renderChatOverview(){ if(!CHAT_OVERVIEW) return; $('chatSessionCount').textContent = CHAT_OVERVIEW.session_count || 0; $('chatOpenCount').textContent = CHAT_OVERVIEW.open_count || 0; $('chatClosedCount').textContent = CHAT_OVERVIEW.closed_count || 0; $('chatUnreadCount').textContent = CHAT_OVERVIEW.unread_count || 0; }
async function loadSessionRuntimeStatus(){
  const el = $('sessionRuntimeInfo');
  if(el) el.textContent = '加载中...';
  try{
    const data = await apiGet('/admin/chat-runtime/session-status');
    const subscriberCount = Number(data.subscriber_count || 0);
    const unreadCount = Number(data.unread_count || 0);
    const botCode = data.bot_code || '-';
    const botUsername = data.bot_username || '-';
    const lastPushAt = data.last_push_at || '-';
    const lastPushResult = data.last_push_result || '-';
    let html = `聚合机器人：<strong>${escapeHtml(botCode)}</strong>（@${escapeHtml(botUsername || '-')}）<br>订阅者数量：<strong>${subscriberCount}</strong><br>未读会话：<strong>${unreadCount}</strong><br>最近推送时间：<strong>${escapeHtml(lastPushAt)}</strong><br>最近推送结果：<strong>${escapeHtml(lastPushResult)}</strong>`;
    if(subscriberCount === 0){
      html += `<div class="space-top" style="color:#b45309;font-weight:700">当前无订阅者，请先在聚合机器人窗口发送 /start。</div>`;
    }
    if(el) el.innerHTML = html;
  }catch(e){
    if(el) el.textContent = '状态加载失败';
    setStatus('sessionsStatus', '聚合机器人状态加载失败：' + e.message);
  }
}
function renderChatSessionsTable(){ const el=$('chatSessionsTable'); if(!CHAT_SESSIONS.length){ el.innerHTML='<div class="muted">暂无会话</div>'; return; } let html='<table><thead><tr><th>ID</th><th>Bot</th><th>客户</th><th>状态</th><th>未读</th><th>最后消息</th><th>最近客户发言</th><th>操作</th></tr></thead><tbody>'; for(const row of CHAT_SESSIONS){ const name = row.display_name || row.telegram_username || row.telegram_user_id; html += `<tr><td>${row.id}</td><td class="mono">${escapeHtml(row.bot_code||'-')}</td><td><div>${escapeHtml(name||'-')}</div><div class="muted mono">${escapeHtml(row.telegram_user_id||'-')}</div></td><td>${chatStatusTag(row.session_status)}</td><td>${row.unread_count||0}</td><td>${escapeHtml((row.last_message_text||'').slice(0,90) || '['+(row.last_message_type||'text')+']')}</td><td>${escapeHtml(formatTime(row.last_customer_message_at))}</td><td><div class="actions"><button class="small secondary" onclick="openChatSession(${row.id})">打开</button><button class="small green" onclick="quickReplyChat(${row.id})">回复</button></div></td></tr>`; } html+='</tbody></table>'; html += `<div class="space-top muted">第 ${CHAT_SESSION_PAGE.page||1} / ${CHAT_SESSION_PAGE.total_pages||1} 页，共 ${CHAT_SESSION_PAGE.total||0} 条会话</div><div class="actions space-top"><button class="small secondary" ${CHAT_SESSION_PAGE.has_prev?'':'disabled'} onclick="changeChatPage(-1)">上一页</button><button class="small secondary" ${CHAT_SESSION_PAGE.has_next?'':'disabled'} onclick="changeChatPage(1)">下一页</button></div>`; el.innerHTML=html; }
function renderChatSessionDetail(){ const el=$('chatSessionDetail'); if(!CURRENT_CHAT_SESSION){ el.innerHTML='<div class="empty-box">请先从左侧打开一个会话。</div>'; return; } const session=CURRENT_CHAT_SESSION.session||{}; const messages=CURRENT_CHAT_SESSION.messages||[]; const name=session.display_name || session.telegram_username || session.telegram_user_id || '-'; let html=`<div class="card soft" style="margin-bottom:12px"><div class="toolbar-inline"><span class="chip">会话 #${session.id}</span><span class="chip">Bot：${escapeHtml(session.bot_code||'-')}</span><span class="chip">客户：${escapeHtml(name)}</span><span class="chip">未读：${session.unread_count||0}</span>${session.session_status==='closed'?'<span class="chip">已关闭</span>':'<span class="chip">开放</span>'}</div><div class="muted space-top mono">用户ID：${escapeHtml(session.telegram_user_id||'-')}</div></div>`; if(!messages.length){ html += '<div class="muted">暂无消息</div>'; el.innerHTML = html; return; } html += '<div>'; for(const row of messages){ html += `<div style="padding:10px 12px;border:1px solid var(--line);border-radius:14px;margin-bottom:10px;background:${row.direction==='operator'?'#fff7ed':'#f8fafc'}"><div class="toolbar-inline" style="justify-content:space-between"><div>${chatDirectionTag(row.direction)} <strong>${escapeHtml(row.sender_name||'-')}</strong></div><div class="muted">${escapeHtml(formatTime(row.created_at))}</div></div><div class="space-top">${escapeHtml(row.content_text||'['+(row.message_type||'text')+']')}</div></div>`; } html += '</div>'; el.innerHTML=html; }
function changeChatPage(delta){ const next = Math.max(1, Number(CHAT_SESSION_PAGE.page||1) + Number(delta||0)); if(next === Number(CHAT_SESSION_PAGE.page||1)) return; CHAT_SESSION_PAGE.page = next; loadChatCenter(false); }
async function loadChatCenter(resetPage=false){ try{ if(resetPage){ CHAT_SESSION_PAGE.page = 1; } CHAT_OVERVIEW = await apiGet('/admin/chat-overview'); const sessionData = await apiGet('/admin/chat-sessions', {bot_code:$('chatFilterBotCode').value.trim(), status:$('chatFilterStatus').value, q:$('chatFilterQ').value.trim(), only_unread:$('chatFilterUnread').value, page:CHAT_SESSION_PAGE.page, page_size:CHAT_SESSION_PAGE.page_size}); CHAT_SESSIONS = sessionData.rows || []; CHAT_SESSION_PAGE = {...CHAT_SESSION_PAGE, page: sessionData.page || 1, page_size: sessionData.page_size || CHAT_SESSION_PAGE.page_size, total: sessionData.total || 0, total_pages: sessionData.total_pages || 1, has_prev: !!sessionData.has_prev, has_next: !!sessionData.has_next}; renderChatOverview(); renderChatSessionsTable(); await loadChatKeywordBlocks(); await loadSessionRuntimeStatus(); setStatus('chatStatus', '会话中心已刷新，共 '+(CHAT_SESSION_PAGE.total||0)+' 条。'); }catch(e){ setStatus('chatStatus', '加载会话中心失败：'+e.message); } }
async function openChatSession(id, markRead=true, refreshList=true){ try{ CURRENT_CHAT_SESSION = await apiGet('/admin/chat-sessions/'+id, {mark_read: markRead ? 'true' : 'false'}); renderChatSessionDetail(); if(refreshList){ await loadChatCenter(false); } }catch(e){ setStatus('chatStatus','打开会话失败：'+e.message); } }
function quickReplyChat(id){ openChatSession(id, true, true).then(()=>{ $('chatReplyText').focus(); }); }
async function sendChatReply(){ try{ if(!CURRENT_CHAT_SESSION || !CURRENT_CHAT_SESSION.session){ setStatus('chatStatus','请先打开一个会话。'); return; } const text = $('chatReplyText').value.trim(); if(!text){ setStatus('chatStatus','请先填写回复内容。'); return; } await apiPost('/admin/chat-sessions/'+CURRENT_CHAT_SESSION.session.id+'/reply', {text, operator_name:'admin_ui'}); $('chatReplyText').value=''; await openChatSession(CURRENT_CHAT_SESSION.session.id, false, false); setStatus('chatStatus','回复已发送给客户。'); }catch(e){ setStatus('chatStatus','发送回复失败：'+e.message); } }
async function markCurrentChatRead(){ try{ if(!CURRENT_CHAT_SESSION || !CURRENT_CHAT_SESSION.session){ setStatus('chatStatus','请先打开一个会话。'); return; } await apiPost('/admin/chat-sessions/'+CURRENT_CHAT_SESSION.session.id+'/read', {}); await openChatSession(CURRENT_CHAT_SESSION.session.id, false, false); setStatus('chatStatus','已标记为已读。'); }catch(e){ setStatus('chatStatus','标记已读失败：'+e.message); } }
async function closeCurrentChat(){ try{ if(!CURRENT_CHAT_SESSION || !CURRENT_CHAT_SESSION.session){ setStatus('chatStatus','请先打开一个会话。'); return; } await apiPost('/admin/chat-sessions/'+CURRENT_CHAT_SESSION.session.id+'/close', {}); await openChatSession(CURRENT_CHAT_SESSION.session.id, false, false); setStatus('chatStatus','会话已关闭。'); }catch(e){ setStatus('chatStatus','关闭会话失败：'+e.message); } }
async function reopenCurrentChat(){ try{ if(!CURRENT_CHAT_SESSION || !CURRENT_CHAT_SESSION.session){ setStatus('chatStatus','请先打开一个会话。'); return; } await apiPost('/admin/chat-sessions/'+CURRENT_CHAT_SESSION.session.id+'/reopen', {}); await openChatSession(CURRENT_CHAT_SESSION.session.id, false, false); setStatus('chatStatus','会话已重新打开。'); }catch(e){ setStatus('chatStatus','重新打开失败：'+e.message); } }


function clearChatKeywordForm(){ $('chatKeywordBlockId').value=''; $('chatKeywordValue').value=''; $('chatKeywordMatchType').value='exact'; $('chatKeywordRemark').value=''; $('chatKeywordIsActive').value='true'; }
function renderChatKeywordBlocks(){ const el=$('chatKeywordBlocksTable'); if(!el) return; const rows=CHAT_KEYWORD_BLOCKS||[]; const system=(CHAT_KEYWORD_EFFECTIVE && CHAT_KEYWORD_EFFECTIVE.system_keywords)||[]; let html=''; if(system.length){ html += `<div class="muted">系统内置：${system.map(x=>escapeHtml(x)).join('、')}</div>`; } if(!rows.length){ el.innerHTML = html + '<div class="empty-box">暂无自定义屏蔽词</div>'; return; } html += '<table class="space-top"><thead><tr><th>关键词</th><th>匹配</th><th>状态</th><th>备注</th><th>操作</th></tr></thead><tbody>'; for(const row of rows){ html += `<tr><td>${escapeHtml(row.keyword)}</td><td>${row.match_type==='contains'?'包含匹配':'完全匹配'}</td><td>${row.is_active?'<span class="tag ok">启用</span>':'<span class="tag off">停用</span>'}</td><td>${escapeHtml(row.remark||'-')}</td><td><div class="actions"><button class="small secondary" onclick="editChatKeywordBlock('${encodeRowPayload(row)}')">编辑</button><button class="small orange" onclick="toggleChatKeywordBlock(${row.id})">${row.is_active?'停用':'启用'}</button><button class="small red" onclick="deleteChatKeywordBlock(${row.id})">删除</button></div></td></tr>`; } html += '</tbody></table>'; el.innerHTML = html; }
function editChatKeywordBlock(raw){ const row=decodeRowPayload(raw); $('chatKeywordBlockId').value=row.id||''; $('chatKeywordValue').value=row.keyword||''; $('chatKeywordMatchType').value=row.match_type||'exact'; $('chatKeywordRemark').value=row.remark||''; $('chatKeywordIsActive').value=String(!!row.is_active); setStatus('chatKeywordStatus','已回填屏蔽词。'); }
async function loadChatKeywordBlocks(){ try{ CHAT_KEYWORD_BLOCKS = await apiGet('/admin/chat-keyword-blocks'); CHAT_KEYWORD_EFFECTIVE = await apiGet('/admin/chat-keyword-blocks/effective'); renderChatKeywordBlocks(); }catch(e){ setStatus('chatKeywordStatus','加载关键词屏蔽失败：'+e.message); } }
async function saveChatKeywordBlock(){ try{ const payload={id:$('chatKeywordBlockId').value||null, keyword:$('chatKeywordValue').value.trim(), match_type:$('chatKeywordMatchType').value, remark:$('chatKeywordRemark').value.trim(), is_active:$('chatKeywordIsActive').value==='true'}; if(!payload.keyword){ setStatus('chatKeywordStatus','请先填写关键词。'); return; } await apiPost('/admin/chat-keyword-blocks', payload); clearChatKeywordForm(); await loadChatKeywordBlocks(); setStatus('chatKeywordStatus','关键词屏蔽已保存。'); }catch(e){ setStatus('chatKeywordStatus','保存屏蔽词失败：'+e.message); } }
async function toggleChatKeywordBlock(id){ try{ await apiPost('/admin/chat-keyword-blocks/'+id+'/toggle', {}); await loadChatKeywordBlocks(); setStatus('chatKeywordStatus','屏蔽词状态已更新。'); }catch(e){ setStatus('chatKeywordStatus','切换失败：'+e.message); } }
async function deleteChatKeywordBlock(id){ if(!confirm('确定删除这个屏蔽词吗？')) return; try{ await apiDelete('/admin/chat-keyword-blocks/'+id); await loadChatKeywordBlocks(); setStatus('chatKeywordStatus','屏蔽词已删除。'); }catch(e){ setStatus('chatKeywordStatus','删除失败：'+e.message); } }

function renderProductImportPreview(){ const el=$('productImportPreviewTable'); if(!el) return; const data=PRODUCT_IMPORT_PREVIEW; if(!data){ el.innerHTML=''; return; } const summary=data.summary||{}; let html=`<div class="muted">总 ${summary.total||0} 行，正常 ${summary.ok||0}，预警 ${summary.warn||0}，错误 ${summary.error||0}；新增 ${summary.create||0}，更新 ${summary.update||0}</div>`; const rows=(data.rows||[]).slice(0,20); if(rows.length){ html += '<table class="space-top"><thead><tr><th>行号</th><th>动作</th><th>商品</th><th>SKU</th><th>结果</th><th>说明</th></tr></thead><tbody>'; for(const row of rows){ const msgs=[...(row.errors||[]), ...(row.warnings||[])].join('；')||'-'; html += `<tr><td>${row.row_no||'-'}</td><td>${escapeHtml(row.action||'-')}</td><td>${escapeHtml(row.product_name||'-')}</td><td>${escapeHtml(row.sku_code||'-')}</td><td>${row.errors&&row.errors.length?'<span class="tag off">错误</span>':(row.warnings&&row.warnings.length?'<span class="tag warn">预警</span>':'<span class="tag ok">通过</span>')}</td><td>${escapeHtml(msgs)}</td></tr>`; } html += '</tbody></table>'; if((data.rows||[]).length>20){ html += '<div class="muted space-top">仅显示前 20 行预览。</div>'; } } el.innerHTML=html; }
function renderProductImportBatchTable(){ const el=$('productImportBatchTable'); if(!el) return; if(!PRODUCT_IMPORT_BATCHES.length){ el.innerHTML='<div class="empty-box">暂无导入批次</div>'; return; } let html='<table><thead><tr><th>批次号</th><th>文件</th><th>操作人</th><th>统计</th><th>检测</th><th>时间</th><th>操作</th></tr></thead><tbody>'; for(const row of PRODUCT_IMPORT_BATCHES){ const resultTag = row.failed_rows>0 ? '<span class="tag warn">有失败</span>' : (row.warning_rows>0 ? '<span class="tag off">有预警</span>' : '<span class="tag ok">成功</span>'); html += `<tr><td class="mono">${escapeHtml(row.batch_no)}</td><td>${escapeHtml(row.file_name||'-')}</td><td>${escapeHtml(row.operator_name||'-')}</td><td>总 ${escapeHtml(row.total_rows)} / 成功 ${escapeHtml(row.success_rows)} / 失败 ${escapeHtml(row.failed_rows)} / 预警 ${escapeHtml(row.warning_rows||0)}<div class="muted">新增 ${escapeHtml(row.created_rows||0)}，更新 ${escapeHtml(row.updated_rows||0)}</div></td><td>${row.check_images?'<span class="tag ok">已检测图片</span>':'<span class="tag off">未检测</span>'} ${resultTag}</td><td>${escapeHtml(formatTime(row.created_at))}</td><td><div class="actions"><button class="small secondary" onclick="viewProductImportErrors(${row.id})">错误详情</button><button class="small" onclick="downloadProductImportErrors(${row.id})">下载失败行</button><button class="small orange" onclick="retryProductImportBatch(${row.id})">二次导入</button><button class="small red" onclick="deleteProductImportBatch(${row.id})">删除</button></div></td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
function renderProductImportErrors(){ const el=$('productImportErrorsTable'); if(!el) return; if(!PRODUCT_IMPORT_ERRORS.length){ el.innerHTML=''; return; } let html='<div class="card-head"><div><div class="title">失败行详情</div><div class="subtitle">便于修正后重新导入</div></div></div>'; html += '<table><thead><tr><th>行号</th><th>商品ID</th><th>SKU</th><th>商品名</th><th>错误原因</th></tr></thead><tbody>'; for(const row of PRODUCT_IMPORT_ERRORS){ html += `<tr><td>${row.row_no||'-'}</td><td>${escapeHtml(row.product_id||'-')}</td><td>${escapeHtml(row.sku_code||'-')}</td><td>${escapeHtml(row.product_name||'-')}</td><td>${escapeHtml(row.error_message||'-')}</td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
async function downloadProductImportTemplate(){ openAdminPath('/admin/products/import-template'); }
async function previewProductImportFile(){ try{ const file=$('productImportFile').files[0]; if(!file){ setStatus('productImportStatus','请先选择导入文件。'); return; } const fd=new FormData(); fd.append('file', file); fd.append('check_images', $('productImportCheckImages').value==='true' ? 'true' : 'false'); PRODUCT_IMPORT_PREVIEW = await apiFormPost('/admin/products/import/preview', fd); renderProductImportPreview(); setStatus('productImportStatus','预览校验已完成。'); }catch(e){ setStatus('productImportStatus','预览校验失败：'+e.message); } }
async function uploadProductImportFile(){ try{ const file=$('productImportFile').files[0]; if(!file){ setStatus('productImportStatus','请先选择导入文件。'); return; } const fd=new FormData(); fd.append('file', file); fd.append('operator_name', $('productImportOperatorName').value.trim()||'admin_ui'); fd.append('check_images', $('productImportCheckImages').value==='true' ? 'true' : 'false'); const data = await apiFormPost('/admin/products/import', fd); setStatus('productImportStatus', `导入完成：批次 ${data.batch_no}，成功 ${data.success_rows}，失败 ${data.failed_rows}。`); PRODUCT_IMPORT_PREVIEW = null; renderProductImportPreview(); await loadProducts(); await loadProductImportBatches(); if(data.batch_id){ await viewProductImportErrors(data.batch_id); } }catch(e){ setStatus('productImportStatus','正式导入失败：'+e.message); } }
async function loadProductImportBatches(){ try{ const kw=$('productImportBatchKeyword') ? $('productImportBatchKeyword').value.trim() : ''; const st=$('productImportBatchStatus') ? $('productImportBatchStatus').value : 'all'; const qs={}; if(kw) qs.keyword=kw; if(st&&st!=='all') qs.result_status=st; PRODUCT_IMPORT_BATCHES = await apiGet('/admin/products/import-batches', qs); renderProductImportBatchTable(); }catch(e){ setStatus('productImportStatus','加载导入批次失败：'+e.message); } }
async function viewProductImportErrors(batchId){ try{ PRODUCT_IMPORT_ERRORS = await apiGet('/admin/products/import-batches/'+batchId+'/errors'); renderProductImportErrors(); setStatus('productImportStatus','已加载失败行详情。'); }catch(e){ setStatus('productImportStatus','加载失败行失败：'+e.message); } }
async function downloadProductImportErrors(batchId){ openAdminPath('/admin/products/import-batches/'+batchId+'/errors.xlsx'); }
async function retryProductImportBatch(batchId){ if(!confirm('确定对当前批次失败行执行二次导入吗？')) return; try{ const data=await apiPost('/admin/products/import-batches/'+batchId+'/retry', {}); setStatus('productImportStatus', `二次导入完成：新批次 ${data.batch_no}，成功 ${data.success_rows}，失败 ${data.failed_rows}。`); await loadProducts(); await loadProductImportBatches(); if(data.batch_id){ await viewProductImportErrors(data.batch_id); } }catch(e){ setStatus('productImportStatus','二次导入失败：'+e.message); } }
async function deleteProductImportBatch(batchId){ if(!confirm('确定删除当前导入批次记录吗？仅删除历史和错误记录，不会删除已导入成功的商品。')) return; try{ await apiDelete('/admin/products/import-batches/'+batchId); PRODUCT_IMPORT_ERRORS=[]; renderProductImportErrors(); await loadProductImportBatches(); setStatus('productImportStatus','导入批次已删除。'); }catch(e){ setStatus('productImportStatus','删除导入批次失败：'+e.message); } }

function renderLogisticsApiOverview(){ const el=$('logisticsApiOverview'); if(!el) return; const data=LOGISTICS_API_OVERVIEW; if(!data){ el.innerHTML='<div class="muted">暂无物流 / API 对接信息。</div>'; return; } const rows=data.rows||[]; let html=`<div class="muted">物流 Provider：<strong>${escapeHtml(data.provider_name||'-')}</strong>；${data.provider_key_configured?'<span class="tag ok">已配置 Key</span>':'<span class="tag warn">未配置 Key</span>'}</div>`; if(rows.length){ html += '<table class="space-top"><thead><tr><th>供应链</th><th>类型</th><th>API 地址</th><th>API Key</th><th>模板</th><th>发货 Bot</th><th>状态</th></tr></thead><tbody>'; for(const row of rows){ html += `<tr><td>${escapeHtml(row.supplier_code)} · ${escapeHtml(row.supplier_name||'')}</td><td>${escapeHtml(row.supplier_type||'-')}</td><td class="mono">${escapeHtml(row.api_base||'-')}</td><td>${row.api_key_configured?'<span class="tag ok">已配置</span>':'<span class="tag off">未配置</span>'}</td><td>${escapeHtml(row.template_type||'-')}</td><td class="mono">${escapeHtml(row.shipping_bot_code||'-')}</td><td>${row.is_active?'<span class="tag ok">启用</span>':'<span class="tag off">停用</span>'}</td></tr>`; } html += '</tbody></table>'; } else { html += '<div class="empty-box">暂无供应链 API 对接项。</div>'; } el.innerHTML=html; }
async function loadLogisticsApiOverview(){ try{ LOGISTICS_API_OVERVIEW = await apiGet('/admin/logistics/api-overview'); renderLogisticsApiOverview(); setStatus('logisticsApiStatus','物流/API 接口对接项已刷新。'); }catch(e){ setStatus('logisticsApiStatus','加载物流/API 对接项失败：'+e.message); } }


  // Categories
  function clearCategoryForm(){ $('categoryId').value=''; $('categoryName').value=''; $('categoryCoverImage').value=''; $('categorySortOrder').value=100; $('categoryIsActive').value='true'; }
  function editCategory(raw){ const row = decodeRowPayload(raw); $('categoryId').value=row.id||''; $('categoryName').value=row.name||''; $('categoryCoverImage').value=row.cover_image||''; $('categorySortOrder').value=row.sort_order||100; $('categoryIsActive').value=String(!!row.is_active); setStatus('categoriesStatus','已回填分类，可直接修改后保存。'); }
  async function saveCategory(){ try{ const payload={id:$('categoryId').value||null,name:$('categoryName').value.trim(),cover_image:$('categoryCoverImage').value.trim(),sort_order:Number($('categorySortOrder').value||100),is_active:$('categoryIsActive').value==='true'}; if(!payload.name){ setStatus('categoriesStatus','请先填写分类名称。'); return; } await apiPost('/admin/categories', payload); setStatus('categoriesStatus','分类已保存。'); clearCategoryForm(); await loadCategories(); }catch(e){ setStatus('categoriesStatus','保存失败：'+e.message); } }
  async function toggleCategory(id){ try{ await apiPost('/admin/categories/'+id+'/toggle', {}); await loadCategories(); }catch(e){ setStatus('categoriesStatus','状态切换失败：'+e.message); } }
  async function deleteCategory(id){ if(!confirm('确定删除该分类吗？')) return; try{ await apiDelete('/admin/categories/'+id); await loadCategories(); }catch(e){ setStatus('categoriesStatus','删除失败：'+e.message); } }
  function renderCategoriesTable(){ const el=$('categoriesTable'); if(!CATEGORIES.length){ el.innerHTML='<div class="muted">暂无分类</div>'; return; } let html='<table><thead><tr><th>ID</th><th>分类</th><th>封面</th><th>排序</th><th>状态</th><th>操作</th></tr></thead><tbody>'; for(const row of CATEGORIES){ html += `<tr><td>${row.id}</td><td>${escapeHtml(row.name)}</td><td>${row.cover_image ? `<img class="thumb" src="${escapeHtml(row.cover_image)}" />` : '<span class="muted">无图</span>'}</td><td>${row.sort_order}</td><td>${boolTag(row.is_active)}</td><td><div class="actions"><button class="small secondary" onclick="editCategory('${encodeRowPayload(row)}')">编辑</button><button class="small orange" onclick="toggleCategory(${row.id})">${row.is_active?'停用':'启用'}</button><button class="small red" onclick="deleteCategory(${row.id})">删除</button></div></td></tr>`; } html+='</tbody></table>'; el.innerHTML=html; }
  async function loadCategories(){ CATEGORIES = await apiGet('/admin/categories'); renderCategoriesTable(); const options = ['<option value="">未分类</option>'].concat(CATEGORIES.map(c=>`<option value="${c.id}">${escapeHtml(c.name)}</option>`)); $('productCategoryId').innerHTML = options.join(''); $('productSearchCategory').innerHTML = '<option value="all">全部分类</option>'+CATEGORIES.map(c=>`<option value="${c.id}">${escapeHtml(c.name)}</option>`).join(''); refreshOverview(); }

  // Products
  function defaultSkuFromRow(row){
    return {
      id: null,
      sku_code: row && row.sku_code ? row.sku_code : '',
      sku_name: row && (row.sku_code || row.name) ? (row.sku_code || row.name) : '默认规格',
      spec_text: '',
      price_cny: row && row.price_cny ? row.price_cny : 0,
      original_price_cny: row && row.original_price_cny ? row.original_price_cny : 0,
      stock_qty: row && row.stock_qty ? row.stock_qty : 0,
      weight_gram: row && row.weight_gram ? row.weight_gram : 0,
      unit_text: row && row.unit_text ? row.unit_text : '件',
      cover_image: '',
      is_active: row ? !!row.is_active : true,
      sort_order: row && row.sort_order ? row.sort_order : 100
    };
  }
  function createSkuRow(data){
    const row = Object.assign(defaultSkuFromRow(null), data||{});
    return `<div class="sku-row card space-top">
      <input class="sku-id" type="hidden" value="${escapeHtml(row.id||'')}" />
      <div class="grid">
        <div class="col-4"><label>SKU编码</label><input class="sku-code" value="${escapeHtml(row.sku_code||'')}" /></div>
        <div class="col-4"><label>SKU名称</label><input class="sku-name" value="${escapeHtml(row.sku_name||'')}" /></div>
        <div class="col-4"><label>规格说明</label><input class="sku-spec-text" value="${escapeHtml(row.spec_text||'')}" /></div>
      </div>
      <div class="grid">
        <div class="col-2"><label>售价</label><input class="sku-price" type="number" step="0.01" value="${escapeHtml(row.price_cny||0)}" /></div>
        <div class="col-2"><label>原价</label><input class="sku-original-price" type="number" step="0.01" value="${escapeHtml(row.original_price_cny||0)}" /></div>
        <div class="col-2"><label>库存</label><input class="sku-stock" type="number" value="${escapeHtml(row.stock_qty||0)}" /></div>
        <div class="col-2"><label>重量(g)</label><input class="sku-weight" type="number" value="${escapeHtml(row.weight_gram||0)}" /></div>
        <div class="col-2"><label>单位</label><input class="sku-unit" value="${escapeHtml(row.unit_text||'件')}" /></div>
        <div class="col-2"><label>排序</label><input class="sku-sort-order" type="number" value="${escapeHtml(row.sort_order||100)}" /></div>
      </div>
      <div class="actions space-top">
        <label><input class="sku-active" type="checkbox" ${row.is_active ? 'checked' : ''}/> 启用</label>
        <button type="button" class="small red" onclick="removeSkuRow(this)">删除SKU</button>
      </div>
    </div>`;
  }
  function renderSkuList(rows){
    const el = $('productSkuList');
    if(!el) return;
    const list = Array.isArray(rows) && rows.length ? rows : [defaultSkuFromRow(window.CURRENT_EDIT_PRODUCT||null)];
    el.innerHTML = list.map(createSkuRow).join('');
  }
  function addSkuRow(){
    const el = $('productSkuList');
    if(!el) return;
    el.insertAdjacentHTML('beforeend', createSkuRow(defaultSkuFromRow(null)));
  }
  function removeSkuRow(btn){
    const wrap = $('productSkuList');
    const box = btn.closest('.sku-row');
    if(!wrap) return;
    if(wrap.querySelectorAll('.sku-row').length <= 1){
      setStatus('productsStatus','至少保留一个 SKU。');
      return;
    }
    if(box) box.remove();
  }
  function collectSkuList(){
    const wrap = $('productSkuList');
    if(!wrap) return [];
    return [...wrap.querySelectorAll('.sku-row')].map(el => ({
      id: el.querySelector('.sku-id')?.value ? Number(el.querySelector('.sku-id').value) : null,
      sku_code: (el.querySelector('.sku-code')?.value || '').trim(),
      sku_name: (el.querySelector('.sku-name')?.value || '').trim(),
      spec_text: (el.querySelector('.sku-spec-text')?.value || '').trim(),
      price_cny: Number(el.querySelector('.sku-price')?.value || 0),
      original_price_cny: Number(el.querySelector('.sku-original-price')?.value || 0),
      stock_qty: Number(el.querySelector('.sku-stock')?.value || 0),
      weight_gram: Number(el.querySelector('.sku-weight')?.value || 0),
      unit_text: (el.querySelector('.sku-unit')?.value || '件').trim() || '件',
      cover_image: '',
      is_active: !!el.querySelector('.sku-active')?.checked,
      sort_order: Number(el.querySelector('.sku-sort-order')?.value || 100)
    }));
  }
  function clearProductForm(){ window.CURRENT_EDIT_PRODUCT = null; $('productId').value=''; $('productCategoryId').value=''; $('productName').value=''; $('productSubtitle').value=''; $('productSkuCode').value=''; $('productCoverImage').value=''; $('productPrice').value=0; $('productOriginalPrice').value=0; $('productStockQty').value=0; $('productWeightGram').value=0; $('productUnitText').value='件'; $('productDescription').value=''; $('productDetailHtml').value=''; $('productSortOrder').value=100; $('productIsActive').value='true'; $('productImageFile').value=''; previewImage('', 'productImagePreview', 'productImagePlaceholder'); renderSkuList([defaultSkuFromRow(null)]); }
  function editProduct(raw){ const row = decodeRowPayload(raw); window.CURRENT_EDIT_PRODUCT = row; $('productId').value=row.id||''; $('productCategoryId').value=row.category_id||''; $('productName').value=row.name||''; $('productSubtitle').value=row.subtitle||''; $('productSkuCode').value=row.sku_code||''; $('productCoverImage').value=row.cover_image||''; $('productPrice').value=row.price_cny||0; $('productOriginalPrice').value=row.original_price_cny||0; $('productStockQty').value=row.stock_qty||0; $('productWeightGram').value=row.weight_gram||0; $('productUnitText').value=row.unit_text||'件'; $('productDescription').value=row.description||''; $('productDetailHtml').value=row.detail_html||''; $('productSortOrder').value=row.sort_order||100; $('productIsActive').value=String(!!row.is_active); previewImage(row.cover_image||'', 'productImagePreview', 'productImagePlaceholder'); renderSkuList(Array.isArray(row.sku_list)&&row.sku_list.length?row.sku_list:[defaultSkuFromRow(row)]); setStatus('productsStatus','已回填商品，可直接修改后保存。'); window.scrollTo({top:0,behavior:'smooth'}); }
  async function uploadProductImage(){ try{ const file = $('productImageFile').files[0]; if(!file){ setStatus('productsStatus','请先选择图片文件。'); return; } const res = await apiUpload('/admin/products/upload-image', file); $('productCoverImage').value = res.url || ''; previewImage(res.url || '', 'productImagePreview', 'productImagePlaceholder'); setStatus('productsStatus','商品图片已上传。'); }catch(e){ setStatus('productsStatus','图片上传失败：'+e.message); } }
  $('productCoverImage').addEventListener('input', e => previewImage(e.target.value.trim(), 'productImagePreview', 'productImagePlaceholder'));
  let savingProduct = false;
  function productPublishTag(isActive){ return isActive ? '<span class="tag ok">已上架</span>' : '<span class="tag off">已下架</span>'; }
  function updateProductRowInCache(row){
    if(!row || !row.id){ return; }
    const idx = PRODUCTS.findIndex(x => Number(x.id) === Number(row.id));
    if(idx >= 0){ PRODUCTS[idx] = row; } else { PRODUCTS.unshift(row); }
  }
  async function safeReloadProductsAfterSave(successMessage){
    try{
      await loadProducts();
      setStatus('productsStatus', successMessage);
    }catch(e){
      setStatus('productsStatus', successMessage + '（列表刷新失败：' + e.message + '）');
    }
  }
  async function saveProduct(){ if(savingProduct){ return; } const btn=$('saveProductBtn'); const isEdit=!!$('productId').value; try{ const weightRaw = $('productWeightGram').value.trim(); const weightNumber = Number(weightRaw || 0); const payload={id:$('productId').value||null,category_id:$('productCategoryId').value?Number($('productCategoryId').value):null,name:$('productName').value.trim(),subtitle:$('productSubtitle').value.trim(),sku_code:$('productSkuCode').value.trim(),cover_image:$('productCoverImage').value.trim(),gallery_images_json:'[]',price_cny:Number($('productPrice').value||0),original_price_cny:Number($('productOriginalPrice').value||0),stock_qty:Number($('productStockQty').value||0),weight_gram:weightNumber,unit_text:$('productUnitText').value.trim()||'件',description:$('productDescription').value.trim(),detail_html:$('productDetailHtml').value.trim(),is_active:$('productIsActive').value==='true',sort_order:Number($('productSortOrder').value||100),sku_list:collectSkuList()}; if(!payload.name){ setStatus('productsStatus','请先填写商品名称。'); return; } if(!payload.sku_list || !payload.sku_list.length){ setStatus('productsStatus','至少保留一个 SKU。'); return; } if(!payload.sku_list.some(x=>!!x.is_active)){ setStatus('productsStatus','至少启用一个 SKU。'); return; } if(payload.sku_list.some(x=>!(x.sku_name||'').trim())){ setStatus('productsStatus','SKU 名称不能为空。'); return; } if(payload.price_cny<=0){ setStatus('productsStatus','商品售价必须大于 0。'); return; } if(payload.stock_qty<0){ setStatus('productsStatus','库存不能小于 0。'); return; } if(weightRaw && (!Number.isInteger(weightNumber) || weightNumber < 0)){ setStatus('productsStatus','重量(g) 必须填写整数，例如 100。'); return; } if(payload.cover_image){ payload.cover_image = normalizeMediaUrl(payload.cover_image).replace(getApiBase(), '') || payload.cover_image; } savingProduct = true; if(btn){ btn.disabled = true; btn.textContent = '保存中...'; } const res = await apiPost('/admin/products', payload); const savedRow = (res && (res.item || res.data)) ? (res.item || res.data) : null; if(savedRow){ updateProductRowInCache(savedRow); } clearProductForm(); await safeReloadProductsAfterSave(isEdit ? '商品已更新。' : '商品已创建。'); }catch(e){ setStatus('productsStatus','保存失败：'+e.message); } finally { savingProduct = false; if(btn){ btn.disabled = false; btn.textContent = ' 保存商品'; } } }
  async function toggleProduct(id){ try{ const res = await apiPost('/admin/products/'+id+'/toggle', {}); const row = res && (res.item || res.data); if(row){ updateProductRowInCache(row); renderProductsTable(); } try{ await loadProducts(); }catch(_){} setStatus('productsStatus', (row && row.is_active) ? '商品已上架。' : '商品已下架。'); }catch(e){ setStatus('productsStatus','状态切换失败：'+e.message); } }
  async function moveProductUp(id){ try{ await apiPost('/admin/products/'+id+'/move-up', {}); await loadProducts(); }catch(e){ setStatus('productsStatus','上移失败：'+e.message); } }
  async function moveProductDown(id){ try{ await apiPost('/admin/products/'+id+'/move-down', {}); await loadProducts(); }catch(e){ setStatus('productsStatus','下移失败：'+e.message); } }
  async function deleteProduct(id){ if(!confirm('确定删除该商品吗？')) return; try{ const res = await apiDelete('/admin/products/'+id); PRODUCTS = PRODUCTS.filter(r=>Number(r.id)!==Number(id)); renderProductsTable(); try{ await loadProducts(); }catch(_){} setStatus('productsStatus',(res && res.message) ? res.message : '商品已删除。'); }catch(e){ setStatus('productsStatus','删除失败：'+e.message); } }
  function renderProductsTable(){ const kw=$('productSearchKeyword').value.trim().toLowerCase(); const cat=$('productSearchCategory').value; const status=$('productSearchStatus').value; let rows=PRODUCTS.slice(); if(kw){ rows=rows.filter(r=>[r.name,r.subtitle,r.sku_code,(Array.isArray(r.sku_list)?('SKU '+r.sku_list.length+'个'):'')].join(' ').toLowerCase().includes(kw)); } if(cat!=='all'){ rows=rows.filter(r=>String(r.category_id||'')===cat); } if(status==='active'){ rows=rows.filter(r=>!!r.is_active); } if(status==='inactive'){ rows=rows.filter(r=>!r.is_active); } const el=$('productsTable'); if(!rows.length){ el.innerHTML='<div class="empty-box">暂无商品，请先新增并保存一条商品记录。</div>'; return; } let html='<table><thead><tr><th>图</th><th>商品</th><th>分类</th><th>售价</th><th>库存</th><th>排序</th><th>上架状态</th><th>操作</th></tr></thead><tbody>'; for(const row of rows){ html += `<tr><td>${row.cover_image ? `<img class="thumb" src="${escapeHtml(row.cover_image)}" />` : '<span class="muted">无 图</span>'}</td><td><div><strong>${escapeHtml(row.name)}</strong></div><div class="muted">${escapeHtml(row.subtitle || (Array.isArray(row.sku_list)&&row.sku_list.length?('SKU '+row.sku_list.length+'个'):row.sku_code) || '')}</div></td><td>${escapeHtml(row.category_name||'未分类')}</td><td>¥ ${escapeHtml(row.price_cny)}</td><td>${escapeHtml(row.stock_qty)}</td><td>${escapeHtml(row.sort_order)}</td><td>${productPublishTag(!!row.is_active)}</td><td><div class="actions"><button class="small secondary" onclick="editProduct('${encodeRowPayload(row)}')">编辑</button><button class="small orange" onclick="toggleProduct(${row.id})">${row.is_active?'下架':'上架'}</button><button class="small" onclick="moveProductUp(${row.id})">上移</button><button class="small" onclick="moveProductDown(${row.id})">下移</button><button class="small red" onclick="deleteProduct(${row.id})">删除</button></div></td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
  async function loadProducts(){ PRODUCTS = await apiGet('/admin/products'); renderProductsTable(); await loadProductImportBatches(); if(typeof fillSupplierSelects==='function'){ try{ fillSupplierSelects(); }catch(_){ } } refreshOverview(); }

  // Orders
  function logisticsShipStatusText(s){ const k=String(s||'').toLowerCase(); const m={pending:'待揽收',shipped:'运输中',signed:'已签收',returned:'退回'}; return m[k]||(s||'-'); }
  function debounceOrdersRefresh(){ if(ORDER_REFRESH_TIMER){ clearTimeout(ORDER_REFRESH_TIMER); } ORDER_REFRESH_TIMER = setTimeout(()=>refreshOrders(true), 260); }
  async function refreshOrders(resetPage=false){ try{ if(resetPage){ ORDER_PAGE.page = 1; } const supplier=$('orderSearchSupplier') ? $('orderSearchSupplier').value : 'all'; const data = await apiGet('/admin/orders', {search:$('orderSearchKeyword').value.trim(), pay_status:$('orderSearchPayStatus').value, delivery_status:$('orderSearchDeliveryStatus').value, supplier_code:supplier, page:ORDER_PAGE.page, page_size:ORDER_PAGE.page_size}); ORDERS = data.rows || []; ORDER_PAGE = {...ORDER_PAGE, page:data.page || 1, page_size:data.page_size || ORDER_PAGE.page_size, total:data.total || 0, total_pages:data.total_pages || 1, has_prev:!!data.has_prev, has_next:!!data.has_next}; renderOrdersTable(); refreshOverview(); }catch(e){ setStatus('ordersStatus','加载订单失败：'+e.message); } }
  function changeOrdersPage(delta){ const next = Math.max(1, Number(ORDER_PAGE.page||1) + Number(delta||0)); if(next === Number(ORDER_PAGE.page||1)) return; ORDER_PAGE.page = next; refreshOrders(false); }
  function renderOrdersTable(){ const el=$('ordersTable'); if(!ORDERS.length){ el.innerHTML='<div class="muted">暂无订单</div>'; return; } let html='<table><thead><tr><th>订单号</th><th>客户</th><th>供应链</th><th>金额</th><th>支付</th><th>发货</th><th>单号</th><th>时间</th><th>操作</th></tr></thead><tbody>'; for(const row of ORDERS){ html += `<tr><td class="mono">${escapeHtml(row.order_no)}</td><td><div>${escapeHtml(row.customer_name)}</div><div class="muted">${escapeHtml(row.customer_phone)}</div></td><td class="mono">${escapeHtml(row.supplier_code || '-')}</td><td>¥ ${escapeHtml(row.payable_amount)}</td><td>${payTag(row.pay_status)}</td><td>${deliveryTag(row.delivery_status)}</td><td class="mono">${escapeHtml(row.tracking_no || '-')}</td><td>${escapeHtml(formatTime(row.created_at))}</td><td><div class="actions"><button class="small secondary" onclick="loadOrderDetail(${row.id})">详情</button><button class="small green" onclick="quickMarkPaid(${row.id})">已支付</button><button class="small orange" onclick="loadOrderDetail(${row.id})">发货</button></div></td></tr>`; } html += `</tbody></table><div class="space-top muted">第 ${ORDER_PAGE.page||1} / ${ORDER_PAGE.total_pages||1} 页，共 ${ORDER_PAGE.total||0} 条订单</div><div class="actions space-top"><button class="small secondary" ${ORDER_PAGE.has_prev?'':'disabled'} onclick="changeOrdersPage(-1)">上一页</button><button class="small secondary" ${ORDER_PAGE.has_next?'':'disabled'} onclick="changeOrdersPage(1)">下一页</button></div>`; el.innerHTML=html; }
  async function quickMarkPaid(id){ try{ await apiPost('/admin/orders/'+id+'/mark-paid', {}); await refreshOrders(); if(CURRENT_ORDER && CURRENT_ORDER.id===id) await loadOrderDetail(id); }catch(e){ setStatus('ordersStatus','标记支付失败：'+e.message); } }
  async function loadOrderDetail(id){ try{ CURRENT_ORDER = await apiGet('/admin/orders/'+id); $('orderDetailEmpty').style.display='none'; $('orderDetailBox').style.display='block'; $('orderDetailBasic').innerHTML = `<div><strong>订单号：</strong><span class="mono">${escapeHtml(CURRENT_ORDER.order_no)}</span></div><div><strong>买家：</strong>${escapeHtml(CURRENT_ORDER.customer_name)} / ${escapeHtml(CURRENT_ORDER.customer_phone)}</div><div><strong>地址：</strong>${escapeHtml([CURRENT_ORDER.province,CURRENT_ORDER.city,CURRENT_ORDER.district,CURRENT_ORDER.address_detail].filter(Boolean).join(' '))}</div><div><strong>状态：</strong>${payTag(CURRENT_ORDER.pay_status)} ${deliveryTag(CURRENT_ORDER.delivery_status)}</div><div><strong>当前供应链：</strong><span class="mono">${escapeHtml(CURRENT_ORDER.supplier_code || '-')}</span></div><div><strong>供应链推单：</strong>${CURRENT_ORDER.fulfillment && CURRENT_ORDER.fulfillment.sync_status==='synced' ? '<span class="tag ok">已回查</span>' : '<span class="tag warn">待推送/待回查</span>'}</div><div><strong>创建时间：</strong>${escapeHtml(formatTime(CURRENT_ORDER.created_at))}</div><div><strong>买家备注：</strong>${escapeHtml(CURRENT_ORDER.buyer_remark || '-')}</div>`; const pay = CURRENT_ORDER.payment; $('orderDetailPayment').innerHTML = pay ? `<div><strong>支付方式：</strong>${escapeHtml(pay.pay_method)}</div><div><strong>收款地址：</strong><span class="mono">${escapeHtml(pay.receive_address)}</span></div><div><strong>应付金额：</strong>${escapeHtml(pay.expected_amount)}</div><div><strong>支付状态：</strong>${escapeHtml(pay.confirm_status)}</div><div><strong>交易哈希：</strong><span class="mono">${escapeHtml(pay.txid || '-')}</span></div>` : '<div class="muted">暂无支付单信息</div>'; $('orderDetailItems').innerHTML = '<strong>商品明细</strong>' + (CURRENT_ORDER.items && CURRENT_ORDER.items.length ? `<table class="space-top"><thead><tr><th>商品</th><th>SKU</th><th>数量</th><th>单价</th><th>小计</th></tr></thead><tbody>${CURRENT_ORDER.items.map(i=>`<tr><td>${escapeHtml(i.product_name)}</td><td>${escapeHtml(i.sku_code || '')}</td><td>${escapeHtml(i.qty)}</td><td>${escapeHtml(i.unit_price)}</td><td>${escapeHtml(i.subtotal)}</td></tr>`).join('')}</tbody></table>` : '<div class="muted">暂无商品明细</div>'); $('shipCourierCompany').value = CURRENT_ORDER.courier_company || ''; $('shipCourierCode').value = CURRENT_ORDER.courier_code || ''; $('shipTrackingNo').value = CURRENT_ORDER.tracking_no || ''; $('orderSellerRemark').value = CURRENT_ORDER.seller_remark || ''; if($('orderAssignSupplier')){ $('orderAssignSupplier').innerHTML = '<option value="">请选择供应链</option>' + (CURRENT_ORDER.available_suppliers||[]).map(s=>`<option value="${s.id}" ${CURRENT_ORDER.fulfillment && CURRENT_ORDER.fulfillment.supplier_id===s.id?'selected':''}>${escapeHtml(s.supplier_code)} · ${escapeHtml(s.supplier_name)}</option>`).join(''); } if($('orderFulfillmentInfo')){ $('orderFulfillmentInfo').innerHTML = CURRENT_ORDER.fulfillment ? `当前履约：<strong>${escapeHtml(CURRENT_ORDER.fulfillment.supplier_code)} · ${escapeHtml(CURRENT_ORDER.fulfillment.supplier_name)}</strong><div class="muted">状态：${escapeHtml(CURRENT_ORDER.fulfillment.fulfillment_status || '-')}</div><div class="muted">分配时间：${escapeHtml(formatTime(CURRENT_ORDER.fulfillment.assigned_at))}</div><div class="muted">同步状态：${escapeHtml(CURRENT_ORDER.fulfillment.sync_status || '-')}</div><div class="muted">说明：${escapeHtml(CURRENT_ORDER.fulfillment.sync_error || '无')}</div><div class="muted">供应链单号：${escapeHtml(CURRENT_ORDER.fulfillment.supplier_order_no || '-')}</div>` : '<span class="muted">当前未分配供应链</span>'; } const _sh=CURRENT_ORDER.shipment; if($('orderDetailShipment')) $('orderDetailShipment').innerHTML=_sh?`<div class=\"card-inset\"><strong>物流同步</strong><div><strong>当前状态：</strong>${escapeHtml(logisticsShipStatusText(_sh.ship_status))}</div><div><strong>最新轨迹：</strong>${escapeHtml(_sh.last_trace_text||'-')}</div><div><strong>最近同步：</strong>${escapeHtml(formatTime(_sh.last_sync_at))}</div></div>`:''; setStatus('orderDetailStatus','订单详情已加载。'); window.scrollTo({top:document.body.scrollHeight, behavior:'smooth'}); }catch(e){ setStatus('ordersStatus','加载详情失败：'+e.message); } }
  async function markOrderPaid(){ if(!CURRENT_ORDER) return; try{ await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/mark-paid', {}); await refreshOrders(); await loadOrderDetail(CURRENT_ORDER.id); }catch(e){ setStatus('orderDetailStatus','标记已支付失败：'+e.message); } }
  async function simulateOrderPaid(){
    if(!CURRENT_ORDER) return;
    const pay = CURRENT_ORDER.payment || null;
    const orderStatus = String(CURRENT_ORDER.order_status || '').trim().toLowerCase();
    const payStatus = String(CURRENT_ORDER.pay_status || '').trim().toLowerCase();
    const confirmStatus = String((pay && pay.confirm_status) || '').trim().toLowerCase();
    if(orderStatus === 'cancelled'){
      setStatus('orderDetailStatus','已取消订单不允许模拟支付成功');
      return;
    }
    if(payStatus === 'paid'){
      setStatus('orderDetailStatus','订单已支付，无需模拟确认');
      return;
    }
    if(!pay){
      setStatus('orderDetailStatus','该订单暂无支付单');
      return;
    }
    if(['confirmed','paid','success'].includes(confirmStatus)){
      setStatus('orderDetailStatus','支付单已确认，无需模拟确认');
      return;
    }
    if(!confirm('确定模拟该订单支付成功吗？此操作仅用于测试。')) return;
    try{
      await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/simulate-paid', {});
      await refreshOrders();
      await loadOrderDetail(CURRENT_ORDER.id);
      setStatus('orderDetailStatus','已模拟支付成功。');
    }catch(e){
      setStatus('orderDetailStatus','模拟支付成功失败：'+e.message);
    }
  }
  async function shipOrder(){ if(!CURRENT_ORDER) return; try{ const payload={courier_company:$('shipCourierCompany').value.trim(), courier_code:$('shipCourierCode').value.trim(), tracking_no:$('shipTrackingNo').value.trim()}; if(!payload.courier_company || !payload.tracking_no){ setStatus('orderDetailStatus','请先填写快递公司和快递单号。'); return; } await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/ship', payload); await refreshOrders(); await loadOrderDetail(CURRENT_ORDER.id); setStatus('orderDetailStatus','已录入发货信息。'); }catch(e){ setStatus('orderDetailStatus','录入发货失败：'+e.message); } }
  async function saveOrderRemark(){ if(!CURRENT_ORDER) return; try{ await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/remark', {seller_remark:$('orderSellerRemark').value.trim()}); await refreshOrders(); await loadOrderDetail(CURRENT_ORDER.id); setStatus('orderDetailStatus','备注已保存。'); }catch(e){ setStatus('orderDetailStatus','保存备注失败：'+e.message); } }
  async function completeOrder(){ if(!CURRENT_ORDER) return; try{ await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/complete', {}); await refreshOrders(); await loadOrderDetail(CURRENT_ORDER.id); }catch(e){ setStatus('orderDetailStatus','标记完成失败：'+e.message); } }
  async function cancelOrder(){ if(!CURRENT_ORDER) return; if(!confirm('确定取消这张订单吗？')) return; try{ await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/cancel', {}); await refreshOrders(); await loadOrderDetail(CURRENT_ORDER.id); }catch(e){ setStatus('orderDetailStatus','取消订单失败：'+e.message); } }


  async function assignOrderSupplier(){ if(!CURRENT_ORDER) return; const supplierId = Number($('orderAssignSupplier').value||0); if(!supplierId){ setStatus('orderDetailStatus','请先选择要指派的供应链。'); return; } try{ const res = await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/assign-supplier', {supplier_id:supplierId}); await refreshOrders(); await loadOrderDetail(CURRENT_ORDER.id); setStatus('orderDetailStatus','已手动指派供应链：' + (res.data?.supplier_code || '')); }catch(e){ setStatus('orderDetailStatus','手动指派失败：'+e.message); } }
  async function autoAssignOrderSupplier(){ if(!CURRENT_ORDER) return; try{ const res = await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/auto-assign-supplier', {}); await refreshOrders(); await loadOrderDetail(CURRENT_ORDER.id); setStatus('orderDetailStatus','自动路由结果：' + (res.reason || '已完成')); }catch(e){ setStatus('orderDetailStatus','自动指派失败：'+e.message); } }
  async function pushOrderToSupplier(){ if(!CURRENT_ORDER) return; try{ const res = await apiPost('/admin/orders/'+CURRENT_ORDER.id+'/push-supplier', {}); await loadOrderDetail(CURRENT_ORDER.id); setStatus('orderDetailStatus','供应链推单骨架已生成：' + (res.supplier_code || '')); }catch(e){ setStatus('orderDetailStatus','推送供应链失败：'+e.message); } }
  async function pullSupplierStatus(){ if(!CURRENT_ORDER || !CURRENT_ORDER.fulfillment){ setStatus('orderDetailStatus','当前订单还没有履约记录。'); return; } try{ const res = await apiPost('/admin/order-fulfillments/'+CURRENT_ORDER.fulfillment.id+'/pull-supplier-status', {}); await loadOrderDetail(CURRENT_ORDER.id); setStatus('orderDetailStatus','供应链状态已回查：' + (res.fulfillment_status || '')); }catch(e){ setStatus('orderDetailStatus','拉取履约状态失败：'+e.message); } }
  async function previewSupplierPayload(){ if(!CURRENT_ORDER) return; try{ const res = await apiGet('/admin/orders/'+CURRENT_ORDER.id+'/supplier-payload'); const payload = JSON.stringify(res.payload || {}, null, 2); alert('供应链载荷预览\n\n'+payload); }catch(e){ alert('预览失败：'+e.message); } }
  async function syncOrderLogistics(orderId){ const oid = orderId || (CURRENT_ORDER && CURRENT_ORDER.id); if(!oid){ setStatus('ordersStatus','请先打开订单详情再同步物流。'); return; } try{ await apiPost('/admin/orders/'+oid+'/sync-logistics', {}); if(CURRENT_ORDER && CURRENT_ORDER.id===oid) await loadOrderDetail(oid); await loadLogisticsCenter(); setStatus('ordersStatus','物流同步已触发。'); }catch(e){ setStatus('ordersStatus','同步物流失败：'+e.message); } }

  // Payment addresses
  function clearPaymentAddressForm(){ $('paymentAddressId').value=''; $('paymentAddressLabel').value=''; $('paymentAddressValue').value=''; $('paymentQrImage').value=''; $('paymentSortOrder').value=100; $('paymentIsActive').value='true'; $('paymentQrFile').value=''; previewImage('', 'paymentQrPreview', 'paymentQrPlaceholder'); }
  function editPaymentAddress(raw){ const row = decodeRowPayload(raw); $('paymentAddressId').value=row.id||''; $('paymentAddressLabel').value=row.address_label||''; $('paymentAddressValue').value=row.address||''; $('paymentQrImage').value=row.qr_image||''; $('paymentSortOrder').value=row.sort_order||100; $('paymentIsActive').value=String(!!row.is_active); previewImage(row.qr_image||'', 'paymentQrPreview', 'paymentQrPlaceholder'); setStatus('paymentStatus','已回填支付地址，可直接修改后保存。'); window.scrollTo({top:0, behavior:'smooth'}); }
  $('paymentQrImage').addEventListener('input', e => previewImage(e.target.value.trim(), 'paymentQrPreview', 'paymentQrPlaceholder'));
  async function uploadPaymentQr(){ try{ const file = $('paymentQrFile').files[0]; if(!file){ setStatus('paymentStatus','请先选择二维码图片。'); return; } const res = await apiUpload('/admin/payment-addresses/upload-image', file); $('paymentQrImage').value = res.url || ''; previewImage(res.url || '', 'paymentQrPreview', 'paymentQrPlaceholder'); setStatus('paymentStatus','二维码图片已上传。'); }catch(e){ setStatus('paymentStatus','二维码上传失败：'+e.message); } }
  async function savePaymentAddress(){ try{ const payload={id:$('paymentAddressId').value||null,address_label:$('paymentAddressLabel').value.trim(),address:$('paymentAddressValue').value.trim(),qr_image:$('paymentQrImage').value.trim(),is_active:$('paymentIsActive').value==='true',sort_order:Number($('paymentSortOrder').value||100)}; if(!payload.address){ setStatus('paymentStatus','请先填写收款地址。'); return; } await apiPost('/admin/payment-addresses', payload); setStatus('paymentStatus','支付地址已保存。'); clearPaymentAddressForm(); await loadPaymentAddresses(); }catch(e){ setStatus('paymentStatus','保存失败：'+e.message); } }
  async function togglePaymentAddress(id){ try{ await apiPost('/admin/payment-addresses/'+id+'/toggle', {}); await loadPaymentAddresses(); }catch(e){ setStatus('paymentStatus','状态切换失败：'+e.message); } }
  async function deletePaymentAddress(id){ if(!confirm('确定删除该支付地址吗？')) return; try{ await apiDelete('/admin/payment-addresses/'+id); await loadPaymentAddresses(); }catch(e){ setStatus('paymentStatus','删除失败：'+e.message); } }
  function renderPaymentAddressesTable(){ const el=$('paymentAddressesTable'); if(!PAYMENT_ADDRESSES.length){ el.innerHTML='<div class="muted">暂无支付地址</div>'; return; } let html='<table><thead><tr><th>二维码</th><th>标签</th><th>收款地址</th><th>排序</th><th>状态</th><th>操作</th></tr></thead><tbody>'; for(const row of PAYMENT_ADDRESSES){ html += `<tr><td>${row.qr_image ? `<img class="thumb" src="${escapeHtml(row.qr_image)}" />` : '<span class="muted">无图</span>'}</td><td>${escapeHtml(row.address_label || '-')}</td><td class="mono">${escapeHtml(row.address)}</td><td>${escapeHtml(row.sort_order)}</td><td>${boolTag(row.is_active)}</td><td><div class="actions"><button class="small secondary" onclick="editPaymentAddress('${encodeRowPayload(row)}')">编辑</button><button class="small orange" onclick="togglePaymentAddress(${row.id})">${row.is_active?'停用':'启用'}</button><button class="small red" onclick="deletePaymentAddress(${row.id})">删除</button></div></td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
  async function loadPaymentAddresses(){ PAYMENT_ADDRESSES = await apiGet('/admin/payment-addresses'); renderPaymentAddressesTable(); refreshOverview(); }


  // Logistics center
  function renderLogisticsTable(){ const kw=$('logisticsSearchKeyword').value.trim().toLowerCase(); const supplier=$('logisticsSearchSupplier').value; const status=$('logisticsSearchStatus').value; let rows=LOGISTICS_ROWS.slice(); if(kw){ rows=rows.filter(r=>[r.order_no,r.tracking_no,r.courier_company,r.customer_name].join(' ').toLowerCase().includes(kw)); } if(supplier && supplier!=='all'){ rows=rows.filter(r=>(r.supplier_code||'')===supplier); } if(status && status!=='all'){ rows=rows.filter(r=>(r.ship_status||'')===status); } const el=$('logisticsTable'); if(!rows.length){ el.innerHTML='<div class="muted">暂无物流数据</div>'; return; } let html='<table><thead><tr><th>订单号</th><th>供应链</th><th>快递公司</th><th>单号</th><th>当前状态</th><th>同步状态</th><th>最新轨迹</th><th>最近同步</th><th>操作</th></tr></thead><tbody>'; for(const row of rows){ html += `<tr><td class="mono">${escapeHtml(row.order_no)}</td><td>${escapeHtml(row.supplier_code||'-')}</td><td>${escapeHtml(row.courier_company||'-')}<div class="muted mono">${escapeHtml(row.courier_code||'-')}</div></td><td class="mono">${escapeHtml(row.tracking_no||'-')}</td><td>${escapeHtml(logisticsShipStatusText(row.ship_status))}</td><td>${escapeHtml(row.sync_status||'-')}${row.sync_error?`<div class="muted">${escapeHtml(row.sync_error)}</div>`:''}</td><td>${escapeHtml(row.last_trace_text||'-')}</td><td>${escapeHtml(formatTime(row.last_sync_at))}</td><td><div class="actions"><button class="small secondary" onclick="showLogisticsDetail(${row.shipment_id})">详情</button><button class="small" onclick="syncLogistics(${row.shipment_id})">同步</button></div></td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
  async function showLogisticsDetail(shipmentId){ try{ const row = await apiGet('/admin/logistics/'+shipmentId); const traces = row.traces && row.traces.length ? row.traces.slice(0,10).map(t=>`${formatTime(t.trace_time)}｜${t.trace_text}`).join('\n') : '暂无轨迹'; alert(`订单号：${row.order_no}\n供应链：${row.supplier_code||'-'}\n快递：${row.courier_company||'-'} (${row.courier_code||'-'})\n单号：${row.tracking_no||'-'}\n当前状态：${logisticsShipStatusText(row.ship_status)}\n同步状态：${row.sync_status||'-'}\n最新轨迹：${row.last_trace_text||'-'}\n最近同步：${formatTime(row.last_sync_at)}\n\n明细轨迹：\n${traces}`); }catch(e){ setStatus('logisticsStatus','加载物流详情失败：'+e.message); } }
  async function syncLogistics(shipmentId){ try{ const data = await apiPost('/admin/logistics/'+shipmentId+'/sync', {}); setStatus('logisticsStatus', '物流同步结果：' + (data.message || data.last_trace_text || '已完成')); await loadLogisticsCenter(); }catch(e){ setStatus('logisticsStatus','同步物流失败：'+e.message); } }
async function loadLogisticsCenter(){ try{ LOGISTICS_ROWS = await apiGet('/admin/logistics'); $('logisticsSearchSupplier').innerHTML = '<option value="all">全部供应链</option>' + SUPPLIERS.map(s=>`<option value="${s.supplier_code}">${escapeHtml(s.supplier_code)} · ${escapeHtml(s.supplier_name)}</option>`).join(''); renderLogisticsTable(); await Promise.all([loadLogisticsApiOverview(), loadLogisticsAlertsCenter()]); setStatus('logisticsStatus','物流中心已刷新。'); }catch(e){ setStatus('logisticsStatus','加载物流失败：'+e.message); } }

  function logisticsAlertTag(level){ const map={red:'<span class="tag red">红色</span>', orange:'<span class="tag orange">橙色</span>', yellow:'<span class="tag warn">黄色</span>'}; return map[level] || '<span class="tag off">普通</span>'; }
  function exportLogisticsAlertsTemplate(){ const params = new URLSearchParams(); const supplier = $('logisticsSearchSupplier') ? $('logisticsSearchSupplier').value : 'all'; const level = $('logisticsAlertLevel') ? $('logisticsAlertLevel').value : 'all'; if(supplier && supplier !== 'all'){ params.set('supplier_code', supplier); } if(level && level !== 'all'){ params.set('level', level); } openAdminPath('/admin/logistics/alerts/export.xlsx' + (params.toString() ? ('?' + params.toString()) : '')); }
  function renderLogisticsAlertsTable(){ const data = LOGISTICS_ALERTS || {overview:{}, rows:[]}; const overview = data.overview || {}; const byLevel = overview.by_level || {}; const byType = overview.by_type || {}; $('logisticsAlertTotal').textContent = Number(overview.total||0); $('logisticsAlertRed').textContent = Number(byLevel.red||0); $('logisticsAlertOrange').textContent = Number(byLevel.orange||0); $('logisticsAlertYellow').textContent = Number(byLevel.yellow||0); const el = $('logisticsAlertsTable'); const rows = data.rows || []; if(!rows.length){ el.innerHTML='<div class="empty-box">当前没有命中物流预警</div>'; return; } let html='<table><thead><tr><th>等级</th><th>预警项</th><th>订单号</th><th>供应链</th><th>快递/单号</th><th>预警说明</th><th>持续时长</th><th>最近时间</th></tr></thead><tbody>'; for(const row of rows){ const supplierText = [row.supplier_code||'', row.supplier_name||''].filter(Boolean).join(' · ') || '-'; const supplierContact = [row.supplier_contact_name||'', row.supplier_contact_phone||''].filter(Boolean).join(' / ') || (row.supplier_contact_tg||''); html += `<tr><td>${logisticsAlertTag(row.alert_level)}</td><td>${escapeHtml(row.alert_name||row.alert_type||'-')}</td><td class="mono">${escapeHtml(row.order_no||'-')}<div class="muted">${escapeHtml(row.customer_name||'-')}</div><div class="muted">${escapeHtml(row.product_summary||'-')}</div></td><td>${escapeHtml(supplierText)}<div class="muted">${escapeHtml(supplierContact||'-')}</div><div class="muted mono">供应链单号：${escapeHtml(row.supplier_order_no||'-')}</div></td><td>${escapeHtml(row.courier_company||'-')}<div class="muted mono">${escapeHtml(row.tracking_no||'-')}</div></td><td>${escapeHtml(row.alert_text||'-')}</td><td>${escapeHtml(row.age_hours||0)} 小时</td><td>${escapeHtml(formatTime(row.last_time||row.updated_at))}</td></tr>`; } html += '</tbody></table>'; const tips = [`未发货超时 ${Number(byType.not_shipped_timeout||0)}`, `首轨迹超时 ${Number(byType.no_first_trace||0)}`, `轨迹停滞 ${Number(byType.trace_stagnant||0)}`, `同步异常 ${Number(byType.sync_error||0)}`, `异常件 ${Number(byType.logistics_exception||0)}`]; el.innerHTML = html + `<div class="muted space-top">预警分布：${tips.join(' ｜ ')}。点击右上角“导出核对表”可把当前筛选结果发给对应供应链核对。</div>`; }
  function onLogisticsSupplierChange(){ renderLogisticsTable(); loadLogisticsAlertsCenter(); }
  async function loadLogisticsAlertsCenter(){ try{ const params = new URLSearchParams(); const supplier = $('logisticsSearchSupplier') ? $('logisticsSearchSupplier').value : 'all'; const level = $('logisticsAlertLevel') ? $('logisticsAlertLevel').value : 'all'; if(supplier && supplier !== 'all'){ params.set('supplier_code', supplier); } if(level && level !== 'all'){ params.set('level', level); } params.set('limit', '120'); const query = params.toString() ? ('?' + params.toString()) : ''; LOGISTICS_ALERTS = await apiGet('/admin/logistics/alerts' + query); renderLogisticsAlertsTable(); setStatus('logisticsAlertsStatus', `物流预警已刷新：共 ${Number((LOGISTICS_ALERTS.overview||{}).total||0)} 条，红 ${Number((((LOGISTICS_ALERTS.overview||{}).by_level)||{}).red||0)} / 橙 ${Number((((LOGISTICS_ALERTS.overview||{}).by_level)||{}).orange||0)} / 黄 ${Number((((LOGISTICS_ALERTS.overview||{}).by_level)||{}).yellow||0)}。`); }catch(e){ setStatus('logisticsAlertsStatus', '加载物流预警失败：' + e.message); } }

  // Shipping center
  function renderShipmentPreview(targetId, rows, emptyText){ const el=$(targetId); if(!rows || !rows.length){ el.innerHTML=`<div class="muted">${emptyText}</div>`; return; } let html='<table><thead><tr><th>订单号</th><th>客户</th><th>供应链</th><th>状态</th><th>单号</th></tr></thead><tbody>'; for(const row of rows){ html += `<tr><td class="mono">${escapeHtml(row.order_no)}</td><td>${escapeHtml(row.customer_name||'-')}</td><td>${escapeHtml(row.supplier_code||'-')}</td><td>${deliveryTag(row.delivery_status||'not_shipped')}</td><td class="mono">${escapeHtml(row.tracking_no||'-')}</td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
  function renderShipmentImportBatches(){ const el=$('shipmentImportBatchesTable'); if(!SHIPMENT_IMPORT_BATCHES.length){ el.innerHTML='<div class="muted">暂无导入批次</div>'; return; } let html='<table><thead><tr><th>批次号</th><th>日期</th><th>供应链</th><th>操作人</th><th>总行数</th><th>成功</th><th>失败</th><th>创建时间</th><th>操作</th></tr></thead><tbody>'; for(const row of SHIPMENT_IMPORT_BATCHES){ html += `<tr><td class="mono">${escapeHtml(row.batch_no)}</td><td>${escapeHtml(row.biz_date||'-')}</td><td>${escapeHtml(row.supplier_code||'-')}</td><td>${escapeHtml(row.operator_name||'-')}</td><td>${escapeHtml(row.total_rows)}</td><td>${escapeHtml(row.success_rows)}</td><td>${escapeHtml(row.failed_rows)}</td><td>${escapeHtml(formatTime(row.created_at))}</td><td><button class="small secondary" onclick="showImportErrors(${row.id})">失败行</button></td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; $('importBatchCount').textContent = SHIPMENT_IMPORT_BATCHES.length; $('importFailedCount').textContent = SHIPMENT_IMPORT_BATCHES.reduce((n,r)=>n+Number(r.failed_rows||0),0); }
  async function showImportErrors(batchId){ try{ const rows = await apiGet('/admin/shipments/import-batches/'+batchId+'/errors'); if(!rows.length){ alert('该批次没有失败行'); return; } alert(rows.map(r=>`第${r.row_no}行｜${r.order_no||'-'}｜${r.tracking_no||'-'}｜${r.error_message}`).join('\n')); }catch(e){ alert('加载失败行失败：'+e.message); } }
  async function downloadShipmentTemplate(){ openAdminPath('/admin/shipments/import-template'); }
  async function exportPendingShipments(){ const supplierCode = $('shipmentImportSupplierCode').value.trim(); const path = '/admin/shipments/export-pending' + (supplierCode ? ('?supplier_code='+encodeURIComponent(supplierCode)) : ''); openAdminPath(path); }
  async function exportShippedShipments(){ const supplierCode = $('shipmentImportSupplierCode').value.trim(); const bizDate = $('shipmentBizDate').value || todayText(-1); const params = new URLSearchParams(); if(supplierCode) params.set('supplier_code', supplierCode); if(bizDate) params.set('biz_date', bizDate); openAdminPath('/admin/shipments/export-shipped?'+params.toString()); }
  async function uploadShipmentImport(){ try{ const file = $('shipmentImportFile').files[0]; if(!file){ setStatus('shippingStatus','请先选择导入文件。'); return; } const fd = new FormData(); fd.append('file', file); fd.append('supplier_code', $('shipmentImportSupplierCode').value.trim()); const res = await fetch(getApiBase() + '/admin/shipments/import', {method:'POST', headers:buildAdminHeaders({}), body:fd}); const data = await parseRes(res, '/admin/shipments/import'); setStatus('shippingStatus', '导入成功，批次号：'+data.batch_no); await loadShippingCenter(); }catch(e){ setStatus('shippingStatus','导入失败：'+e.message); } }
  async function loadShippingCenter(){ try{ const supplierCode = $('shipmentImportSupplierCode').value.trim(); const bizDate = $('shipmentBizDate').value || todayText(-1); const pending = await apiGet('/admin/shipments/pending-summary' + (supplierCode ? ('?supplier_code='+encodeURIComponent(supplierCode)) : '')); const shippedParams = new URLSearchParams(); if(supplierCode) shippedParams.set('supplier_code', supplierCode); if(bizDate) shippedParams.set('biz_date', bizDate); const shipped = await apiGet('/admin/shipments/shipped-summary' + (shippedParams.toString() ? ('?'+shippedParams.toString()) : '')); SHIPMENT_IMPORT_BATCHES = await apiGet('/admin/shipments/import-batches'); SHIPPING_PENDING = pending.rows || []; SHIPPING_SHIPPED = shipped.rows || []; $('pendingShipCount').textContent = pending.pending_shipment_count || 0; $('shippedCount').textContent = shipped.shipped_count || 0; renderShipmentImportBatches(); renderShipmentPreview('pendingShippingPreview', SHIPPING_PENDING, '暂无待发货预览'); renderShipmentPreview('shippedPreview', SHIPPING_SHIPPED, '暂无已发货预览'); setStatus('shippingStatus', '发货中心已刷新，业务日期：' + (bizDate || todayText(-1)) + '。'); }catch(e){ setStatus('shippingStatus','加载发货中心失败：'+e.message); } }
  async function downloadCurrentSupplierTemplateSample(){ const supplierCode = $('shipmentImportSupplierCode').value.trim(); if(!supplierCode){ setStatus('shippingStatus','请先填写供应链编码，例如 A 或 B。'); return; } const supplier = SUPPLIERS.find(s => (s.supplier_code||'').trim() === supplierCode); if(!supplier){ setStatus('shippingStatus','未找到对应供应链，请先到供应链中心保存供应链。'); return; } openAdminPath(`/admin/suppliers/${supplier.id}/template-sample`); }
  async function downloadSupplierTemplateSample(id){ openAdminPath(`/admin/suppliers/${id}/template-sample`); }


  // Suppliers center
// Suppliers center
  function fillSupplierSelects(){ $('psmSupplierId').innerHTML = SUPPLIERS.map(s=>`<option value="${s.id}">${escapeHtml(s.supplier_code)} · ${escapeHtml(s.supplier_name)}</option>`).join(''); $('psmProductId').innerHTML = PRODUCTS.map(p=>`<option value="${p.id}">${escapeHtml(p.name)}</option>`).join(''); const supplierOptions = '<option value="all">全部供应链</option>' + SUPPLIERS.map(s=>`<option value="${s.supplier_code}">${escapeHtml(s.supplier_code)} · ${escapeHtml(s.supplier_name)}</option>`).join(''); $('logisticsSearchSupplier').innerHTML = supplierOptions; if($('orderSearchSupplier')) $('orderSearchSupplier').innerHTML = supplierOptions; }
  function clearSupplierForm(){ $('supplierId').value=''; $('supplierCode').value=''; $('supplierName').value=''; $('supplierType').value='manual'; $('supplierApiBase').value=''; $('supplierApiKey').value=''; $('supplierApiKey').placeholder='留空则保持原 API Key'; $('supplierApiSecret').value=''; $('supplierApiSecret').placeholder='留空则保持原 API Secret'; $('supplierShippingBotCode').value=''; $('supplierTemplateType').value='standard'; $('supplierContactName').value=''; $('supplierContactPhone').value=''; $('supplierContactTg').value=''; $('supplierRemark').value=''; $('supplierIsActive').value='true'; }
  function editSupplier(raw){ const row = decodeRowPayload(raw); $('supplierId').value=row.id||''; $('supplierCode').value=row.supplier_code||''; $('supplierName').value=row.supplier_name||''; $('supplierType').value=row.supplier_type||'manual'; $('supplierApiBase').value=row.api_base||''; $('supplierApiKey').value=''; $('supplierApiKey').placeholder=row.api_key_masked||row.api_key||'留空则保持原 API Key'; $('supplierApiSecret').value=''; $('supplierApiSecret').placeholder=row.api_secret_masked||row.api_secret||'留空则保持原 API Secret'; $('supplierShippingBotCode').value=row.shipping_bot_code||''; $('supplierTemplateType').value=row.template_type||'standard'; $('supplierContactName').value=row.contact_name||''; $('supplierContactPhone').value=row.contact_phone||''; $('supplierContactTg').value=row.contact_tg||''; $('supplierRemark').value=row.remark||''; $('supplierIsActive').value=String(!!row.is_active); setStatus('suppliersStatus','已回填供应链；密钥留空则保持原值。'); }
  async function saveSupplier(){ try{ const payload={id:$('supplierId').value||null,supplier_code:$('supplierCode').value.trim(),supplier_name:$('supplierName').value.trim(),supplier_type:$('supplierType').value,api_base:$('supplierApiBase').value.trim(),api_key:$('supplierApiKey').value.trim(),api_secret:$('supplierApiSecret').value.trim(),shipping_bot_code:$('supplierShippingBotCode').value.trim(),template_type:$('supplierTemplateType').value.trim(),contact_name:$('supplierContactName').value.trim(),contact_phone:$('supplierContactPhone').value.trim(),contact_tg:$('supplierContactTg').value.trim(),remark:$('supplierRemark').value.trim(),is_active:$('supplierIsActive').value==='true'}; if(!payload.supplier_code || !payload.supplier_name){ setStatus('suppliersStatus','请先填写供应链编码和名称。'); return; } await apiPost('/admin/suppliers', payload); setStatus('suppliersStatus','供应链已保存。'); clearSupplierForm(); await loadSuppliersCenter(); }catch(e){ setStatus('suppliersStatus','保存供应链失败：'+e.message); } }
  async function toggleSupplier(id){ try{ await apiPost('/admin/suppliers/'+id+'/toggle', {}); await loadSuppliersCenter(); }catch(e){ setStatus('suppliersStatus','切换供应链状态失败：'+e.message); } }
  function renderSuppliersTable(){ const el=$('suppliersTable'); if(!SUPPLIERS.length){ el.innerHTML='<div class="muted">暂无供应链</div>'; return; } let html='<table><thead><tr><th>编码</th><th>名称</th><th>类型</th><th>供应链机器人</th><th>模板</th><th>API</th><th>状态</th><th>操作</th></tr></thead><tbody>'; for(const row of SUPPLIERS){ const apiTag = row.supplier_type==='api' ? (row.api_ready ? '<span class="tag ok">已配置</span>' : '<span class="tag warn">待配置</span>') : '<span class="tag off">人工</span>'; html += `<tr><td class="mono">${escapeHtml(row.supplier_code)}</td><td><div>${escapeHtml(row.supplier_name)}</div><div class="muted">${escapeHtml(row.contact_name||'')} ${escapeHtml(row.contact_phone||'')}</div></td><td>${escapeHtml(row.supplier_type)}</td><td class="mono">${escapeHtml(row.shipping_bot_code||'-')}</td><td>${escapeHtml(row.template_type||'-')}</td><td>${apiTag}</td><td>${boolTag(row.is_active)}</td><td><div class="actions"><button class="small secondary" onclick="editSupplier('${encodeRowPayload(row)}')">编辑</button><button class="small secondary" onclick="downloadSupplierTemplateSample(${row.id})">样例</button><button class="small orange" onclick="toggleSupplier(${row.id})">${row.is_active?'停用':'启用'}</button></div></td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
  function renderProductSupplierMapTable(){ const el=$('productSupplierMapTable'); if(!PRODUCT_SUPPLIER_MAPS.length){ el.innerHTML='<div class="muted">暂无商品绑定供应链</div>'; return; } let html='<table><thead><tr><th>商品</th><th>供应链</th><th>供应链 SKU</th><th>优先级</th><th>默认</th><th>状态</th></tr></thead><tbody>'; for(const row of PRODUCT_SUPPLIER_MAPS){ html += `<tr><td>${escapeHtml(row.product_name)}</td><td>${escapeHtml(row.supplier_code)} · ${escapeHtml(row.supplier_name)}</td><td class="mono">${escapeHtml(row.supplier_sku||'-')}</td><td>${escapeHtml(row.priority)}</td><td>${row.is_default?'<span class="tag ok">默认</span>':'-'}</td><td>${boolTag(row.is_active)}</td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
  function renderOrderFulfillmentsTable(){ const el=$('orderFulfillmentsTable'); if(!ORDER_FULFILLMENTS.length){ el.innerHTML='<div class="muted">暂无订单履约分配</div>'; return; } let html='<table><thead><tr><th>订单号</th><th>供应链</th><th>状态</th><th>供应链单号</th><th>分配时间</th><th>同步状态</th></tr></thead><tbody>'; for(const row of ORDER_FULFILLMENTS){ html += `<tr><td class="mono">${escapeHtml(row.order_no)}</td><td>${escapeHtml(row.supplier_code)} · ${escapeHtml(row.supplier_name)}</td><td>${escapeHtml(row.fulfillment_status)}</td><td class="mono">${escapeHtml(row.supplier_order_no||'-')}</td><td>${escapeHtml(formatTime(row.assigned_at))}</td><td>${escapeHtml(row.sync_status||'-')}</td></tr>`; } html += '</tbody></table>'; el.innerHTML=html; }
  async function saveProductSupplierMap(){ try{ const payload={product_id:Number($('psmProductId').value||0), supplier_id:Number($('psmSupplierId').value||0), supplier_sku:$('psmSupplierSku').value.trim(), priority:Number($('psmPriority').value||100), is_default:$('psmIsDefault').value==='true', is_active:true}; if(!payload.product_id || !payload.supplier_id){ setStatus('productSupplierMapStatus','请选择商品和供应链。'); return; } await apiPost('/admin/product-supplier-map', payload); setStatus('productSupplierMapStatus','商品绑定供应链已保存。'); $('psmSupplierSku').value=''; $('psmPriority').value=100; await loadSuppliersCenter(); }catch(e){ setStatus('productSupplierMapStatus','保存绑定失败：'+e.message); } }
  async function loadSuppliersCenter(){ try{ SUPPLIERS = await apiGet('/admin/suppliers'); PRODUCT_SUPPLIER_MAPS = await apiGet('/admin/product-supplier-map'); ORDER_FULFILLMENTS = await apiGet('/admin/order-fulfillments'); renderSuppliersTable(); renderProductSupplierMapTable(); renderOrderFulfillmentsTable(); fillSupplierSelects(); setStatus('suppliersStatus','供应链中心已刷新，可下载各供应链模板样例并进行推单骨架测试。'); }catch(e){ setStatus('suppliersStatus','加载供应链中心失败：'+e.message); } }

  // Data center
  function dcNum(v){ return new Intl.NumberFormat('zh-CN',{maximumFractionDigits:0}).format(Number(v||0)); }
  function dcMoney(v){ return '¥' + new Intl.NumberFormat('zh-CN',{maximumFractionDigits:2, minimumFractionDigits:0}).format(Number(v||0)); }
  function dcPct(v){ return Number(v||0).toFixed(1).replace('.0','') + '%'; }
  function dcHours(v){ return Number(v||0).toFixed(1).replace('.0','') + 'h'; }
  function dcDeltaText(metric, formatter){ if(!metric) return '-'; const delta=Number(metric.delta||0); const pct = metric.delta_pct; const prefix = delta>0 ? '↑' : (delta<0 ? '↓' : '→'); const deltaText = formatter ? formatter(Math.abs(delta)) : Math.abs(delta).toFixed(1); const pctText = pct===null || pct===undefined ? '基线不足' : (Math.abs(Number(pct)).toFixed(1).replace('.0','') + '%'); return `${prefix} 较上期 ${deltaText} / ${pctText}`; }
  function fillDataCenterSupplierSelect(){ const el=$('dcSupplierCode'); if(!el) return; const current = el.value || ''; let html='<option value="">全部供应链</option>'; for(const s of SUPPLIERS){ html += `<option value="${escapeHtml(s.supplier_code)}">${escapeHtml(s.supplier_code)} · ${escapeHtml(s.supplier_name)}</option>`; } el.innerHTML = html; el.value = current; }
  function dataCenterQuery(extra={}){ const params = new URLSearchParams(); const days = $('dcDays') ? $('dcDays').value : '30'; const supplier = $('dcSupplierCode') ? $('dcSupplierCode').value.trim() : ''; if(days) params.set('days', days); if(supplier) params.set('supplier_code', supplier); Object.entries(extra||{}).forEach(([k,v])=>{ if(v!==undefined && v!==null && String(v)!=='') params.set(k, String(v)); }); return params.toString() ? ('?' + params.toString()) : ''; }
  function renderDataCenterOverview(){ const data = DATA_CENTER_OVERVIEW; if(!data || !data.metrics){ return; } const m=data.metrics; $('dcPaidGmv').textContent = dcMoney(m.paid_gmv.current); $('dcPaidOrders').textContent = dcNum(m.paid_orders.current); $('dcShippedOrders').textContent = dcNum(m.shipped_orders.current); $('dcSignRate').textContent = dcPct(m.sign_rate.current); $('dcPendingShip').textContent = dcNum(m.pending_shipments.current); $('dcAvgShipHours').textContent = dcHours(m.avg_ship_hours.current); $('dcSyncAbnormal').textContent = dcNum(m.sync_abnormal_count.current); $('dcStagnantCount').textContent = dcNum(m.stagnant_count.current); $('dcPaidGmvTip').textContent = dcDeltaText(m.paid_gmv, dcMoney); $('dcPaidOrdersTip').textContent = dcDeltaText(m.paid_orders, x=>dcNum(x)); $('dcShippedOrdersTip').textContent = dcDeltaText(m.shipped_orders, x=>dcNum(x)); $('dcSignRateTip').textContent = dcDeltaText(m.sign_rate, x=>dcPct(x)); $('dcPendingShipTip').textContent = dcDeltaText(m.pending_shipments, x=>dcNum(x)); $('dcAvgShipHoursTip').textContent = dcDeltaText(m.avg_ship_hours, x=>dcHours(x)); $('dcSyncAbnormalTip').textContent = dcDeltaText(m.sync_abnormal_count, x=>dcNum(x)); $('dcStagnantCountTip').textContent = dcDeltaText(m.stagnant_count, x=>dcNum(x)); const cur=data.summary.current, prev=data.summary.previous; $('dcCompareSummary').innerHTML = `当前窗口：支付 <b>${dcNum(cur.paid_orders)}</b> 单，GMV <b>${dcMoney(cur.paid_gmv)}</b>，发货率 <b>${dcPct(cur.ship_rate)}</b>，签收率 <b>${dcPct(cur.sign_rate)}</b>；上期：支付 <b>${dcNum(prev.paid_orders)}</b> 单，GMV <b>${dcMoney(prev.paid_gmv)}</b>，发货率 <b>${dcPct(prev.ship_rate)}</b>，签收率 <b>${dcPct(prev.sign_rate)}</b>。`; }
  function renderDataCenterSupplierBoard(){ const el=$('dcSupplierBoard'); const rows=DATA_CENTER_SUPPLIER_BOARD||[]; if(!rows.length){ el.innerHTML='<div class="muted">当前窗口暂无供应链数据。</div>'; return; } let html='<table><thead><tr><th>供应链</th><th>映射商品</th><th>订单</th><th>已支付</th><th>GMV</th><th>待发货</th><th>发货率</th><th>签收率</th><th>平均发货时效</th><th>同步异常</th><th>停滞物流</th><th>最近订单</th></tr></thead><tbody>'; for(const r of rows){ html += `<tr><td>${escapeHtml(r.supplier_code)} · ${escapeHtml(r.supplier_name||'')}</td><td>${dcNum(r.mapped_products)}</td><td>${dcNum(r.orders)}</td><td>${dcNum(r.paid_orders)}</td><td>${dcMoney(r.paid_gmv)}</td><td>${dcNum(r.pending_shipments)}</td><td>${dcPct(r.ship_rate)}</td><td>${dcPct(r.sign_rate)}</td><td>${dcHours(r.avg_ship_hours)}</td><td>${dcNum(r.sync_abnormal_count)}</td><td>${dcNum(r.stagnant_count)}</td><td>${escapeHtml(formatTime(r.latest_order_at))}</td></tr>`; } html += '</tbody></table>'; el.innerHTML = html; }
  function renderDataCenterTrend(){ const el=$('dcTrend'); const rows=(DATA_CENTER_TREND||[]).slice().reverse(); if(!rows.length){ el.innerHTML='<div class="muted">暂无趋势数据。</div>'; return; } let html='<table><thead><tr><th>日期</th><th>新建订单</th><th>已支付</th><th>支付GMV</th><th>已发货</th><th>已签收</th></tr></thead><tbody>'; for(const r of rows){ html += `<tr><td>${escapeHtml(r.date)}</td><td>${dcNum(r.created_orders)}</td><td>${dcNum(r.paid_orders)}</td><td>${dcMoney(r.paid_gmv)}</td><td>${dcNum(r.shipped_orders)}</td><td>${dcNum(r.signed_orders)}</td></tr>`; } html += '</tbody></table>'; el.innerHTML = html; }
  function renderDataCenterCategoryBoard(){ const el=$('dcCategorySupplierBoard'); const rows=DATA_CENTER_CATEGORY_BOARD||[]; if(!rows.length){ el.innerHTML='<div class="muted">暂无分类与供应链联动数据。</div>'; return; } let html='<table><thead><tr><th>分类</th><th>供应链</th><th>订单数</th><th>销量</th><th>GMV</th><th>商品数</th></tr></thead><tbody>'; for(const r of rows){ html += `<tr><td>${escapeHtml(r.category_name)}</td><td>${escapeHtml(r.supplier_code)}</td><td>${dcNum(r.order_count)}</td><td>${dcNum(r.qty)}</td><td>${dcMoney(r.gmv)}</td><td>${dcNum(r.product_count)}</td></tr>`; } html += '</tbody></table>'; el.innerHTML = html; }
  function renderDataCenterProductRanking(){ const el=$('dcProductRanking'); const rows=DATA_CENTER_PRODUCT_RANKING||[]; if(!rows.length){ el.innerHTML='<div class="muted">暂无商品销量榜。</div>'; return; } let html='<table><thead><tr><th>商品</th><th>SKU</th><th>分类</th><th>供应链</th><th>订单数</th><th>销量</th><th>GMV</th><th>已支付</th><th>已签收</th></tr></thead><tbody>'; for(const r of rows){ html += `<tr><td>${escapeHtml(r.product_name)}</td><td class="mono">${escapeHtml(r.sku_code||'-')}</td><td>${escapeHtml(r.category_name||'-')}</td><td>${escapeHtml(r.supplier_code||'-')}</td><td>${dcNum(r.order_count)}</td><td>${dcNum(r.qty)}</td><td>${dcMoney(r.gmv)}</td><td>${dcNum(r.paid_orders)}</td><td>${dcNum(r.signed_orders)}</td></tr>`; } html += '</tbody></table>'; el.innerHTML = html; }
  function renderDataCenterFunnel(){ const el=$('dcFunnel'); const rows=DATA_CENTER_FUNNEL||[]; if(!rows.length){ el.innerHTML='<div class="muted">暂无订单漏斗。</div>'; return; } let html='<table><thead><tr><th>阶段</th><th>数量</th><th>转化率</th></tr></thead><tbody>'; for(const r of rows){ html += `<tr><td>${escapeHtml(r.stage)}</td><td>${dcNum(r.count)}</td><td>${dcPct(r.rate_vs_prev)}</td></tr>`; } html += '</tbody></table>'; el.innerHTML = html; }
  function renderDataCenterAlertsTrend(){ const el=$('dcAlertsTrend'); const rows=(DATA_CENTER_ALERTS_TREND||[]).slice().reverse(); if(!rows.length){ el.innerHTML='<div class="muted">暂无异常趋势。</div>'; return; } let html='<table><thead><tr><th>日期</th><th>已跟踪物流</th><th>同步异常</th><th>停滞物流</th><th>异常件</th></tr></thead><tbody>'; for(const r of rows){ html += `<tr><td>${escapeHtml(r.date)}</td><td>${dcNum(r.tracked_shipments)}</td><td>${dcNum(r.sync_abnormal)}</td><td>${dcNum(r.stagnant)}</td><td>${dcNum(r.exceptional)}</td></tr>`; } html += '</tbody></table>'; el.innerHTML = html; }
  async function loadDataCenter(){ try{ if(!SUPPLIERS.length){ SUPPLIERS = await apiGet('/admin/suppliers'); } fillDataCenterSupplierSelect(); const query = dataCenterQuery(); const [overview, board, trend, categoryBoard, ranking, funnel, alertsTrend] = await Promise.all([ apiGet('/admin/data-center/overview'+query), apiGet('/admin/data-center/supplier-board'+query), apiGet('/admin/data-center/trend'+query), apiGet('/admin/data-center/category-supplier-board'+query), apiGet('/admin/data-center/product-ranking'+dataCenterQuery({limit:20})), apiGet('/admin/data-center/funnel'+query), apiGet('/admin/data-center/alerts-trend'+query) ]); DATA_CENTER_OVERVIEW = overview; DATA_CENTER_SUPPLIER_BOARD = board.rows || []; DATA_CENTER_TREND = trend.rows || []; DATA_CENTER_CATEGORY_BOARD = categoryBoard.rows || []; DATA_CENTER_PRODUCT_RANKING = ranking.rows || []; DATA_CENTER_FUNNEL = funnel.rows || []; DATA_CENTER_ALERTS_TREND = alertsTrend.rows || []; renderDataCenterOverview(); renderDataCenterSupplierBoard(); renderDataCenterTrend(); renderDataCenterCategoryBoard(); renderDataCenterProductRanking(); renderDataCenterFunnel(); renderDataCenterAlertsTrend(); setStatus('dataCenterStatus', `数据中心已刷新：${$('dcDays').value} 天窗口${$('dcSupplierCode').value ? '，供应链 ' + $('dcSupplierCode').value : '，全部供应链'}。`); }catch(e){ setStatus('dataCenterStatus','加载数据中心失败：'+e.message); } }
  function exportDataCenterSupplierBoard(){ openAdminPath('/admin/data-center/supplier-board.xlsx' + dataCenterQuery()); }
  function exportDataCenterProductRanking(){ const extra = dataCenterQuery({limit:50}); openAdminPath('/admin/data-center/product-ranking.xlsx' + extra); }

  async function loadOverviewBootstrap(){
    const tasks = [
      ['bots', ()=>loadBots()],
      ['sessions', ()=>loadChatCenter()],
      ['categories', ()=>loadCategories()],
      ['products', ()=>loadProducts()],
      ['orders', ()=>refreshOrders()],
      ['payment', ()=>loadPaymentAddresses()],
      ['suppliers', ()=>loadSuppliersCenter()],
      ['shipping', ()=>loadShippingCenter()],
      ['logistics', ()=>loadLogisticsCenter()],
      ['folder-link', ()=>loadFolderLinkConfig()],
    ];
    const results = await Promise.allSettled(tasks.map(([, fn]) => fn()));
    const failed = results
      .map((result, idx) => ({result, name: tasks[idx][0]}))
      .filter(item => item.result.status === 'rejected');
    if(failed.length){
      console.warn('[admin-ui] overview bootstrap partial failure:', failed.map(x => x.name).join(', '), failed.map(x => x.result.reason && x.result.reason.message ? x.result.reason.message : String(x.result.reason || '未知错误')).join(' | '));
    }
    return failed;
  }
  async function bootAdminUi(){
    try{
      await ensureAdminAuth(false);
      const requestedTab = readRequestedTab();
      // 先把当前 tab 立即显示出来，避免 F5 时因为任一接口失败导致整页空白。
      showTab(requestedTab, findTabButton(requestedTab), {persist:false});
      // 其余概览数据后台补齐，失败也不再阻塞当前页展示。
      loadOverviewBootstrap().catch(err => console.error('[admin-ui] overview bootstrap failed:', err));
    }catch(err){
      console.error(err);
    }
  }
  window.addEventListener('hashchange', function(){
    const requestedTab = readRequestedTab();
    showTab(requestedTab, findTabButton(requestedTab), {persist:false});
  });
  window.addEventListener('popstate', function(){
    const requestedTab = readRequestedTab();
    showTab(requestedTab, findTabButton(requestedTab), {persist:false});
  });
  if($('shipmentBizDate') && !$('shipmentBizDate').value){ $('shipmentBizDate').value = todayText(-1); }
  bootAdminUi();
</script>
</body>
</html>
""").replace("__ADMIN_USERNAME__", html.escape(str(admin_username or "admin")))


def render_admin_login_page(default_username: str = "admin") -> str:
    return (r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Goodsmall 后台登录</title>
  <style>
    :root{--bg:#eef4ff;--card:#ffffff;--line:#dbe4f0;--text:#0f172a;--muted:#64748b;--primary:#0f172a;--accent:#2563eb;--green:#16a34a;--danger:#ef4444;--shadow:0 18px 40px rgba(15,23,42,.10);--radius:24px}
    *{box-sizing:border-box}html,body{margin:0;padding:0}body{min-height:100vh;font-family:Inter,"PingFang SC","Microsoft YaHei",Arial,sans-serif;background:linear-gradient(135deg,#eef4ff 0,#f8fbff 42%,#f3f6fb 100%);color:var(--text);display:flex;align-items:center;justify-content:center;padding:24px}
    .shell{width:100%;max-width:960px;display:grid;grid-template-columns:1.1fr .9fr;gap:24px;align-items:stretch}
    .hero,.card{background:var(--card);border:1px solid rgba(15,23,42,.06);border-radius:var(--radius);box-shadow:var(--shadow)}
    .hero{padding:34px;background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 100%);color:#fff;position:relative;overflow:hidden}
    .hero:before,.hero:after{content:"";position:absolute;border-radius:999px;background:rgba(255,255,255,.08)}
    .hero:before{width:220px;height:220px;right:-70px;top:-90px}.hero:after{width:160px;height:160px;left:-60px;bottom:-70px}
    .hero h1{margin:0 0 14px;font-size:34px;line-height:1.2}.hero p{margin:0;color:rgba(255,255,255,.82);line-height:1.8;font-size:14px}
    .bullet{margin-top:18px;display:grid;gap:10px}.bullet div{padding:12px 14px;border-radius:14px;background:rgba(255,255,255,.12);backdrop-filter:blur(8px);font-size:13px}
    .card{padding:30px}.card h2{margin:0 0 8px;font-size:28px}.sub{color:var(--muted);font-size:14px;line-height:1.7;margin-bottom:22px}
    label{display:block;font-size:13px;color:#475569;margin:0 0 8px;font-weight:700}
    input{width:100%;padding:14px 15px;border:1px solid var(--line);border-radius:16px;font-size:15px;outline:none;transition:border-color .15s ease, box-shadow .15s ease}
    input:focus{border-color:#93c5fd;box-shadow:0 0 0 4px rgba(37,99,235,.10)}
    .space-top{margin-top:14px}.actions{display:flex;gap:10px;align-items:center;margin-top:22px}.actions button{flex:1;border:none;border-radius:16px;padding:14px 16px;font-size:15px;font-weight:800;cursor:pointer}.actions .primary{background:var(--primary);color:#fff}.actions .secondary{background:#e2e8f0;color:#0f172a}
    .status{margin-top:18px;padding:13px 14px;border-radius:16px;background:#f8fafc;border:1px solid var(--line);font-size:13px;color:#334155;min-height:48px}.status.error{background:#fef2f2;border-color:#fecaca;color:#b91c1c}.status.ok{background:#effdf5;border-color:#bbf7d0;color:#15803d}
    .foot{margin-top:14px;color:var(--muted);font-size:12px;line-height:1.7}.badge{display:inline-flex;align-items:center;gap:8px;padding:9px 12px;border-radius:999px;background:#eff6ff;color:#1d4ed8;font-size:12px;font-weight:800;margin-bottom:14px}
    @media (max-width: 860px){.shell{grid-template-columns:1fr}.hero{order:2}.card{order:1}}
  </style>
</head>
<body>
  <div class="shell">
    <div class="hero">
      <div class="badge">后台账号密码登录页 v2</div>
      <h1>Goodsmall 实货商城后台</h1>
      <p>这一版把原来的“频繁弹口令”替换为正式登录页。登录成功后写入后台会话，不再反复弹窗，体验会顺很多。</p>
      <div class="bullet">
        <div>默认管理员账号：<b>__DEFAULT_USERNAME__</b></div>
        <div>首次启动可用 <b>ADMIN_USERNAME / ADMIN_PASSWORD</b> 自动生成超级管理员</div>
        <div>登录后使用会话 Cookie 保持登录，后续可在后台继续新增更多管理员</div>
      </div>
    </div>
    <div class="card">
      <h2>后台登录</h2>
      <div class="sub">请输入后台管理员账号和密码。登录成功后将自动跳转到管理后台。</div>
      <label>管理员账号</label>
      <input id="username" autocomplete="username" value="__DEFAULT_USERNAME__" placeholder="请输入管理员账号" />
      <label class="space-top">管理员密码</label>
      <input id="password" type="password" autocomplete="current-password" placeholder="请输入后台密码" />
      <div class="actions">
        <button class="primary" onclick="submitLogin()">进入后台</button>
        <button class="secondary" onclick="clearLoginForm()">清空</button>
      </div>
      <div id="loginStatus" class="status">请使用后台管理员账号登录。</div>
      <div class="foot">如果你是首次升级到多账号版，请先在 <b>.env</b> 中保留 <b>ADMIN_USERNAME</b> 和 <b>ADMIN_PASSWORD</b> 作为初始化超级管理员，然后执行 <b>docker compose up -d --build backend</b>。后续新增账号将保存在数据库里。</div>
    </div>
  </div>
<script>
  function $(id){ return document.getElementById(id); }
  function status(text, cls=''){ const el=$('loginStatus'); el.className='status'+(cls?(' '+cls):''); el.textContent=text; }
  function clearLoginForm(){ $('password').value=''; status('已清空，请重新输入账号密码。'); }
  async function submitLogin(){
    const username = String($('username').value || '').trim();
    const password = String($('password').value || '').trim();
    if(!username){ status('请先填写管理员账号。','error'); $('username').focus(); return; }
    if(!password){ status('请先填写管理员密码。','error'); $('password').focus(); return; }
    status('正在验证账号密码，请稍候...');
    try{
      const res = await fetch('/admin/login', {method:'POST', headers:{'Content-Type':'application/json','Accept':'application/json'}, body: JSON.stringify({username, password})});
      const data = await res.json().catch(()=>({}));
      if(!res.ok){ throw new Error(data.detail || '登录失败'); }
      status('登录成功，正在进入后台...','ok');
      const nextUrl = new URLSearchParams(window.location.search).get('next');
      window.location.href = (nextUrl && nextUrl.startsWith('/admin')) ? nextUrl : '/admin/ui';
    }catch(e){
      status('登录失败：' + (e && e.message ? e.message : '账号或密码错误'), 'error');
    }
  }
  $('password').addEventListener('keydown', function(e){ if(e.key==='Enter'){ submitLogin(); } });
  $('username').addEventListener('keydown', function(e){ if(e.key==='Enter'){ submitLogin(); } });
</script>
</body>
</html>
""").replace("__DEFAULT_USERNAME__", html.escape(str(default_username or "admin")))
