using System;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OutboxCore.Abstractions;
using OutboxCore.Background;
using OutboxCore.Configuration;

namespace OutboxCore.Dashboard;

public class OutboxDashboardMiddleware
{
    private readonly RequestDelegate _next;
    private readonly string _pathPrefix;

    public OutboxDashboardMiddleware(RequestDelegate _next, string pathPrefix = "/outbox-dashboard")
    {
        this._next = _next;
        _pathPrefix = pathPrefix.TrimEnd('/');
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var requestPath = context.Request.Path.Value?.TrimEnd('/');
        if (requestPath == null)
        {
            await _next(context);
            return;
        }

        // 1. HTML Route
        if (requestPath.Equals(_pathPrefix, StringComparison.OrdinalIgnoreCase) ||
            requestPath.Equals(_pathPrefix + "/", StringComparison.OrdinalIgnoreCase))
        {
            context.Response.ContentType = MediaTypeNames.Text.Html;
            await context.Response.WriteAsync(GetHtmlPage());
            return;
        }

        // 2. API Routes
        if (requestPath.StartsWith(_pathPrefix + "/api/", StringComparison.OrdinalIgnoreCase))
        {
            var apiPath = requestPath.Substring((_pathPrefix + "/api/").Length).ToLower();
            context.Response.ContentType = MediaTypeNames.Application.Json;

            try
            {
                var options = context.RequestServices.GetRequiredService<IOptions<OutboxOptions>>().Value;
                var modules = options.Modules.Select(m => m.ModuleName).ToList();
                if (!modules.Contains("Default"))
                {
                    modules.Insert(0, "Default");
                }

                var module = context.Request.Query["module"].FirstOrDefault() ?? "Default";

                // Resolve repos for module (either keyed or fallback)
                var outboxRepo = context.RequestServices.GetKeyedServices<IOutboxRepository>(module).FirstOrDefault() 
                                 ?? context.RequestServices.GetService<IOutboxRepository>();
                var inboxProcessor = context.RequestServices.GetKeyedServices<IInboxProcessor>(module).FirstOrDefault() 
                                     ?? context.RequestServices.GetService<IInboxProcessor>();

                if (apiPath == "stats")
                {
                    var pendingCount = outboxRepo != null ? await outboxRepo.GetCountByStatusAsync(module, "Pending", context.RequestAborted) : 0;
                    var processingCount = outboxRepo != null ? await outboxRepo.GetCountByStatusAsync(module, "Processing", context.RequestAborted) : 0;
                    var successCount = outboxRepo != null ? await outboxRepo.GetCountByStatusAsync(module, "Processed", context.RequestAborted) : 0;
                    var failedCount = outboxRepo != null ? await outboxRepo.GetCountByStatusAsync(module, "Failed", context.RequestAborted) : 0;

                    var inboxPending = inboxProcessor != null ? await inboxProcessor.GetCountByStatusAsync(module, "Pending", context.RequestAborted) : 0;
                    var inboxSuccess = inboxProcessor != null ? await inboxProcessor.GetCountByStatusAsync(module, "Processed", context.RequestAborted) : 0;
                    var inboxFailed = inboxProcessor != null ? await inboxProcessor.GetCountByStatusAsync(module, "Failed", context.RequestAborted) : 0;

                    var response = new
                    {
                        modules,
                        selectedModule = module,
                        outbox = new { pending = pendingCount + processingCount, success = successCount, failed = failedCount },
                        inbox = new { pending = inboxPending, success = inboxSuccess, failed = inboxFailed }
                    };

                    await context.Response.WriteAsync(JsonSerializer.Serialize(response));
                    return;
                }

                if (apiPath == "messages")
                {
                    var type = context.Request.Query["type"].FirstOrDefault() ?? "outbox";
                    var status = context.Request.Query["status"].FirstOrDefault();
                    if (status == "all") status = null;

                    if (type == "outbox" && outboxRepo != null)
                    {
                        var messages = await outboxRepo.GetMessagesAsync(module, status, 50, context.RequestAborted);
                        await context.Response.WriteAsync(JsonSerializer.Serialize(messages));
                        return;
                    }
                    else if (type == "inbox" && inboxProcessor != null)
                    {
                        var messages = await inboxProcessor.GetMessagesAsync(module, status, 50, context.RequestAborted);
                        await context.Response.WriteAsync(JsonSerializer.Serialize(messages));
                        return;
                    }

                    await context.Response.WriteAsync("[]");
                    return;
                }

                if (apiPath == "retry" && HttpMethods.IsPost(context.Request.Method))
                {
                    if (outboxRepo == null)
                    {
                        context.Response.StatusCode = 400;
                        await context.Response.WriteAsync(JsonSerializer.Serialize(new { error = "Repository not registered" }));
                        return;
                    }

                    var idStr = context.Request.Query["id"].FirstOrDefault();
                    if (Guid.TryParse(idStr, out var messageId))
                    {
                        await outboxRepo.UpdateMessageStatusAsync(module, messageId, "Pending", null, null, 0, context.RequestAborted);
                        
                        // Notify outbox publisher channel
                        var channel = context.RequestServices.GetRequiredService<IOutboxChannel>();
                        await channel.WriteAsync(module, context.RequestAborted);

                        await context.Response.WriteAsync(JsonSerializer.Serialize(new { success = true }));
                        return;
                    }

                    context.Response.StatusCode = 400;
                    await context.Response.WriteAsync(JsonSerializer.Serialize(new { error = "Invalid message ID" }));
                    return;
                }
            }
            catch (Exception ex)
            {
                context.Response.StatusCode = 500;
                await context.Response.WriteAsync(JsonSerializer.Serialize(new { error = ex.Message }));
                return;
            }
        }

        await _next(context);
    }

    private string GetHtmlPage()
    {
        return @"<!DOCTYPE html>
<html lang='en' class='dark'>
<head>
    <meta charset='UTF-8'>
    <meta name='viewport' content='width=device-width, initial-scale=1.0'>
    <title>OutboxCore Dashboard</title>
    <script src='https://cdn.tailwindcss.com'></script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        glass: 'rgba(255, 255, 255, 0.05)',
                        borderGlass: 'rgba(255, 255, 255, 0.1)',
                    }
                }
            }
        }
    </script>
    <style>
        body {
            background: radial-gradient(circle at top, #1e1b4b, #0f0a19, #05020a);
            min-height: 100vh;
        }
        .glass-card {
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.08);
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }
    </style>
</head>
<body class='text-slate-100 font-sans p-6'>
    <div class='max-w-7xl mx-auto'>
        <!-- Header -->
        <header class='flex justify-between items-center mb-8 pb-4 border-b border-white/10'>
            <div>
                <h1 class='text-3xl font-extrabold tracking-wider bg-clip-text text-transparent bg-gradient-to-r from-violet-400 to-fuchsia-400'>
                    OUTBOX<span class='text-slate-100'>CORE</span>
                </h1>
                <p class='text-xs text-slate-400 mt-1'>Modular Monolith Outbox & Inbox Management</p>
            </div>
            <div class='flex items-center space-x-3'>
                <label for='moduleSelector' class='text-sm text-slate-400 font-semibold'>Module:</label>
                <select id='moduleSelector' onchange='changeModule()' class='bg-slate-900 border border-white/20 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:border-violet-500 transition'>
                    <option value='Default'>Default</option>
                </select>
                <button onclick='refreshData()' class='bg-violet-600 hover:bg-violet-500 text-white font-semibold text-sm px-4 py-1.5 rounded-lg transition shadow-lg shadow-violet-500/20 active:scale-95'>
                    Refresh
                </button>
            </div>
        </header>

        <!-- Stats Grid -->
        <div class='grid grid-cols-1 md:grid-cols-3 gap-6 mb-8'>
            <!-- Pending Card -->
            <div class='glass-card rounded-2xl p-6 relative overflow-hidden'>
                <div class='absolute -right-4 -bottom-4 text-violet-500/10 text-9xl font-bold select-none'>P</div>
                <h3 class='text-sm font-semibold text-slate-400 uppercase tracking-wider'>Pending / Processing</h3>
                <div class='flex items-baseline space-x-2 mt-4'>
                    <span id='pendingCount' class='text-4xl font-extrabold text-amber-400'>0</span>
                    <span class='text-slate-400 text-sm'>messages</span>
                </div>
            </div>
            <!-- Success Card -->
            <div class='glass-card rounded-2xl p-6 relative overflow-hidden'>
                <div class='absolute -right-4 -bottom-4 text-emerald-500/10 text-9xl font-bold select-none'>S</div>
                <h3 class='text-sm font-semibold text-slate-400 uppercase tracking-wider'>Processed Successfully</h3>
                <div class='flex items-baseline space-x-2 mt-4'>
                    <span id='successCount' class='text-4xl font-extrabold text-emerald-400'>0</span>
                    <span class='text-slate-400 text-sm'>messages</span>
                </div>
            </div>
            <!-- Failed Card -->
            <div class='glass-card rounded-2xl p-6 relative overflow-hidden'>
                <div class='absolute -right-4 -bottom-4 text-rose-500/10 text-9xl font-bold select-none'>F</div>
                <h3 class='text-sm font-semibold text-slate-400 uppercase tracking-wider'>Failed</h3>
                <div class='flex items-baseline space-x-2 mt-4'>
                    <span id='failedCount' class='text-4xl font-extrabold text-rose-400'>0</span>
                    <span class='text-slate-400 text-sm'>messages</span>
                </div>
            </div>
        </div>

        <!-- Main Workspace -->
        <div class='glass-card rounded-2xl overflow-hidden'>
            <!-- Table Tabs & Filter -->
            <div class='flex flex-col sm:flex-row justify-between items-stretch sm:items-center p-4 border-b border-white/10 gap-4 bg-white/5'>
                <div class='flex space-x-2 bg-slate-950/40 p-1 rounded-xl self-start'>
                    <button id='tabOutbox' onclick='switchTab(""outbox"")' class='px-4 py-2 rounded-lg text-sm font-semibold transition bg-violet-600 text-white shadow'>
                        Outbox Messages
                    </button>
                    <button id='tabInbox' onclick='switchTab(""inbox"")' class='px-4 py-2 rounded-lg text-sm font-semibold transition text-slate-400 hover:text-white hover:bg-white/5'>
                        Inbox Messages
                    </button>
                </div>
                <div class='flex items-center space-x-3'>
                    <label for='statusFilter' class='text-sm text-slate-400'>Status:</label>
                    <select id='statusFilter' onchange='loadMessages()' class='bg-slate-900 border border-white/10 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:border-violet-500 transition'>
                        <option value='all'>All</option>
                        <option value='Pending'>Pending</option>
                        <option value='Processed'>Processed</option>
                        <option value='Failed'>Failed</option>
                    </select>
                </div>
            </div>

            <!-- Table -->
            <div class='overflow-x-auto'>
                <table class='w-full border-collapse text-left text-sm'>
                    <thead class='bg-white/5 border-b border-white/10 text-slate-400 font-semibold'>
                        <tr>
                            <th class='p-4'>ID</th>
                            <th class='p-4'>Message Type</th>
                            <th class='p-4' id='colTimeHead'>Created At</th>
                            <th class='p-4'>Status</th>
                            <th class='p-4' id='colRetryHead'>Retry Count</th>
                            <th class='p-4'>Error / Info</th>
                            <th class='p-4 text-right'>Actions</th>
                        </tr>
                    </thead>
                    <tbody id='messageTableBody' class='divide-y divide-white/5'>
                        <!-- Loaded dynamically -->
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Notification Toast -->
    <div id='toast' class='fixed bottom-6 right-6 glass-card px-6 py-3 rounded-xl shadow-2xl transition-all duration-300 transform translate-y-24 opacity-0 flex items-center space-x-2 z-50'>
        <span id='toastMessage'></span>
    </div>

    <script>
        let currentTab = 'outbox';
        let currentModule = 'Default';

        function showToast(message, isError = false) {
            const toast = document.getElementById('toast');
            const toastMessage = document.getElementById('toastMessage');
            toastMessage.textContent = message;
            if (isError) {
                toast.classList.add('border-rose-500/50', 'text-rose-400');
            } else {
                toast.classList.remove('border-rose-500/50', 'text-rose-400');
            }
            toast.classList.remove('translate-y-24', 'opacity-0');
            setTimeout(() => {
                toast.classList.add('translate-y-24', 'opacity-0');
            }, 3000);
        }

        async function fetchJson(url, options = {}) {
            const res = await fetch(url, options);
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                throw new Error(err.error || 'Request failed');
            }
            return res.json();
        }

        async function refreshData() {
            try {
                // Fetch stats
                const stats = await fetchJson('./api/stats?module=' + currentModule);
                
                // Render module selector
                const selector = document.getElementById('moduleSelector');
                selector.innerHTML = '';
                stats.modules.forEach(m => {
                    const opt = document.createElement('option');
                    opt.value = m;
                    opt.textContent = m;
                    opt.selected = m === stats.selectedModule;
                    selector.appendChild(opt);
                });

                // Update cards based on current tab
                const counts = currentTab === 'outbox' ? stats.outbox : stats.inbox;
                document.getElementById('pendingCount').textContent = counts.pending;
                document.getElementById('successCount').textContent = counts.success;
                document.getElementById('failedCount').textContent = counts.failed;

                await loadMessages();
            } catch (e) {
                showToast(e.message, true);
            }
        }

        function changeModule() {
            currentModule = document.getElementById('moduleSelector').value;
            refreshData();
        }

        function switchTab(tab) {
            currentTab = tab;
            document.getElementById('tabOutbox').className = tab === 'outbox' 
                ? 'px-4 py-2 rounded-lg text-sm font-semibold transition bg-violet-600 text-white shadow'
                : 'px-4 py-2 rounded-lg text-sm font-semibold transition text-slate-400 hover:text-white hover:bg-white/5';
            document.getElementById('tabInbox').className = tab === 'inbox' 
                ? 'px-4 py-2 rounded-lg text-sm font-semibold transition bg-violet-600 text-white shadow'
                : 'px-4 py-2 rounded-lg text-sm font-semibold transition text-slate-400 hover:text-white hover:bg-white/5';
            
            document.getElementById('colTimeHead').textContent = tab === 'outbox' ? 'Created At' : 'Received At';
            document.getElementById('colRetryHead').style.display = tab === 'outbox' ? 'table-cell' : 'none';

            refreshData();
        }

        async function loadMessages() {
            try {
                const status = document.getElementById('statusFilter').value;
                const messages = await fetchJson('./api/messages?module=' + currentModule + '&type=' + currentTab + '&status=' + status);
                
                const tbody = document.getElementById('messageTableBody');
                tbody.innerHTML = '';

                if (messages.length === 0) {
                    tbody.innerHTML = '<tr><td colspan=""7"" class=""p-8 text-center text-slate-500 font-semibold"">No messages found</td></tr>';
                    return;
                }

                messages.forEach(msg => {
                    const tr = document.createElement('tr');
                    tr.className = 'hover:bg-white/5 transition';

                    const statusColor = msg.Status === 'Processed' || msg.Status === 'Success'
                        ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                        : msg.Status === 'Failed'
                            ? 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                            : 'bg-amber-500/10 text-amber-400 border border-amber-500/20';

                    const timeVal = msg.CreatedAt || msg.ReceivedAt || msg.receivedAt || msg.createdAt;
                    const dateStr = timeVal ? new Date(timeVal).toLocaleString() : 'N/A';

                    const retryCell = currentTab === 'outbox' ? '<td class=""p-4"">' + (msg.RetryCount ?? msg.retryCount ?? 0) + '</td>' : '';

                    const actions = currentTab === 'outbox' && (msg.Status === 'Failed' || msg.status === 'Failed')
                        ? '<button onclick=""retryMessage(\'' + msg.Id + '\')"" class=""bg-violet-600/20 hover:bg-violet-600 text-violet-300 hover:text-white font-semibold text-xs px-3 py-1 rounded-md border border-violet-500/30 transition"">Retry</button>'
                        : '';

                    tr.innerHTML = ' \
                        <td class=""p-4 font-mono text-xs text-slate-400"">' + msg.Id + '</td> \
                        <td class=""p-4 font-semibold text-slate-300 max-w-[200px] truncate"" title=""' + msg.MessageType + '"">' + msg.MessageType.split('.').pop() + '</td> \
                        <td class=""p-4 text-slate-400"">' + dateStr + '</td> \
                        <td class=""p-4""><span class=""px-2.5 py-0.5 rounded-full text-xs font-semibold ' + statusColor + '"">' + msg.Status + '</span></td> \
                        ' + retryCell + ' \
                        <td class=""p-4 max-w-[250px] truncate text-slate-400"" title=""' + (msg.Error || msg.Content || '') + '"">' + (msg.Error || msg.Content || '') + '</td> \
                        <td class=""p-4 text-right"">' + actions + '</td>';
                    tbody.appendChild(tr);
                });
            } catch (e) {
                showToast(e.message, true);
            }
        }

        async function retryMessage(id) {
            try {
                await fetchJson('./api/retry?module=' + currentModule + '&id=' + id, { method: 'POST' });
                showToast('Message queued for reprocessing successfully');
                await refreshData();
            } catch (e) {
                showToast(e.message, true);
            }
        }

        // Auto Refresh
        window.addEventListener('DOMContentLoaded', () => {
            refreshData();
            setInterval(refreshData, 5000);
        });
    </script>
</body>
</html>";
    }
}
