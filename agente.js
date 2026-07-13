// ─── GOOMER TOOLS — AGENTE ───────────────────────────────────────────────────
// Roda na máquina da loja, coleta dados e envia para o Supabase a cada 5 min.
// Instalação: npm install   →   node agente.js
// Como serviço: instale NSSM e rode: nssm install GoomerAgente "node" "C:\caminho\agente.js"

const mysql  = require('mysql2/promise');
const axios  = require('axios');
const { execSync } = require('child_process');
const fs     = require('fs');
const crypto = require('crypto');

// ── Versão do agente (SHA do commit — atualizado automaticamente) ─────────────
const AGENTE_VERSION  = '1.0.1'; // Incrementar a cada publicação: MAJOR.MINOR.PATCH
const GITHUB_RAW_USER = 'jadsonmenezes';
const GITHUB_RAW_REPO = 'goomer-noc';
const GITHUB_RAW_FILE = 'agente.js';
const GITHUB_RAW_BRANCH = 'main';

// ── Configuração Supabase ─────────────────────────────────────────────────────
const SUPABASE_URL      = 'https://jlatddmzhatbznbgfhfa.supabase.co';
const SUPABASE_KEY      = 'sb_secret_CPEAm2QPMkPyIMs3uRMZRQ_B2bC-3Rn';
const INTERVALO_MS      = 5 * 60 * 1000; // 5 minutos

// ── Configuração MySQL ────────────────────────────────────────────────────────
const DB_PASS  = 'QrhPWmxXDSBLT5pq';
const DB_PORTS = [3306, 3307, 3308];

let dbPortAtiva = null;
async function getDb() {
    if (dbPortAtiva) {
        try {
            return await mysql.createConnection({
                host:'127.0.0.1', user:'root', password:DB_PASS,
                database:'oimenu', timezone:'+00:00', port:dbPortAtiva
            });
        } catch(e) { dbPortAtiva = null; }
    }
    for (const port of DB_PORTS) {
        try {
            const conn = await mysql.createConnection({
                host:'127.0.0.1', user:'root', password:DB_PASS,
                database:'oimenu', timezone:'+00:00', port
            });
            dbPortAtiva = port;
            return conn;
        } catch(e) { continue; }
    }
    throw new Error('MySQL não encontrado');
}

async function getTokenAbrahao() {
    try {
        const db = await getDb();
        const [rows] = await db.execute('SELECT token FROM store WHERE deletedAt IS NULL AND token IS NOT NULL LIMIT 1');
        await db.end();
        if (rows.length > 0 && rows[0].token) return rows[0].token.trim();
    } catch(e) {}
    return null;
}

function pingHost(ip) {
    try {
        const result = execSync(`ping -n 2 -w 800 ${ip}`, { timeout:5000 }).toString();
        const matches = [...result.matchAll(/(?:tempo|time)[=<](\d+)ms/gi)];
        const times = matches.map(m => parseInt(m[1])).filter(n => !isNaN(n));
        const lossMatch = result.match(/(\d+)%\s+(?:de\s+)?(?:perda|loss)/i);
        const loss = lossMatch ? parseInt(lossMatch[1]) : 0;
        if (!times.length) return { latencia:null, perda:100, status:'sem resposta' };
        const avg = Math.round(times.reduce((a,b)=>a+b,0)/times.length);
        return { latencia:avg, perda:loss, status:loss===100?'sem resposta':loss>0?'instável':'ok' };
    } catch(e) { return { latencia:null, perda:100, status:'sem resposta' }; }
}

// ── Coletar snapshot completo ─────────────────────────────────────────────────
async function coletarSnapshot() {
    const db = await getDb();
    const snapshot = { coletado_em: new Date().toISOString() };

    try {
        // Loja
        const [cfgRows] = await db.execute('SELECT store_name, is_overdue, erp_id, local_server_port FROM app_configuration LIMIT 1');
        const [storeRows] = await db.execute('SELECT token FROM store WHERE deletedAt IS NULL LIMIT 1');
        snapshot.token_loja  = storeRows[0]?.token || 'sem_token';
        snapshot.nome_loja   = cfgRows[0]?.store_name || 'Loja';
        snapshot.inadimplente = cfgRows[0]?.is_overdue || false;
        snapshot.erp_id       = cfgRows[0]?.erp_id || null;
        snapshot.porta_local  = cfgRows[0]?.local_server_port || 5000;
        _portaLocalCache = parseInt(snapshot.porta_local) || 4999;
    } catch(e) { snapshot.erro_loja = e.message; }

    try {
        // Pedidos do dia + volume acumulado sem deletedAt (causa de deadlock)
        const [ped] = await db.execute(`SELECT COUNT(*) AS total, SUM(retry_expired) AS retries, SUM(sent_to_erp_error) AS erros_erp FROM sale_order WHERE DATE(createdAt)=CURDATE()`);
        const [acumulo] = await db.execute(`SELECT COUNT(*) AS total FROM sale_order WHERE sent_to_erp=1 AND deletedAt IS NULL AND createdAt < NOW() - INTERVAL 3 DAY`);
        snapshot.acumulo_sale_order = parseInt(acumulo[0].total || 0);
        snapshot.pedidos_hoje       = parseInt(ped[0].total || 0);
        snapshot.retry_expirados    = parseInt(ped[0].retries || 0);
        snapshot.erros_erp          = parseInt(ped[0].erros_erp || 0);
    } catch(e) {}

    try {
        // Logs de erro desde o último ciclo do agente
        // Usar janela do intervalo configurado para não recontar erros já vistos
        const minutosIntervalo = Math.ceil((INTERVALO_MS || 300000) / 60000);
        const [logs] = await db.execute(`SELECT COUNT(*) AS total FROM device_log WHERE level='error' AND created_at >= UTC_TIMESTAMP() - INTERVAL ${minutosIntervalo} MINUTE`);
        snapshot.erros_log = parseInt(logs[0].total || 0);
    } catch(e) {}

    try {
        // Tablets — banco como base
        const [tabs] = await db.execute(`
            SELECT t.id, t.note AS modelo, t.app_version, t.battery_level, t.wifi_level,
                   t.identity,
                   JSON_UNQUOTE(JSON_EXTRACT(t.data_send,'$.ip')) AS ip,
                   st.code AS mesa_numero,
                   ABS(TIMESTAMPDIFF(MINUTE,t.updatedAt,UTC_TIMESTAMP())) AS mins_sem_update
            FROM tablet t LEFT JOIN store_table st ON t.store_table_id=st.id
            WHERE t.deletedAt IS NULL
        `);
        const ativos = tabs.filter(t => t.app_version);
        snapshot.tablets_total  = tabs.length;
        snapshot.tablets_ativos = ativos.length;

        // Enriquecer com API Abrahão (bateria e sinal em tempo real)
        let abrahaoMap = {};
        try {
            const token = await getTokenAbrahao();
            if (token) {
                const resp = await axios.get(
                    `https://api.abrahao.com.br/tablets?token=${token}`,
                    { timeout:4000 }
                );
                const lista = resp.data?.data || [];
                lista.forEach(t => {
                    if (t.identity) abrahaoMap[t.identity.replace(/^dev-/,'')] = t;
                });
            }
        } catch(e) {}

        // Ping nos tablets ativos
        const tabletsComPing = await Promise.all(ativos.map(async t => {
            const key  = (t.identity||'').replace(/^dev-/,'');
            const api  = abrahaoMap[key];
            // API tem prioridade para dados voláteis
            const bateria = api ? parseInt(api.battery_level) : t.battery_level;
            const sinal   = api ? parseInt(api.wifi_level)    : t.wifi_level;
            const ip      = (t.ip && t.ip!=='null') ? t.ip : api?.ip;
            const ping = (ip && ip !== 'null') ? pingHost(ip) : { status:'sem_ip' };
            return {
                mesa:           t.mesa_numero,
                modelo:         t.modelo,
                ip,
                bateria,
                sinal,
                versao:         api?.app_version || t.app_version,
                mins_sem_update: t.mins_sem_update,
                fonte_bateria:  api ? 'api' : 'banco',
                ping
            };
        }));
        // Enriquecer cada tablet com dados de pedidos do dia
        try {
            const minutosIntervalo = Math.ceil((INTERVALO_MS || 300000) / 60000);
            const [pedidosPorTablet] = await db.execute(`
                SELECT 
                    tablet_identity,
                    COUNT(*)                                                          AS pedidos_hoje,
                    MIN(createdAt)                                                    AS primeiro_pedido,
                    MAX(createdAt)                                                    AS ultimo_pedido,
                    SUM(CASE WHEN HOUR(createdAt) >= 23 OR HOUR(createdAt) <= 5 
                        THEN 1 ELSE 0 END)                                            AS pedidos_madrugada,
                    MIN(CASE WHEN HOUR(createdAt) >= 23 OR HOUR(createdAt) <= 5 
                        THEN createdAt END)                                           AS primeira_madrugada,
                    MAX(CASE WHEN HOUR(createdAt) >= 23 OR HOUR(createdAt) <= 5 
                        THEN createdAt END)                                           AS ultima_madrugada
                FROM sale_order
                WHERE deletedAt IS NULL
                  AND DATE(createdAt) = CURDATE()
                  AND tablet_identity IS NOT NULL
                GROUP BY tablet_identity
            `);

            // Mapear por identity
            const pedMap = {};
            pedidosPorTablet.forEach(p => { pedMap[p.tablet_identity] = p; });

            // Enriquecer os tablets com dados de pedidos
            tabletsComPing = tabletsComPing.map(t => {
                const ped = pedMap[t.identity] || null;
                return {
                    ...t,
                    pedidos_hoje:       ped ? parseInt(ped.pedidos_hoje) : 0,
                    ultimo_pedido:      ped ? ped.ultimo_pedido : null,
                    primeiro_pedido:    ped ? ped.primeiro_pedido : null,
                    pedidos_madrugada:  ped ? parseInt(ped.pedidos_madrugada) : 0,
                    primeira_madrugada: ped ? ped.primeira_madrugada : null,
                    ultima_madrugada:   ped ? ped.ultima_madrugada : null,
                };
            });

            // Alertar se algum tablet tem pedidos de madrugada
            const tabletsMadrugada = tabletsComPing.filter(t => t.pedidos_madrugada > 0);
            if (tabletsMadrugada.length > 0) {
                snapshot.alertas_avisos = (snapshot.alertas_avisos||0) + 1;
                console.log(`⚠ Pedidos de madrugada: ${tabletsMadrugada.map(t=>`${t.mesa_numero||t.identity}(${t.pedidos_madrugada}x)`).join(', ')}`);
            }
        } catch(ePed) {}

        snapshot.tablets = tabletsComPing;

        // ── Modo Teste de Rede — executar bateria de pings se dentro da janela ──
        if (_testeRedeAtivo && tabletsComPing && tabletsComPing.length > 0) {
            // Não await — roda em background para não bloquear o snapshot
            executarTesteRede(tabletsComPing).catch(e =>
                console.log(`[TESTE-REDE] Erro: ${e.message}`)
            );
        }

        // Só contar como crítico tablets que têm IP (ativos na rede)
        // Tablets sem IP = desligados ou sem mesa atribuída — não é problema sistêmico
        snapshot.tablets_criticos = tabletsComPing.filter(t =>
            t.ip && t.ip !== 'null' && t.ip !== 'sem_ip' && (
                t.ping?.status === 'sem resposta' ||
                parseInt(t.bateria||0) < 15 ||
                parseInt(t.sinal||0) < 20
            )
        ).length;
    } catch(e) { snapshot.erro_tablets = e.message; }

    try {
        // Impressoras — banco + diagnóstico de conectividade
        const [imps] = await db.execute('SELECT name, uri, default_printer FROM printer WHERE deletedAt IS NULL');
        const minutosIntervaloImp = Math.ceil((INTERVALO_MS || 300000) / 60000);
        const [logsImp] = await db.execute(`SELECT COUNT(*) AS total FROM device_log WHERE type='print' AND level='error' AND created_at >= UTC_TIMESTAMP() - INTERVAL ${minutosIntervaloImp} MINUTE`);
        snapshot.impressoras_total = imps.length;
        snapshot.erros_impressora  = parseInt(logsImp[0].total || 0);

        // Testar conectividade de cada impressora
        const { execSync: esImp } = require('child_process');
        const net = require('net');

        const testarPortaTCP = (host, porta, timeout = 1500) => new Promise(resolve => {
            const sock = new net.Socket();
            sock.setTimeout(timeout);
            sock.on('connect', () => { sock.destroy(); resolve(true); });
            sock.on('error',   () => resolve(false));
            sock.on('timeout', () => { sock.destroy(); resolve(false); });
            try { sock.connect(porta, host); } catch(e) { resolve(false); }
        });

        const extrairIP = uri => {
            if (!uri) return null;
            // Formatos: socket://192.168.1.100:9100, 192.168.1.100, \\192.168.1.100\share
            const m = uri.match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/);
            if (!m) return null;
            // Excluir localhost/loopback — não são impressoras de rede real
            if (m[1] === '127.0.0.1' || m[1] === '0.0.0.0') return null;
            return m[1];
        };

        // Detectar tipo real da URI
        const tipoUri = uri => {
            if (!uri) return 'desconhecido';
            const u = uri.toLowerCase();
            if (u.includes('localhost') || u.includes('127.0.0.1')) return 'compartilhada_local';
            if (/\d+\.\d+\.\d+\.\d+/.test(u) && !u.includes('localhost')) return 'rede';
            if (u.startsWith('usb') || u.startsWith('\\\\') && !u.includes('localhost')) return 'usb';
            return 'outro';
        };

        const impsDiag = await Promise.all(imps.map(async i => {
            const ip     = extrairIP(i.uri);
            const isRede = !!ip;
            let ping_ms = null, porta_ok = null, status_win = null, jobs_presos = 0, porta_fisica = null;
            const tipoImp = tipoUri(i.uri);

            if (isRede) {
                // Ping ICMP via PowerShell
                try {
                    const pingOut = esImp(
                        `powershell -NoProfile -Command "Test-Connection -ComputerName '${ip}' -Count 1 -Quiet -BufferSize 16 | Write-Output; (Test-Connection -ComputerName '${ip}' -Count 1 -ErrorAction SilentlyContinue).ResponseTime"`,
                        { timeout: 4000, encoding: 'utf8' }
                    );
                    const linhas = pingOut.trim().split('\n').map(l => l.trim());
                    const respondeu = linhas[0] === 'True';
                    const ms = parseInt(linhas[1]);
                    ping_ms = respondeu ? (isNaN(ms) ? 1 : ms) : null;
                } catch(e) {}

                // TCP porta 9100 (impressão RAW)
                try { porta_ok = await testarPortaTCP(ip, 9100, 1500); } catch(e) { porta_ok = false; }
            }

            // Status via PowerShell Get-Printer (rede e USB)
            try {
                const nomeEsc = i.name.replace(/'/g, "''");
                const psOut = esImp(
                    `powershell -NoProfile -Command "Get-Printer -Name '${nomeEsc}' -ErrorAction SilentlyContinue | Select-Object -ExpandProperty PrinterStatus"`,
                    { timeout: 3000, encoding: 'utf8' }
                );
                status_win = psOut.trim() || null;
            } catch(e) {}

            // Jobs presos na fila
            try {
                const nomeEsc = i.name.replace(/'/g, "''");
                const jobsOut = esImp(
                    `powershell -NoProfile -Command "(Get-PrintJob -PrinterName '${nomeEsc}' -ErrorAction SilentlyContinue | Where-Object { $_.SubmittedTime -lt (Get-Date).AddMinutes(-5) } | Measure-Object).Count"`,
                    { timeout: 3000, encoding: 'utf8' }
                );
                jobs_presos = parseInt(jobsOut.trim()) || 0;
            } catch(e) {}

            // Diagnóstico consolidado
            let diag = 'ok';
            if (isRede) {
                if (ping_ms === null)       diag = 'offline_rede';
                else if (!porta_ok)         diag = 'sem_servico_impressao';
                else                        diag = 'online';
            }

            // Impressoras compartilhadas via localhost (\\localhost\Share)
            // Get-Printer pode retornar Normal mesmo sem dispositivo físico
            // Validar porta física para confirmar existência real
            if (tipoImp === 'compartilhada_local') {
                try {
                    const nomeEsc2 = i.name.replace(/'/g, "''");
                    const portaFisicaOut = esImp(
                        `powershell -NoProfile -Command "Get-Printer -Name '${nomeEsc2}' -ErrorAction SilentlyContinue | Select-Object -ExpandProperty PortName"`,
                        { timeout: 3000, encoding: 'utf8' }
                    ).trim();

                    porta_fisica = portaFisicaOut || null;

                    if (!portaFisicaOut) {
                        diag = 'sem_porta_fisica';
                    } else {
                        // Confirmar que a porta existe no sistema
                        const portaExiste = esImp(
                            `powershell -NoProfile -Command "(Get-PrinterPort -Name '${portaFisicaOut.replace(/'/g,"''")}' -ErrorAction SilentlyContinue | Measure-Object).Count"`,
                            { timeout: 3000, encoding: 'utf8' }
                        ).trim();

                        if (portaExiste === '0' || portaExiste === '') {
                            diag = 'porta_nao_encontrada';
                        } else {
                            diag = status_win === 'Normal' ? 'online' : (status_win||'desconhecido').toLowerCase();
                        }
                    }
                } catch(eLocal) { diag = 'erro_verificacao'; }
            }

            if (status_win && status_win.toLowerCase().includes('error'))    diag = 'erro_driver';
            if (status_win && status_win.toLowerCase().includes('offline'))  diag = 'offline_windows';
            if (status_win === 'Normal' && isRede && ping_ms !== null)       diag = 'online';
            if (status_win === 'Normal' && diag === 'ok')                    diag = 'online';
            if (jobs_presos > 0 && diag === 'online')                        diag = 'fila_travada';

            return {
                nome:        i.name,
                uri:         i.uri,
                padrao:      i.default_printer,
                tipo:        tipoImp,
                ip:          ip,
                ping_ms,
                porta_9100:  porta_ok,
                status_win,
                jobs_presos,
                porta_fisica,
                diag         // online | offline_rede | sem_servico_impressao | erro_driver | offline_windows | fila_travada | ok
            };
        }));

        snapshot.impressoras = impsDiag;

        // Correlacionar erros de log com status das impressoras
        const impsOffline = impsDiag.filter(i => i.diag !== 'online' && i.diag !== 'ok');
        if (snapshot.erros_impressora > 0 && impsOffline.length > 0) {
            snapshot.causa_erros_impressao = impsOffline.map(i =>
                `${i.nome}: ${i.diag}${i.ip ? ' ('+i.ip+')' : ''}`
            ).join('; ');
        } else if (snapshot.erros_impressora > 0 && impsOffline.length === 0) {
            snapshot.causa_erros_impressao = 'impressoras respondendo — erro pode ser spooler ou configuração';
        } else {
            snapshot.causa_erros_impressao = null;
        }

    } catch(e) { snapshot.impressoras = []; }


    // ── Teste 1: Conflito de portas ───────────────────────────────────────────
    // Verifica se as portas do servidor Goomer estão sendo usadas por outro processo
    try {
        const { execSync: esPort } = require('child_process');
        const portaGoomer = snapshot.porta_local || 4999;
        const portasVerificar = [...new Set([portaGoomer, 5000, 5001, 4999])];
        const conflitosPorta = [];

        for (const porta of portasVerificar) {
            try {
                // Listar processos usando a porta
                const out = esPort(
                    `powershell -NoProfile -Command "Get-NetTCPConnection -LocalPort ${porta} -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess"`,
                    { timeout: 3000, encoding: 'utf8' }
                ).trim();

                if (!out) continue;

                const pids = out.split('\n').map(p => parseInt(p.trim())).filter(p => !isNaN(p));
                for (const pid of pids) {
                    // Identificar o processo pelo PID
                    const procOut = esPort(
                        `powershell -NoProfile -Command "(Get-Process -Id ${pid} -ErrorAction SilentlyContinue).MainModule.FileName"`,
                        { timeout: 3000, encoding: 'utf8' }
                    ).trim();

                    const nomeProc = procOut || `PID ${pid}`;
                    const ehGoomer = nomeProc.toLowerCase().includes('servidor') ||
                                     nomeProc.toLowerCase().includes('abrahao') ||
                                     nomeProc.toLowerCase().includes('goomer');

                    if (!ehGoomer && nomeProc) {
                        conflitosPorta.push({ porta, processo: nomeProc, pid });
                        console.log(`⚠ CONFLITO PORTA: porta ${porta} usada por ${nomeProc} (PID ${pid})`);
                    }
                }
            } catch(e) {}
        }

        snapshot.conflitos_porta = conflitosPorta;
        if (conflitosPorta.length > 0) {
            snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
        }
    } catch(ePort) { snapshot.conflitos_porta = []; }

    // ── Teste 2: Conflito de IP e ARP ─────────────────────────────────────────
    // Verifica se tablets, impressoras ou outros dispositivos usam o mesmo IP do servidor
    // Também detecta conflito de MAC na tabela ARP
    try {
        const { execSync: esARP } = require('child_process');
        const ipServidor = snapshot.ip_servidor || snapshot.ip_configurado;
        const conflitosIP = [];

        if (ipServidor) {
            // Verificar se algum tablet tem o mesmo IP do servidor
            const tablets = snapshot.tablets || [];
            const impressoras = snapshot.impressoras || [];

            for (const t of tablets) {
                if (t.ip && t.ip === ipServidor) {
                    conflitosIP.push({
                        tipo: 'tablet',
                        nome: t.mesa ? `Mesa ${t.mesa}` : t.ip,
                        ip: t.ip,
                        problema: 'mesmo IP do servidor'
                    });
                }
            }
            for (const i of impressoras) {
                if (i.ip && i.ip === ipServidor) {
                    conflitosIP.push({
                        tipo: 'impressora',
                        nome: i.nome,
                        ip: i.ip,
                        problema: 'mesmo IP do servidor'
                    });
                }
            }

            // Verificar conflito de IP do servidor na rede via ARP ativo
            // Detecta quando OUTRO dispositivo está usando o mesmo IP do servidor
            // independente de estar no banco de dados ou não
            try {
                const esARP2 = require('child_process').execSync;

                // Detecção de conflito de IP via tabela ARP completa da sub-rede
                // O teste direto no próprio IP falha pois o Windows responde localmente
                // Estratégia: pingar todos os IPs conhecidos e verificar MACs na tabela ARP
                const os8 = require('os');

                // Coletar MACs da própria máquina
                const meusMACs = new Set();
                const meusIPs  = new Set();
                Object.values(os8.networkInterfaces()).forEach(iface => {
                    iface.forEach(addr => {
                        if (!addr.internal && addr.mac && addr.mac !== '00:00:00:00:00:00') {
                            meusMACs.add(addr.mac.toLowerCase().replace(/-/g, ':'));
                        }
                        if (!addr.internal && addr.family === 'IPv4') {
                            meusIPs.add(addr.address);
                        }
                    });
                });

                // Pingar todos os IPs conhecidos da loja para popular tabela ARP
                const ipsConhecidos = [
                    ...(snapshot.tablets||[]).filter(t=>t.ip).map(t=>t.ip),
                    ...(snapshot.impressoras||[]).filter(i=>i.ip).map(i=>i.ip),
                ].filter(ip => ip && ip !== ipServidor && !meusIPs.has(ip));

                for (const ip of ipsConhecidos) {
                    try { esARP2(`ping -n 1 -w 300 ${ip}`, { timeout: 1500, encoding: 'utf8' }); } catch(e) {}
                }

                // Ler tabela ARP completa via Get-NetNeighbor (mais completo que arp -a)
                let arpCompleto = '';
                try {
                    arpCompleto = esARP2(
                        `powershell -NoProfile -Command "Get-NetNeighbor -State Reachable,Stale -ErrorAction SilentlyContinue | Select-Object IPAddress,LinkLayerAddress | ConvertTo-Csv -NoTypeInformation"`,
                        { timeout: 5000, encoding: 'utf8' }
                    );
                } catch(e) {
                    // Fallback: arp -a
                    try { arpCompleto = esARP2('arp -a', { timeout: 3000, encoding: 'utf8' }); } catch(e2) {}
                }

                // Construir mapa IP→MACs da tabela ARP
                const arpMap2 = {};
                for (const linha of arpCompleto.split('\n')) {
                    // Formato CSV do Get-NetNeighbor: "IP","MAC"
                    const csvMatch = linha.match(/"([^"]+)","([0-9a-fA-F-:]+)"/);
                    if (csvMatch) {
                        const ip  = csvMatch[1];
                        const mac = csvMatch[2].toLowerCase().replace(/-/g, ':');
                        if (!arpMap2[ip]) arpMap2[ip] = new Set();
                        arpMap2[ip].add(mac);
                    }
                    // Formato arp -a: 192.168.1.x   aa-bb-cc-dd-ee-ff   dynamic
                    const arpMatch = linha.match(/(\d+\.\d+\.\d+\.\d+)\s+([0-9a-fA-F]{2}[-:][0-9a-fA-F]{2}[-:][0-9a-fA-F]{2}[-:][0-9a-fA-F]{2}[-:][0-9a-fA-F]{2}[-:][0-9a-fA-F]{2})/);
                    if (arpMatch) {
                        const ip  = arpMatch[1];
                        const mac = arpMatch[2].toLowerCase().replace(/-/g, ':');
                        if (!arpMap2[ip]) arpMap2[ip] = new Set();
                        arpMap2[ip].add(mac);
                    }
                }

                // Detectar conflito: MAC do servidor aparece em IP diferente do servidor
                for (const [ip, macs] of Object.entries(arpMap2)) {
                    if (meusIPs.has(ip)) continue; // pular nossos próprios IPs
                    for (const mac of macs) {
                        if (meusMACs.has(mac)) {
                            // Nosso MAC aparece num IP que não é o nosso — conflito
                            conflitosIP.push({
                                tipo:    'ip_conflito_rede',
                                ip:      ipServidor,
                                ip_arp:  ip,
                                mac_intruso: mac,
                                problema: `MAC do servidor aparece no IP ${ip} — possível conflito de IP`
                            });
                            snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
                            console.log(`⚠ Conflito de IP: MAC do servidor (${mac}) detectado no IP ${ip}`);
                        }
                    }
                }

                // Detectar: IPs conhecidos com múltiplos MACs = conflito ARP na rede
                for (const [ip, macs] of Object.entries(arpMap2)) {
                    if (macs.size > 1 && !meusIPs.has(ip)) {
                        conflitosIP.push({
                            tipo:    'arp',
                            ip,
                            macs:    [...macs],
                            problema: `${macs.size} MACs para o mesmo IP ${ip}`
                        });
                        if (ip === ipServidor) {
                            snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
                        }
                    }
                }

                // Detectar: tablet/impressora cadastrado com mesmo IP do servidor
                for (const t of (snapshot.tablets||[])) {
                    if (t.ip && meusIPs.has(t.ip)) {
                        conflitosIP.push({
                            tipo:    'ip_conflito_rede',
                            ip:      t.ip,
                            nome:    t.mesa ? `Mesa ${t.mesa}` : t.ip,
                            problema: `Tablet com mesmo IP do servidor (${t.ip})`
                        });
                        snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
                        console.log(`⚠ Tablet com IP igual ao servidor: ${t.ip}`);
                    }
                }

                // Passo 4: varrer tabela ARP completa para detectar outros conflitos
                const arpTudo = esARP2('arp -a', { timeout: 3000, encoding: 'utf8' });
                const linhasARP = arpTudo.split('\n');
                const arpMap = {};
                for (const linha of linhasARP) {
                    const partes = linha.trim().split(/\s+/);
                    if (partes.length >= 2) {
                        const ip  = partes[0].replace(/[()]/g, '');
                        const mac = partes[1].toLowerCase();
                        if (/\d+\.\d+\.\d+\.\d+/.test(ip) && (mac.includes('-') || mac.includes(':'))) {
                            if (!arpMap[ip]) arpMap[ip] = new Set();
                            arpMap[ip].add(mac);
                        }
                    }
                }
                // IPs com mais de um MAC = conflito ARP na rede
                for (const [ip, macs] of Object.entries(arpMap)) {
                    if (macs.size > 1) {
                        conflitosIP.push({
                            tipo:    'arp',
                            ip,
                            macs:    [...macs],
                            problema: `${macs.size} MACs diferentes para o IP ${ip}`
                        });
                        if (ip === ipServidor) {
                            snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
                        }
                    }
                }
            } catch(eARP) {}
        }

        snapshot.conflitos_ip = conflitosIP;
        if (conflitosIP.filter(c => c.tipo !== 'arp').length > 0) {
            snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
        }
    } catch(eIP) { snapshot.conflitos_ip = []; }

    // ── Teste 3: Detecção de agente duplicado ─────────────────────────────────
    // Verifica se outro agente está enviando snapshots para o mesmo token
    // comparando hostname e MAC da máquina atual com o último snapshot no Supabase
    try {
        const { execSync: esHW } = require('child_process');
        const os9 = require('os');

        // Coletar hostname e MAC da máquina atual
        const hostnameAtual = os9.hostname();

        // MAC principal (primeira interface não-loopback com MAC real)
        let macAtual = null;
        try {
            const ifaces = os9.networkInterfaces();
            for (const iface of Object.values(ifaces)) {
                for (const addr of iface) {
                    if (!addr.internal && addr.mac && addr.mac !== '00:00:00:00:00:00') {
                        macAtual = addr.mac;
                        break;
                    }
                }
                if (macAtual) break;
            }
        } catch(e) {}

        snapshot.hostname    = hostnameAtual;
        snapshot.agente_version = AGENTE_VERSION;
        snapshot.mac_address = macAtual;

        // Consultar último snapshot do mesmo token no Supabase
        // para verificar se veio de máquina diferente
        if (snapshot.token_loja && hostnameAtual) {
            try {
                const urlUlt = `${process.env.SUPABASE_URL||SUPABASE_URL}/rest/v1/snapshots?token_loja=eq.${encodeURIComponent(snapshot.token_loja)}&order=criado_em.desc&limit=2&select=payload`;
                const resUlt = await axios.get(urlUlt, {
                    headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` },
                    timeout: 4000
                });
                const ultimosSnaps = resUlt.data || [];

                // Verificar se o último snapshot (não o atual) veio de hostname diferente
                // Comparar case-insensitive — Windows pode variar capitalização
                const hostnameAtualNorm = (hostnameAtual||'').toLowerCase().trim();
                const snapAnterior = ultimosSnaps.find(s => {
                    const h = (s.payload?.hostname||'').toLowerCase().trim();
                    return h && h !== hostnameAtualNorm;
                });

                if (snapAnterior) {
                    const hostnameAnterior = snapAnterior.payload.hostname;
                    const macAnterior      = snapAnterior.payload.mac_address;
                    snapshot.agente_duplicado = {
                        detectado: true,
                        hostname_atual:    hostnameAtual,
                        hostname_anterior: hostnameAnterior,
                        mac_atual:         macAtual,
                        mac_anterior:      macAnterior
                    };
                    snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
                    console.log(`⚠ AGENTE DUPLICADO: token ${snapshot.token_loja} também enviando de ${hostnameAnterior}`);
                } else {
                    snapshot.agente_duplicado = { detectado: false };
                }
            } catch(eDup) {
                snapshot.agente_duplicado = { detectado: false };
            }
        }
    } catch(eHW) {}


    try {
        // Informações do banco MySQL
        const [versaoDb] = await db.execute('SELECT VERSION() AS v');
        const [instDb]   = await db.execute('SELECT MIN(createdAt) AS dt FROM store');
        snapshot.db_versao    = versaoDb[0]?.v || null;
        snapshot.db_port      = dbPortAtiva;
        snapshot.db_instalado = instDb[0]?.dt || null;
    } catch(e) {}

    try {
        // IDs de acesso remoto (AnyDesk e TeamViewer)
        const { execSync: esAR } = require('child_process');

        // ── AnyDesk ──────────────────────────────────────────────────────────
        let anydeskId = null;
        try {
            // Método 1: linha de comando
            const anydeskPaths = [
                '"C:\\Program Files (x86)\\AnyDesk\\AnyDesk.exe"',
                '"C:\\Program Files\\AnyDesk\\AnyDesk.exe"',
            ];
            for (const p of anydeskPaths) {
                try {
                    const out = esAR(`${p} --get-id`, { timeout: 4000, encoding: 'utf8' }).trim();
                    if (out && /^\d+$/.test(out.trim())) {
                        anydeskId = out.trim();
                        break;
                    }
                } catch(e) {}
            }
            // Método 2: arquivo de configuração
            if (!anydeskId) {
                const fs3 = require('fs');
                const confPaths = [
                    'C:\\ProgramData\\AnyDesk\\system.conf',
                    'C:\\Users\\Goomer\\AppData\\Roaming\\AnyDesk\\system.conf',
                ];
                for (const p of confPaths) {
                    try {
                        if (fs3.existsSync(p)) {
                            const content = fs3.readFileSync(p, 'utf8');
                            const match = content.match(/ad\.anynet\.id=(\d+)/);
                            if (match) { anydeskId = match[1]; break; }
                        }
                    } catch(e) {}
                }
            }
        } catch(e) {}

        // ── TeamViewer ───────────────────────────────────────────────────────
        let teamviewerId = null;
        try {
            // Registro do Windows — mais confiável
            const tvKeys = [
                'HKLM\\SOFTWARE\\WOW6432Node\\TeamViewer',
                'HKLM\\SOFTWARE\\TeamViewer',
            ];
            for (const key of tvKeys) {
                try {
                    const out = esAR(
                        `reg query "${key}" /v ClientID 2>nul`,
                        { timeout: 3000, encoding: 'utf8' }
                    );
                    const match = out.match(/ClientID\s+REG_DWORD\s+(0x[\da-fA-F]+)/);
                    if (match) {
                        teamviewerId = String(parseInt(match[1], 16));
                        break;
                    }
                } catch(e) {}
            }
        } catch(e) {}

        snapshot.anydesk_id    = anydeskId;
        snapshot.teamviewer_id = teamviewerId;

        if (anydeskId)    console.log(`[AR] AnyDesk ID: ${anydeskId}`);
        if (teamviewerId) console.log(`[AR] TeamViewer ID: ${teamviewerId}`);
        if (!anydeskId && !teamviewerId) console.log(`[AR] Nenhuma ferramenta de acesso remoto encontrada`);
    } catch(e) { snapshot.anydesk_id = null; snapshot.teamviewer_id = null; }

    try {
        // Versão do servidor Goomer — extraída do bundle.js
        // Testa múltiplas portas pois varia por instalação (4999, 5000, 5001...)
        let gVersion = null;
        const portaBase = snapshot.porta_local || 5000;
        const portasTeste = [...new Set([portaBase, 4999, 5000, 5001, 5002, 5003, 5004])];

        for (const porta of portasTeste) {
            try {
                const resp = await axios.get(`http://localhost:${porta}/bundle.js`, {
                    timeout: 4000,
                    responseType: 'text'
                });
                const match = resp.data.match(/"name"\s*:\s*"abrahao-servidor"[^}]*?"version"\s*:\s*"([^"]+)"/);
                if (match) {
                    gVersion = match[1];
                    console.log(`✅ Versão Goomer: ${gVersion} (porta ${porta})`);
                    break;
                }
            } catch(e) {}
        }

        snapshot.goomer_version = gVersion;
    } catch(e) { snapshot.goomer_version = null; }

    // ── Diagnóstico 1: Conflito de portas ────────────────────────────────────
    // Detecta processos de terceiros ocupando portas que o Goomer precisa
    try {
        const { execSync: esPorta } = require('child_process');

        // Portas que o Goomer usa
        const portaServidor = snapshot.porta_local || 4999;
        const portasGoomer  = [...new Set([portaServidor, 4999, 5000, 5001])];

        const portasOut = esPorta(
            'powershell -NoProfile -Command "Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Select-Object LocalPort,OwningProcess | ConvertTo-Json"',
            { timeout: 5000, encoding: 'utf8' }
        );

        let conflitosPorta = [];
        try {
            const conexoes = JSON.parse(portasOut);
            const lista = Array.isArray(conexoes) ? conexoes : [conexoes];

            for (const porta of portasGoomer) {
                const usando = lista.filter(c => Number(c.LocalPort) === porta);
                for (const conn of usando) {
                    // Verificar se o processo é o servidor Goomer
                    try {
                        const proc = esPorta(
                            `powershell -NoProfile -Command "(Get-Process -Id ${conn.OwningProcess} -ErrorAction SilentlyContinue).Name"`,
                            { timeout: 2000, encoding: 'utf8' }
                        ).trim().toLowerCase();

                        const ehGoomer = proc.includes('goomer') || proc.includes('abrahao') || proc.includes('node');
                        if (!ehGoomer && proc) {
                            conflitosPorta.push({ porta, processo: proc, pid: conn.OwningProcess });
                        }
                    } catch(e) {}
                }
            }
        } catch(e) {}

        snapshot.conflitos_porta = conflitosPorta.length > 0 ? conflitosPorta : null;
        if (conflitosPorta.length > 0) {
            console.log(`⚠ [PORTA] Conflito detectado: ${conflitosPorta.map(c=>`${c.processo} na porta ${c.porta}`).join(', ')}`);
            snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
        }
    } catch(ePorta) {}

    // ── Diagnóstico 2: Conflito de IP (ARP e sobreposição) ───────────────────
    // Detecta tablets/impressoras com o mesmo IP do servidor (conflito DHCP)
    // e duplicação de MAC na tabela ARP (dois dispositivos com mesmo IP)
    try {
        const { execSync: esArp } = require('child_process');
        const ipServidor = snapshot.ip_servidor || snapshot.ip_configurado;
        let conflitosIP = [];

        if (ipServidor) {
            // Verificar se algum tablet ou impressora tem o mesmo IP do servidor
            const tabletIPs = (snapshot.tablets   || []).map(t => t.ip).filter(Boolean);
            const impIPs    = (snapshot.impressoras|| []).map(i => i.ip).filter(Boolean);

            const duplicados = [...tabletIPs, ...impIPs].filter(ip => ip === ipServidor);
            if (duplicados.length > 0) {
                conflitosIP.push({ tipo: 'ip_duplicado', ip: ipServidor, descricao: `IP do servidor (${ipServidor}) também está em uso por tablet/impressora` });
            }

            // Verificar tabela ARP — múltiplos MACs para o mesmo IP
            try {
                const arpOut = esArp('arp -a', { timeout: 3000, encoding: 'utf8' });
                const linhas = arpOut.split('\n').map(l => l.trim()).filter(Boolean);

                const mapIPMAC = {};
                for (const linha of linhas) {
                    // Formato: 192.168.1.14  aa-bb-cc-dd-ee-ff  dynamic
                    const m = linha.match(/(\d+\.\d+\.\d+\.\d+)\s+([0-9a-f-]{17})/i);
                    if (m) {
                        const [, ip, mac] = m;
                        if (!mapIPMAC[ip]) mapIPMAC[ip] = [];
                        mapIPMAC[ip].push(mac);
                    }
                }

                // IPs relevantes (servidor + tablets + impressoras)
                const ipsMonitorados = [ipServidor, ...tabletIPs, ...impIPs].filter(Boolean);
                for (const ip of ipsMonitorados) {
                    const macs = mapIPMAC[ip];
                    if (macs && macs.length > 1) {
                        conflitosIP.push({ tipo: 'conflito_arp', ip, macs, descricao: `IP ${ip} com ${macs.length} MACs diferentes — conflito de endereço na rede` });
                    }
                }
            } catch(eArp) {}
        }

        snapshot.conflitos_ip = conflitosIP.length > 0 ? conflitosIP : null;
        if (conflitosIP.length > 0) {
            console.log(`⚠ [IP-CONFLICT] ${conflitosIP.map(c=>c.descricao).join(' | ')}`);
            snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
        }
    } catch(eIP) {}

    // ── Diagnóstico 3: Agente duplicado (mesmo token, outra máquina) ──────────
    // Se outro agente para o mesmo token estiver enviando snapshots, o NOC
    // fica oscilando entre dois hosts — identificar e alertar
    try {
        const os3 = require('os');
        const hostname_atual = os3.hostname();

        // Buscar no Supabase o hostname do último snapshot diferente do atual
        const urlDup = `${SUPABASE_URL}/rest/v1/snapshots?token_loja=eq.${encodeURIComponent(snapshot.token_loja || '')}&order=criado_em.desc&limit=5&select=payload`;
        const resDup = await axios.get(urlDup, {
            headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` },
            timeout: 4000
        });

        const snapshots_recentes = resDup.data || [];
        const hosts_vistos = [...new Set(
            snapshots_recentes
                .map(s => s.payload?.hostname)
                .filter(h => h && h !== hostname_atual)
        )];

        if (hosts_vistos.length > 0) {
            snapshot.agente_duplicado = {
                detectado:     true,
                hosts_outros:  hosts_vistos,
                host_atual:    hostname_atual,
                descricao:     `Agente detectado em outra(s) máquina(s): ${hosts_vistos.join(', ')}`
            };
            console.log(`⚠ [DUPLICADO] Outro agente ativo para este token: ${hosts_vistos.join(', ')}`);
            snapshot.alertas_avisos = (snapshot.alertas_avisos||0) + 1;
        } else {
            snapshot.agente_duplicado = null;
        }
    } catch(eDup) { snapshot.agente_duplicado = null; }

    if (_config.coleta_maquina) try {
        // Performance da máquina via PowerShell
        const { execSync } = require('child_process');
        const fs = require('fs');
        const script = [
            '$cpu = (Get-WmiObject Win32_Processor | Measure-Object -Property LoadPercentage -Average).Average',
            '$os  = Get-WmiObject Win32_OperatingSystem',
            '$ram = [math]::Round($os.TotalVisibleMemorySize/1MB,1)',
            '$ramUsed = [math]::Round(($os.TotalVisibleMemorySize-$os.FreePhysicalMemory)/1MB,1)',
            '$ramPct  = [math]::Round($ramUsed/$ram*100)',
            '$discos  = Get-WmiObject Win32_LogicalDisk -Filter "DriveType=3" | ForEach-Object {',
            '    "$($_.DeviceID)|$([math]::Round(($_.Size-$_.FreeSpace)/1GB,1))|$([math]::Round($_.Size/1GB,1))|$([math]::Round(($_.Size-$_.FreeSpace)/$_.Size*100))"',
            '}',
            '$uptime = (Get-Date)-$os.ConvertToDateTime($os.LastBootUpTime)',
            '[PSCustomObject]@{',
            '    cpu=$cpu; ram=$ram; ramUsed=$ramUsed; ramPct=$ramPct',
            '    os=$os.Caption; build=$os.BuildNumber',
            '    host=$env:COMPUTERNAME',
            '    uptime="{0}d {1:D2}h {2:D2}m" -f [int]$uptime.TotalDays,$uptime.Hours,$uptime.Minutes',
            '    discos=$discos -join ";"',
            '} | ConvertTo-Json -Compress'
        ].join("\n");
        const tmp = require('os').tmpdir() + '\\gt_ag_perf_' + Date.now() + '.ps1';
        try {
            fs.writeFileSync(tmp, script, 'utf8');
            const raw = execSync('powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + tmp + '"',
                { timeout:10000, encoding:'utf8' }).trim();
            const d = JSON.parse(raw);
            snapshot.maquina = {
                cpu:     d.cpu,
                ram:     d.ram,
                ram_usado: d.ramUsed,
                ram_pct: d.ramPct,
                so:      d.os,
                build:   d.build,
                host:    d.host,
                uptime:  d.uptime,
                discos:  (d.discos||'').split(';').filter(Boolean).map(s=>{
                    const p=s.split('|');
                    return {drive:p[0],usado:p[1],total:p[2],pct:parseInt(p[3])};
                })
            };
        } finally { try { require('fs').unlinkSync(tmp); } catch(e){} }
    } catch(e) { snapshot.maquina = null; }

    try {
        // IP real da máquina
        const os = require('os');
        const ifaces = os.networkInterfaces();
        const ips = [];
        Object.values(ifaces).forEach(list => {
            (list||[]).forEach(i => {
                if (i.family === 'IPv4' && !i.internal && !i.address.startsWith('169.254')) {
                    ips.push(i.address);
                }
            });
        });
        snapshot.ip_servidor  = ips[0] || null;
        snapshot.ips_servidor = ips;
    } catch(e) {}

    try {
        // Verificar divergência entre IP configurado no Goomer e IP real da máquina
        // Se divergirem, os tablets não conseguem se comunicar com o servidor
        const db2 = await getDb();
        const [cfgIp] = await db2.execute(
            'SELECT local_server_ip, local_server_port FROM app_configuration LIMIT 1'
        );
        await db2.end();

        // IP configurado — pode ser null quando está em modo "obter automaticamente"
        const ipConfiguradoBruto = cfgIp[0]?.local_server_ip;
        const ipConfigurado = (ipConfiguradoBruto && ipConfiguradoBruto.trim() !== '' && ipConfiguradoBruto !== '0.0.0.0')
            ? ipConfiguradoBruto.trim()
            : null;

        snapshot.ip_configurado = ipConfigurado;

        // Função de suporte
        const subnet = ip => ip.split('.').slice(0,3).join('.');
        const ipValido = ip => ip && ip !== 'null' && /^\d+\.\d+\.\d+\.\d+$/.test(ip);

        // IPs reais da máquina
        const maquinaIPs = (snapshot.ips_servidor||[]).filter(ipValido);

        // IPs dos tablets para cruzamento de sub-rede
        const tabletIPs = (snapshot.tablets||[])
            .map(t => t.ip)
            .filter(ipValido);

        // ── Função de autocorreção ─────────────────────────────────────────
        const tentarCorrigirIP = async (ipAntigo, motivoChamada) => {
            // Sub-rede predominante dos tablets
            const subnetCount = {};
            tabletIPs.forEach(ip => {
                const s = subnet(ip);
                subnetCount[s] = (subnetCount[s]||0) + 1;
            });
            const subnetTablets = Object.entries(subnetCount)
                .sort((a,b)=>b[1]-a[1])[0]?.[0] || null;

            // IP da máquina na mesma sub-rede dos tablets
            let ipCorreto = null;
            let metodo = '';

            if (subnetTablets) {
                ipCorreto = maquinaIPs.find(ip => subnet(ip) === subnetTablets);
                if (ipCorreto) metodo = `sub-rede dos tablets (${subnetTablets}.x)`;
                else console.log(`[IP-AUTO] Nenhum IP da máquina está na sub-rede dos tablets (${subnetTablets}.x) | máquina: [${maquinaIPs.join(', ')}]`);
            }

            // Fallback: único IP disponível na máquina
            // Só usar fallback se não há tablets cadastrados (não dá pra validar sub-rede)
            if (!ipCorreto && maquinaIPs.length === 1 && tabletIPs.length === 0) {
                ipCorreto = maquinaIPs[0];
                metodo = 'único IP disponível (sem tablets para validar sub-rede)';
            } else if (!ipCorreto && tabletIPs.length > 0) {
                // Nenhum IP da máquina está na sub-rede dos tablets
                // Nenhum IP da máquina está na sub-rede dos tablets
                // Pode ser rede com múltiplas sub-redes e roteamento entre elas
                // Aplicar melhor IP disponível com aviso — prioridade: único > primeiro
                if (maquinaIPs.length === 1) {
                    ipCorreto = maquinaIPs[0];
                    metodo = `único IP da máquina (sub-redes distintas: máquina ${subnet(ipCorreto)}.x, tablets ${subnetTablets}.x — pode funcionar se houver roteamento)`;
                } else {
                    // Múltiplos IPs — preferir o que não é link-local (169.254.x.x)
                    const ipFiltrado = maquinaIPs.find(ip => !ip.startsWith('169.254'));
                    ipCorreto = ipFiltrado || maquinaIPs[0];
                    metodo = `primeiro IP da máquina (sub-redes distintas: máquina ${subnet(ipCorreto)}.x, tablets ${subnetTablets}.x — verificar se há roteamento)`;
                }
                console.log(`[IP-AUTO] Sub-redes diferentes mas aplicando IP: ${ipCorreto} (${metodo})`);
            }

            // Fallback 2: primeiro IP da máquina quando não há tablets cadastrados
            if (!ipCorreto && maquinaIPs.length > 0 && tabletIPs.length === 0) {
                ipCorreto = maquinaIPs[0];
                metodo = 'primeiro IP da máquina (sem tablets cadastrados)';
            }

            if (!snapshot.autocorrecoes) snapshot.autocorrecoes = [];
            // Merge autocorreções pendentes (ex: servidor iniciado antes do snapshot)
            if (_autocorrecoesPendentes.length > 0) {
                snapshot.autocorrecoes.push(..._autocorrecoesPendentes);
                _autocorrecoesPendentes = [];
            }

            if (ipCorreto) {
                try {
                    const dbFix = await getDb();
                    await dbFix.execute(
                        'UPDATE app_configuration SET local_server_ip = ? WHERE id = 1',
                        [ipCorreto]
                    );
                    await dbFix.end();
                    console.log(`✅ IP sincronizado no banco: ${ipAntigo||'vazio'} → ${ipCorreto} (${metodo})`);
                    snapshot.ip_configurado  = ipCorreto;
                    snapshot.ip_divergente   = false;
                    snapshot.ip_servidor     = ipCorreto;
                    snapshot.alertas_criticos = Math.max(0, (snapshot.alertas_criticos||0) - 1);

                    // ── Notificar o servidor Goomer para recarregar o IP ──────
                    // Estratégia: editar config.json no disco diretamente
                    // Evita o problema de GET /configurations não retornar campo db
                    // e o POST sobrescrever o arquivo perdendo db.port
                    let reloadOk = false;
                    const portaLocal = snapshot.porta_local || 4999;
                    const portasTeste = [...new Set([portaLocal,4999,5000,5001,5002,5003,5004])];

                    try {
                        const fs8   = require('fs');
                        const cpLib = require('child_process');

                        // Encontrar o config.json do servidor Abrahão
                        let configPath = null;
                        let cfgDisco   = null;

                        // Listar usuários para montar os caminhos
                        let usuarios8 = ['Goomer'];
                        try {
                            const usrsOut = cpLib.execSync(
                                'powershell -NoProfile -Command "Get-ChildItem C:\\Users | Select-Object -ExpandProperty Name"',
                                { timeout: 3000, encoding: 'utf8' }
                            );
                            usuarios8 = usrsOut.trim().split('\n').map(u => u.trim()).filter(Boolean);
                        } catch(e) {}

                        const candidatosCfg = [];
                        for (const u of usuarios8) {
                            candidatosCfg.push(
                                `C:\\Users\\${u}\\AppData\\Roaming\\Goomer - Servidor\\config.json`,
                                `C:\\Users\\${u}\\AppData\\Roaming\\abrahao-servidor\\config.json`,
                                `C:\\Users\\${u}\\AppData\\Local\\Programs\\abrahao-servidor\\config.json`
                            );
                        }

                        for (const p of candidatosCfg) {
                            try {
                                if (fs8.existsSync(p)) {
                                    cfgDisco   = JSON.parse(fs8.readFileSync(p, 'utf8'));
                                    configPath = p;
                                    console.log(`[IP-RELOAD] config.json encontrado: ${p}`);
                                    console.log(`[IP-RELOAD] db.port no disco: ${cfgDisco?.db?.port || 'não encontrado'}`);
                                    break;
                                }
                            } catch(e) {}
                        }

                        if (configPath && cfgDisco) {
                            // Modificar APENAS o campo do IP — preservar tudo mais
                            const ipAnterior = cfgDisco.apiOimenuLocalIp;
                            cfgDisco.apiOimenuLocalIp = ipCorreto;

                            // Salvar de volta no disco
                            fs8.writeFileSync(configPath, JSON.stringify(cfgDisco, null, 2), 'utf8');
                            console.log(`[IP-RELOAD] config.json salvo: ip ${ipAnterior} → ${ipCorreto} | db.port preservado: ${cfgDisco?.db?.port || '?'}`);
                            reloadOk = true;
                        } else {
                            // Fallback: usar GET+POST quando config.json não encontrado
                            console.log(`[IP-RELOAD] config.json não encontrado — tentando via API`);
                            for (const porta of portasTeste) {
                                let cfgAtualFb = null;
                                try {
                                    const resGet = await axios.get(`http://localhost:${porta}/configurations`, { timeout: 3000 });
                                    cfgAtualFb = resGet.data;
                                } catch(e) { continue; }
                                if (!cfgAtualFb) continue;

                                const cfgNovaFb = { ...cfgAtualFb, apiOimenuLocalIp: ipCorreto };
                                for (const method of ['put','post','patch']) {
                                    try {
                                        const res = await axios[method](
                                            `http://localhost:${porta}/configurations`,
                                            cfgNovaFb,
                                            { timeout: 4000, headers: { 'Content-Type': 'application/json' } }
                                        );
                                        if (res.status < 400) {
                                            console.log(`[IP-RELOAD] IP salvo via API ${method.toUpperCase()} :${porta}`);
                                            reloadOk = true;
                                            break;
                                        }
                                    } catch(e) {}
                                }
                                if (reloadOk) break;
                            }
                        }
                    } catch(eConfig) {
                        console.log(`[IP-RELOAD] Erro ao salvar config: ${eConfig.message}`);
                    }

                    // Reiniciar servidor se conseguiu salvar a config
                    if (reloadOk) {
                        await new Promise(r => setTimeout(r, 1000));
                        try {
                            const { execSync: esRst, spawn: spRst } = require('child_process');
                            const fs6 = require('fs');

                            let usuarios9 = ['Goomer'];
                            try {
                                const usrOut = esRst('powershell -NoProfile -Command "Get-ChildItem C:\\Users | Select-Object -ExpandProperty Name"', { timeout: 3000, encoding: 'utf8' });
                                usuarios9 = usrOut.trim().split('\n').map(u => u.trim()).filter(Boolean);
                            } catch(eUsr) {}

                            const candidatos = [];
                            for (const u of usuarios9) {
                                candidatos.push(
                                    `C:\\Users\\${u}\\AppData\\Local\\Programs\\abrahao-servidor\\Goomer - Servidor.exe`,
                                    `C:\\Users\\${u}\\AppData\\Local\\Programs\\abrahao-servidor\\abrahao-servidor.exe`,
                                    `C:\\Users\\${u}\\AppData\\Local\\Programs\\Servidor Goomer\\Goomer - Servidor.exe`
                                );
                            }

                            let exePath = null;
                            for (const p of candidatos) {
                                try { if (fs6.existsSync(p)) { exePath = p; break; } } catch(e) {}
                            }
                            if (!exePath) {
                                try {
                                    const psOut = esRst(
                                        'powershell -NoProfile -Command "(Get-Process | Where-Object {$_.MainModule -and ($_.MainModule.FileName -like '*servidor*' -or $_.MainModule.FileName -like '*abrahao*')} | Select-Object -First 1).MainModule.FileName"',
                                        { timeout: 5000, encoding: 'utf8' }
                                    );
                                    const fp = psOut.trim();
                                    try { if (fp && fs6.existsSync(fp)) exePath = fp; } catch(e) {}
                                } catch(ePs) {}
                            }

                            if (exePath) {
                                const nomeExe = exePath.split('\\').pop();
                                const dirExe  = exePath.substring(0, exePath.lastIndexOf('\\'));
                                console.log(`[IP-RELOAD] Executável: ${exePath}`);

                                try {
                                    esRst(`powershell -NoProfile -Command "Stop-Process -Name '${nomeExe.replace('.exe','')}' -Force -ErrorAction SilentlyContinue"`, { timeout: 6000, encoding: 'utf8' });
                                    console.log(`[IP-RELOAD] Encerrado: ${nomeExe}`);
                                } catch(eStop) {
                                    try { esRst(`taskkill /IM "${nomeExe}" /F`, { timeout: 5000, encoding: 'utf8' }); } catch(eT) {}
                                }

                                await new Promise(r => setTimeout(r, 3000));

                                let spawnou = false;
                                try {
                                    spRst(exePath, [], { detached: true, stdio: 'ignore', cwd: dirExe, shell: false }).unref();
                                    spawnou = true;
                                } catch(eSpawn) {}

                                if (!spawnou) {
                                    try {
                                        esRst(`schtasks /Create /TN "GoomerServidorReinicio" /TR "${exePath}" /SC ONCE /ST 00:00 /RL HIGHEST /F`, { timeout: 5000, encoding: 'utf8', shell: true });
                                        esRst('schtasks /Run /TN "GoomerServidorReinicio"', { timeout: 5000, encoding: 'utf8' });
                                        setTimeout(() => { try { esRst('schtasks /Delete /TN "GoomerServidorReinicio" /F', { timeout: 3000, encoding: 'utf8' }); } catch(e) {} }, 10000);
                                        spawnou = true;
                                        console.log(`[IP-RELOAD] Servidor acionado via Scheduled Task`);
                                    } catch(eTask) {
                                        console.log(`[IP-RELOAD] Task falhou: ${eTask.message}`);
                                    }
                                }
                                if (!spawnou) {
                                    try { esRst(`cmd /c start "" "${exePath}"`, { timeout: 3000, encoding: 'utf8', shell: true }); spawnou = true; } catch(e) {}
                                }

                                if (spawnou) {
                                    console.log(`✅ [IP-RELOAD] Servidor reiniciado: ${ipCorreto} — aguardando inicialização...`);
                                    let servidorOnline = false;
                                    for (let tentativa = 0; tentativa < 15; tentativa++) {
                                        await new Promise(r => setTimeout(r, 3000));
                                        for (const p of portasTeste) {
                                            try {
                                                const chk = await axios.get(`http://localhost:${p}/configurations`, { timeout: 2000 });
                                                if (chk.status < 400) {
                                                    console.log(`✅ [IP-RELOAD] Servidor online na porta ${p} após ${(tentativa+1)*3}s`);
                                                    servidorOnline = true; break;
                                                }
                                            } catch(e) {}
                                        }
                                        if (servidorOnline) break;
                                    }
                                    if (!servidorOnline) console.log(`[IP-RELOAD] ⚠ Servidor não respondeu em 45s`);
                                } else {
                                    console.log(`[IP-RELOAD] ⚠ Não foi possível reiniciar — faça manualmente`);
                                }
                            } else {
                                console.log(`[IP-RELOAD] ⚠ Executável não encontrado — reinicie manualmente`);
                            }
                        } catch(eReinicio) {
                            console.log(`[IP-RELOAD] Erro no reinício: ${eReinicio.message}`);
                        }
                    } else {
                        console.log(`[IP-RELOAD] ⚠ Não foi possível salvar config — reinicialização manual necessária`);
                    }
                } catch(eFix) {
                    console.log(`[IP-AUTO] Erro ao gravar: ${eFix.message}`);
                    snapshot.autocorrecoes.push({
                        tipo: 'ip_sincronizado', horario: new Date().toISOString(),
                        ip_antigo: ipAntigo||'vazio', ip_novo: null,
                        metodo: `erro ao gravar: ${eFix.message}`, resultado: 'falhou'
                    });
                }
            } else {
                console.log(`[IP-AUTO] Não foi possível identificar IP correto. Máquina: [${maquinaIPs.join(', ')}] Tablets: [${tabletIPs.join(', ')}]`);
                snapshot.autocorrecoes.push({
                    tipo: 'ip_sincronizado', horario: new Date().toISOString(),
                    ip_antigo: ipAntigo||'vazio', ip_novo: null,
                    metodo: 'nenhum IP compatível encontrado', resultado: 'falhou'
                });
            }
        };

        // ── Caso 1: IP configurado está vazio/null
        if (!ipConfigurado && maquinaIPs.length > 0) {
            snapshot.ip_divergente = true;
            snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;
            if (_config.autocorrecao_ip) {
                console.log(`[IP] Configurado vazio — tentando sincronização automática`);
                await tentarCorrigirIP(null);
            } else {
                console.log(`[IP] Configurado vazio — autocorreção DESATIVADA por flag`);
            }

        // ── Caso 2: verificar se o IP configurado alcança os tablets ─────────────
        // Critério correto: não basta o IP existir na máquina —
        // ele precisa estar na mesma sub-rede dos tablets para operação
        } else if (ipConfigurado && maquinaIPs.length > 0) {

            // Sub-rede predominante dos tablets (mesmo cálculo de tentarCorrigirIP)
            const subnetCount = {};
            tabletIPs.forEach(ip => {
                const s = subnet(ip);
                subnetCount[s] = (subnetCount[s]||0) + 1;
            });
            const subnetTablets = Object.entries(subnetCount)
                .sort((a,b) => b[1]-a[1])[0]?.[0] || null;

            // IP está na máquina?
            const ipNaMaquina = maquinaIPs.includes(ipConfigurado);

            // IP está na sub-rede dos tablets?
            const ipNaSubredeTablets = subnetTablets
                ? subnet(ipConfigurado) === subnetTablets
                : true; // sem tablets cadastrados — não pode avaliar, assume OK

            // Existe algum IP da máquina na sub-rede dos tablets?
            const ipCorretoDisponivel = subnetTablets
                ? maquinaIPs.find(ip => subnet(ip) === subnetTablets)
                : null;

            // Divergência: IP não está na máquina OU está na máquina mas não na sub-rede dos tablets
            // (desde que existam tablets cadastrados com IP para servir de referência)
            const ipDivergente = !ipNaMaquina ||
                (subnetTablets && !ipNaSubredeTablets && !!ipCorretoDisponivel);

            snapshot.ip_divergente = ipDivergente;

            if (ipDivergente) {
                snapshot.alertas_criticos = (snapshot.alertas_criticos||0) + 1;

                if (!ipNaMaquina) {
                    console.log(`⚠ IP DIVERGENTE: configurado=${ipConfigurado} não está na máquina | IPs=[${maquinaIPs.join(', ')}]`);
                } else {
                    console.log(`⚠ IP FORA DA REDE DOS TABLETS: configurado=${ipConfigurado} (sub-rede ${subnet(ipConfigurado)}) | tablets em ${subnetTablets}.x | IP correto disponível: ${ipCorretoDisponivel}`);
                }

                if (_config.autocorrecao_ip) {
                    await tentarCorrigirIP(ipConfigurado, 'divergente');
                } else {
                    console.log(`[IP] Autocorreção DESATIVADA por flag — divergência registrada`);
                }
            } else {
                snapshot.ip_servidor = ipConfigurado;
                if (!ipNaSubredeTablets && !subnetTablets) {
                    // log de IP OK omitido para reduzir verbosidade
                } else {
                    // log de IP OK omitido para reduzir verbosidade
                }
            }
        } else {
            snapshot.ip_divergente = false;
        }

        console.log(`[IP] divergente=${snapshot.ip_divergente} | configurado=${snapshot.ip_configurado} | reais=[${maquinaIPs.join(', ')}]`);
    } catch(e) { snapshot.ip_divergente = null; }

    try {
        // Logs categorizados — top erros por mensagem
        const minutosIntervalo = Math.ceil((INTERVALO_MS || 300000) / 60000);
        const [topMsgs] = await db.execute(`
            SELECT type, level,
                   LEFT(message, 150) AS msg_resumo,
                   COUNT(*) AS total,
                   MAX(created_at) AS ultima_vez
            FROM device_log
            WHERE level='error'
              AND created_at >= UTC_TIMESTAMP() - INTERVAL ${minutosIntervalo} MINUTE
            GROUP BY type, LEFT(message, 150)
            ORDER BY total DESC
            LIMIT 10
        `);
        snapshot.top_erros = topMsgs;
    } catch(e) {}

    try {
        // Volume de erros por hora (últimas 6h)
        const [volHora] = await db.execute(`
            SELECT DATE_FORMAT(created_at,'%H:00') AS hora,
                   SUM(CASE WHEN level='error' THEN 1 ELSE 0 END) AS erros
            FROM device_log
            WHERE created_at >= UTC_TIMESTAMP() - INTERVAL 6 HOUR
            GROUP BY hora ORDER BY hora ASC
        `);
        snapshot.erros_por_hora = volHora;
    } catch(e) {}

    try {
        // Status do servidor Goomer local
        // O Goomer retorna 401 quando recebe requisição sem token — isso confirma que está rodando
        const r = await axios.get(`http://localhost:${snapshot.porta_local}/configurations`, { timeout:2000 });
        snapshot.servidor_goomer = 'online';
    } catch(e) {
        const status = e.response?.status;
        const body   = JSON.stringify(e.response?.data || '');
        // 401/403 com mensagem de credenciais = Goomer está rodando
        if ((status === 401 || status === 403) && (
            body.includes('token') || body.includes('credentials') ||
            body.includes('credenciais') || body.includes('Authorization')
        )) {
            snapshot.servidor_goomer = 'online';
        } else {
            snapshot.servidor_goomer = 'offline';
        }
    }

    await db.end();

    // Capturar latência acumulada (só se flag ativa)
    snapshot.latencia = _config.coleta_latencia ? getLatenciaSnapshot() : null;

    // ── Autocorreção: Spooler de impressão ────────────────────────────────────
    if (_config.autocorrecao_spooler && (snapshot.erros_impressora || 0) > 20) {
        try {
            const { execSync: esSP } = require('child_process');

            // Verificar se o serviço Spooler está travado
            const spoolerStatus = esSP('sc query Spooler', { timeout: 3000, encoding: 'utf8' });
            const spoolerParado = spoolerStatus.includes('STOPPED') || spoolerStatus.includes('PARADO');
            const spoolerRodando = spoolerStatus.includes('RUNNING') || spoolerStatus.includes('EM_EXECUCAO');

            // Contar erros de impressão recentes (últimos 2 ciclos)
            const errosImpressaoAltos = (snapshot.erros_impressora || 0) > 20;

            // Agir se: spooler parado OU erros altos com spooler rodando (fila travada)
            if (spoolerParado || (errosImpressaoAltos && spoolerRodando)) {
                console.log(`[SPOOLER] Iniciando correção — erros=${snapshot.erros_impressora}, parado=${spoolerParado}`);

                // Parar spooler via PowerShell com elevação automática
                // net stop/start requer admin — usar PowerShell com runas ou sc via admin
                const spoolPath = 'C:\\Windows\\System32\\spool\\PRINTERS';
                const fs4 = require('fs');

                // Script PowerShell que para, limpa fila e reinicia — roda elevado via scheduled task
                const psScript = `
                    Stop-Service -Name Spooler -Force -ErrorAction SilentlyContinue
                    Start-Sleep -Seconds 2
                    Get-ChildItem -Path '${spoolPath}' -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
                    Start-Service -Name Spooler -ErrorAction SilentlyContinue
                `.trim().replace(/\n/g, '; ');

                // Método 1: via Scheduled Task (roda como SYSTEM, sempre tem permissão)
                let sucesso = false;
                try {
                    // Registrar e executar task como SYSTEM
                    esSP(
                        `schtasks /Create /TN "GoomerSpoolerFix" /TR "powershell -NoProfile -Command \"${psScript}\"" /SC ONCE /ST 00:00 /RU SYSTEM /F`,
                        { timeout: 5000, encoding: 'utf8', shell: true }
                    );
                    esSP('schtasks /Run /TN "GoomerSpoolerFix"', { timeout: 5000, encoding: 'utf8' });
                    await new Promise(r => setTimeout(r, 6000));
                    esSP('schtasks /Delete /TN "GoomerSpoolerFix" /F', { timeout: 3000, encoding: 'utf8' }).catch?.(() => {});
                    console.log(`[SPOOLER] Correção executada via Scheduled Task (SYSTEM)`);
                } catch(eTask) {
                    console.log(`[SPOOLER] Task falhou: ${eTask.message} — tentando PowerShell direto`);

                    // Método 2: PowerShell direto (funciona se agente rodar como admin)
                    try {
                        esSP(
                            `powershell -NoProfile -Command "Stop-Service Spooler -Force; Start-Sleep 2; Start-Service Spooler"`,
                            { timeout: 15000, encoding: 'utf8' }
                        );
                    } catch(ePs) {
                        console.log(`[SPOOLER] PowerShell direto também falhou — agente precisa rodar como admin`);
                    }
                }

                await new Promise(r => setTimeout(r, 2000));

                // Verificar resultado
                const statusPos = esSP('sc query Spooler', { timeout: 3000, encoding: 'utf8' });
                sucesso = statusPos.includes('RUNNING') || statusPos.includes('EM_EXECUCAO') || statusPos.includes('4  RUNNING');

                console.log(`[SPOOLER] Resultado: ${sucesso ? 'ativo' : 'falhou'}`);

                if (!snapshot.autocorrecoes) snapshot.autocorrecoes = [];
                snapshot.autocorrecoes.push({
                    tipo:           'spooler_reiniciado',
                    horario:        new Date().toISOString(),
                    motivo:         `${snapshot.erros_impressora} erros de impressão${spoolerParado?' + serviço parado':''}`,
                    erros_antes:    snapshot.erros_impressora,
                    resultado:      sucesso ? 'sucesso' : 'falhou'
                });
            }
        } catch(eSpooler) {
            console.log(`[SPOOLER] Erro na autocorreção: ${eSpooler.message}`);
        }
    }

    // ── Análise de diagnóstico inteligente ──────────────────────────────────
    // Explica POR QUE o score está no nível atual e aponta causas reais
    const correlacoes = [];

    try {
        const cpu      = parseInt(snapshot.maquina?.cpu || 0);
        const ram      = parseInt(snapshot.maquina?.ram_pct || 0);
        const discos   = snapshot.maquina?.discos || [];
        const erros    = parseInt(snapshot.erros_log || 0);
        const retries  = parseInt(snapshot.retry_expirados || 0);
        const pedidos  = parseInt(snapshot.pedidos_hoje || 0);
        const latAPI   = snapshot.latencia?.api?.med || null;
        const latGW    = snapshot.latencia?.gateway?.med || null;
        const latSrv   = snapshot.latencia?.servidor?.med || null;
        const perdaAPI = snapshot.latencia?.api?.perda_pct || 0;
        const perdaGW  = snapshot.latencia?.gateway?.perda_pct || 0;
        const perdaNet = snapshot.latencia?.internet?.perda_pct || 0;
        const srvOk    = snapshot.servidor_goomer === 'online';
        const errosImp = parseInt(snapshot.erros_impressora || 0);
        const topErros = snapshot.top_erros || [];

        // ── Por que o score está baixo? ──────────────────────────────────────
        // Bloco 1: Servidor offline (impacto -40pts)
        if (!srvOk) {
            correlacoes.push({
                nivel: 'critico',
                categoria: 'score',
                titulo: 'Servidor Goomer offline — maior impacto no score (-40pts)',
                detalhe: 'O servidor Goomer está offline. Este é o componente de maior peso no score (40 pontos). Todos os outros serviços dependem dele.',
                acao: 'Verifique se o processo do servidor está rodando. Use a aba Sistema para verificar o serviço.'
            });
        }

        // Bloco 2: Pedidos sem ERP (impacto -25pts)
        if (retries > 0) {
            const minutosCiclo  = Math.ceil((INTERVALO_MS || 300000) / 60000);
            const taxaHoraRetry = Math.round((retries / minutosCiclo) * 60);
            const ptsPerdidos   = taxaHoraRetry <= 3 ? 3 : taxaHoraRetry <= 10 ? 11 : 25;
            const nivelRetry    = taxaHoraRetry <= 3 ? 'aviso' : 'critico';
            correlacoes.push({
                nivel: nivelRetry,
                categoria: 'score',
                titulo: `${retries} pedido(s) não chegaram ao ERP (~${taxaHoraRetry}/hora) — impacto no score (-${ptsPerdidos}pts)`,
                detalhe: taxaHoraRetry <= 3
                    ? `${retries} pedido(s) com retry expirado. Taxa baixa (~${taxaHoraRetry}/hora) — desconto leve de ${ptsPerdidos}pts.`
                    : `${retries} pedido(s) com retry expirado. Taxa de ~${taxaHoraRetry}/hora — desconto de ${ptsPerdidos}pts no score.`,
                acao: 'Acesse Integração ERP no Goomer Tools para reprocessar os pedidos.'
            });
        }

        // Bloco 2b: Acúmulo de pedidos antigos — risco de Deadlock MySQL
        const acumulo = parseInt(snapshot.acumulo_sale_order || 0);
        if (acumulo > 5000) {
            correlacoes.push({
                nivel: 'critico',
                categoria: 'erp',
                titulo: `${acumulo} pedidos acumulados no banco — risco de Deadlock MySQL`,
                detalhe: `Há ${acumulo} pedidos com sent_to_erp=1 e sem deletedAt há mais de 3 dias. Este acúmulo causa Deadlock nas transações do servidor Goomer, travando o sync com o ERP e derrubando a conexão com a API Abrahão.`,
                acao: 'Ative a Limpeza preventiva de Deadlock na aba Agente do NOC para corrigir automaticamente.'
            });
        } else if (acumulo > 1000) {
            correlacoes.push({
                nivel: 'aviso',
                categoria: 'erp',
                titulo: `${acumulo} pedidos acumulados — crescimento que pode causar Deadlock`,
                detalhe: `Volume ainda controlado, mas crescendo. Recomendável ativar a limpeza preventiva antes de atingir o patamar crítico.`,
                acao: 'Ative a Limpeza preventiva de Deadlock na aba Agente do NOC.'
            });
        }

        // Deadlock detectado nos logs
        const temDeadlock = topErros.some(e => (e.msg_resumo || '').toLowerCase().includes('deadlock'));
        if (temDeadlock) {
            const erroDeadlock = topErros.find(e => (e.msg_resumo||'').toLowerCase().includes('deadlock'));
            correlacoes.push({
                nivel: 'critico',
                categoria: 'erp',
                titulo: 'Deadlock ativo no MySQL — transações travando',
                detalhe: `Detectado erro de Deadlock nos logs recentes (${erroDeadlock?.total||1}x). Causado por acúmulo de registros em sale_order disputando locks simultaneamente.`,
                acao: acumulo > 0
                    ? `${acumulo} pedidos acumulados. Ative a Limpeza preventiva de Deadlock na aba Agente para resolver.`
                    : 'Verifique transações longas no MySQL e considere ativar a limpeza preventiva.'
            });
        }

        // Bloco 3: Erros de log (impacto variável no score)
        if (erros >= 500) {
            correlacoes.push({
                nivel: 'critico',
                categoria: 'score',
                titulo: `Volume crítico de erros (${erros}) — score perde 20pts`,
                detalhe: `${erros} erros nas últimas 2h. Com mais de 500 erros o bloco de logs zera (0/20 pontos).`,
                acao: 'Verifique a aba Logs de Erro para identificar o tipo predominante.'
            });
        } else if (erros >= 200) {
            correlacoes.push({
                nivel: 'aviso',
                categoria: 'score',
                titulo: `Alto volume de erros (${erros}) — score perde 15pts neste bloco`,
                detalhe: `${erros} erros nas últimas 2h reduzem o bloco de logs para 5/20 pontos.`,
                acao: 'Verifique a aba Logs de Erro para identificar o tipo predominante.'
            });
        } else if (erros >= 50) {
            correlacoes.push({
                nivel: 'aviso',
                categoria: 'score',
                titulo: `Erros de log moderados (${erros}) — score perde 10pts neste bloco`,
                detalhe: `${erros} erros reduzem o bloco de logs para 10/20 pontos. Abaixo de 50 seria 16/20.`,
                acao: 'Verifique os tipos de erro na aba Logs de Erro.'
            });
        }

        // ── Diagnósticos de causa raiz ───────────────────────────────────────

        // Erros de impressão — causa mais comum de volume alto de erros
        const errosImpressao = topErros.filter(e =>
            e.type === 'print' || (e.msg_resumo||'').toLowerCase().includes('print') ||
            (e.msg_resumo||'').toLowerCase().includes('impressao') ||
            (e.msg_resumo||'').toLowerCase().includes('printer')
        );
        if (errosImpressao.length > 0) {
            const totalImpErros = errosImpressao.reduce((s,e)=>s+parseInt(e.total||1),0);
            const pctErros = erros > 0 ? Math.round((totalImpErros/erros)*100) : 0;

            // Verificar se a impressora está inacessível na rede
            const impressorasProblema = (snapshot.impressoras||[]).filter(i => {
                const ipMatch = (i.uri||'').match(/(\d+\.\d+\.\d+\.\d+)/);
                return ipMatch; // impressoras de rede (pode estar offline)
            });

            correlacoes.push({
                nivel: totalImpErros > 50 ? 'critico' : 'aviso',
                categoria: 'impressao',
                titulo: `Erros de impressão representam ${pctErros}% dos erros (${totalImpErros}x)`,
                detalhe: `${errosImpressao.map(e=>`"${(e.msg_resumo||'').slice(0,80)}" (${e.total}x)`).join(' | ')}. ${impressorasProblema.length > 0 ? `Há ${impressorasProblema.length} impressora(s) de rede cadastrada(s) — verifique se estão acessíveis.` : ''}`,
                acao: 'Verifique se a impressora está ligada e acessível na rede. Use a aba Impressoras para testar conectividade. Reiniciar o spooler pode resolver.'
            });
        }

        // ERP — erros de integração
        const errosERP = topErros.filter(e =>
            e.type === 'erp' || (e.msg_resumo||'').toLowerCase().includes('erp') ||
            (e.msg_resumo||'').toLowerCase().includes('totvs') ||
            (e.msg_resumo||'').toLowerCase().includes('linx')
        );
        if (errosERP.length > 0 || retries > 0) {
            const latCausa = latAPI && latAPI > 300 ? `Latência da API em ${latAPI}ms pode ser contribuinte.` :
                             perdaAPI > 10 ? `${perdaAPI}% de perda de pacotes para API detectado.` : '';
            const ramCausa = ram > 85 ? `RAM em ${ram}% pode estar atrasando o processamento.` : '';
            const causas = [latCausa, ramCausa].filter(Boolean).join(' ');

            if (errosERP.length > 0 || retries > 0) {
                correlacoes.push({
                    nivel: retries > 0 ? 'critico' : 'aviso',
                    categoria: 'erp',
                    titulo: `Falhas na integração ERP${retries>0?' com pedidos perdidos':''}`,
                    detalhe: `${retries > 0 ? `${retries} pedido(s) não chegaram ao ERP. ` : ''}${errosERP.map(e=>`"${(e.msg_resumo||'').slice(0,80)}" (${e.total}x)`).join(' | ')}${causas?' — '+causas:''}`,
                    acao: retries > 0
                        ? 'Reprocesse os pedidos em Integração ERP. Verifique se o sistema de gestão está respondendo.'
                        : 'Monitore se os erros evoluem para pedidos perdidos. Verifique conectividade com o servidor ERP.'
                });
            }
        }

        // Rede local instável
        if (perdaGW > 15 || (latGW && latGW > 50)) {
            const tabletsSemResp = (snapshot.tablets||[]).filter(t=>t.ping?.status==='sem resposta').length;
            correlacoes.push({
                nivel: perdaGW > 30 ? 'critico' : 'aviso',
                categoria: 'rede',
                titulo: `Rede local instável${tabletsSemResp>0?` — ${tabletsSemResp} tablet(s) sem resposta`:''}`,
                detalhe: `Gateway: ${latGW||'?'}ms, ${perdaGW}% perda de pacotes.${tabletsSemResp>0?` ${tabletsSemResp} tablet(s) não respondem ao ping — podem estar com dificuldade de comunicação com o servidor.`:''}`,
                acao: 'Verifique o roteador e cabos de rede. Se os tablets não respondem, o problema pode ser de Wi-Fi ou switch.'
            });
        }

        // Internet instável com impacto nos serviços
        if (perdaNet > 10 || (latAPI && latAPI > 250)) {
            correlacoes.push({
                nivel: perdaNet > 20 ? 'critico' : 'aviso',
                categoria: 'internet',
                titulo: `Qualidade de internet comprometida`,
                detalhe: `Internet: ${snapshot.latencia?.internet?.med||'?'}ms, ${perdaNet}% perda. API Goomer: ${latAPI||'?'}ms. Comunicação com os servidores Goomer pode estar lenta ou instável.`,
                acao: 'Verifique com o provedor de internet. Reiniciar o roteador pode ajudar. Se persistir, pode ser instabilidade nos servidores Goomer.'
            });
        }

        // Máquina sob pressão
        const discoCheio = discos.find(d => parseInt(d.pct) > 88);
        if (discoCheio) {
            correlacoes.push({
                nivel: 'critico',
                categoria: 'maquina',
                titulo: `Disco ${discoCheio.drive} quase cheio (${discoCheio.pct}%)`,
                detalhe: `Com ${discoCheio.pct}% do disco ${discoCheio.drive} ocupado, o sistema pode falhar ao gravar logs, arquivos temporários e dados de transações.`,
                acao: `Libere espaço no disco ${discoCheio.drive}. Verifique arquivos de log antigos em C:\ProgramData e pastas temporárias.`
            });
        }
        if (ram > 88) {
            correlacoes.push({
                nivel: ram > 94 ? 'critico' : 'aviso',
                categoria: 'maquina',
                titulo: `Memória RAM crítica (${ram}%)`,
                detalhe: `Com apenas ${100-ram}% de RAM livre, processos podem travar ou falhar. ${erros > 50 ? `Os ${erros} erros registrados podem estar relacionados.` : ''}`,
                acao: 'Reinicie o servidor Goomer para liberar memória. Verifique processos em Saúde da Máquina.'
            });
        }
        if (cpu > 88) {
            correlacoes.push({
                nivel: 'aviso',
                categoria: 'maquina',
                titulo: `CPU sobrecarregada (${cpu}%)`,
                detalhe: `CPU acima de 88% pode causar lentidão geral no sistema, incluindo resposta ao ERP e impressão.`,
                acao: 'Verifique processos consumindo CPU em Saúde da Máquina.'
            });
        }

        // IP divergente — causa de tablets sem comunicação
        if (snapshot.ip_divergente) {
            correlacoes.push({
                nivel: 'critico',
                categoria: 'rede',
                titulo: 'IP divergente — tablets provavelmente sem comunicação com o servidor',
                detalhe: `O servidor está em ${snapshot.ip_servidor} mas está configurado para ${snapshot.ip_configurado}. Os tablets procuram o servidor no IP configurado e não conseguem se conectar.`,
                acao: 'Verifique as configurações no servidor local Goomer e atualize o IP.'
            });
        }

        // Conflito de IP na rede
        if ((snapshot.conflito_ip_rede||[]).length > 0) {
            const conflitos = snapshot.conflito_ip_rede.map(c=>`${c.tipo}`).join(', ');
            correlacoes.push({
                nivel: 'critico',
                categoria: 'rede',
                titulo: `Conflito de IP — servidor com mesmo endereço de ${conflitos}`,
                detalhe: `O IP ${snapshot.ip_servidor} está sendo usado por outro dispositivo na rede (${conflitos}). Isso causa falhas intermitentes de comunicação para todos os dispositivos.`,
                acao: 'Atribua IPs fixos e únicos a cada dispositivo. Verifique as configurações de rede do servidor e dos dispositivos conflitantes.'
            });
        }

        // Tudo saudável
        if (correlacoes.length === 0) {
            correlacoes.push({
                nivel: 'info',
                categoria: 'geral',
                titulo: 'Sistema operando normalmente',
                detalhe: `Servidor online, ERP sem pendências, erros dentro do normal (${erros}), rede estável. ${pedidos > 0 ? `${pedidos} pedido(s) processado(s) hoje.` : 'Sem pedidos registrados no período — pode ser fora do horário de funcionamento.'}`,
                acao: null
            });
        }

    } catch(e) { console.log('diagnostico erro:', e.message); }

    // ── Correlações de conflitos (porta, IP, agente duplicado) ───────────────
    try {
        // Conflito de porta
        (snapshot.conflitos_porta||[]).forEach(c => {
            correlacoes.push({
                nivel:     'critico',
                categoria: 'rede',
                titulo:    `Conflito de porta ${c.porta}: processo "${c.processo}"`,
                acao:      `Outro software (${c.processo}) está ocupando a porta ${c.porta} do servidor Goomer. Encerrar o processo ou reconfigurar a porta. Exemplo: AnotaAí usa porta 5000 por padrão.`
            });
        });

        // Conflito de IP duplicado
        (snapshot.conflitos_ip||[]).filter(c => c.tipo === 'ip_duplicado').forEach(c => {
            correlacoes.push({
                nivel:     'critico',
                categoria: 'rede',
                titulo:    `IP duplicado: ${c.ip} em uso por tablet/impressora e servidor`,
                acao:      'Dois dispositivos com o mesmo IP causam falha intermitente de comunicação. Verificar DHCP do roteador e renovar IP dos dispositivos afetados.'
            });
        });

        // Conflito ARP
        (snapshot.conflitos_ip||[]).filter(c => c.tipo === 'conflito_arp').forEach(c => {
            correlacoes.push({
                nivel:     'critico',
                categoria: 'rede',
                titulo:    `Conflito ARP no IP ${c.ip} — ${(c.macs||[]).length} dispositivos`,
                acao:      'Dois dispositivos respondem pelo mesmo IP na rede. Verificar tabela DHCP do roteador e desconectar o dispositivo intruso.'
            });
        });

        // Agente duplicado
        if (snapshot.agente_duplicado?.detectado) {
            correlacoes.push({
                nivel:     'aviso',
                categoria: 'configuracao',
                titulo:    `Agente duplicado em: ${(snapshot.agente_duplicado.hosts_outros||[]).join(', ')}`,
                acao:      'Apenas um servidor Goomer por token é permitido. Remover o agente da máquina incorreta para evitar oscilação dos dados no NOC.'
            });
        }
    } catch(eCorrExtra) {}

    snapshot.correlacoes = correlacoes;
    if (correlacoes.filter(c => c.nivel === 'critico').length > 0) {
        snapshot.alertas_criticos = (snapshot.alertas_criticos || 0) + 1;
    }

    // ── Calcular score de saúde sistêmico ────────────────────────────────────
    // Princípio: medir só o que o suporte pode controlar.
    // Tablets físicos (bateria, ping) não entram — dependem do comportamento da loja.
    let pontos = 0, total = 0;

    // ── Servidor Goomer (40pts) ───────────────────────────────────────────────
    // É o coração do sistema — offline é o pior cenário possível
    total += 40;
    if (snapshot.servidor_goomer === 'online') pontos += 40;

    // ── IP divergente (penalidade direta) ───────────────────────────────────
    // Se o IP configurado no Goomer não bate com o IP real,
    // os tablets não conseguem se comunicar — é um problema crítico
    if (snapshot.ip_divergente === true) {
        total += 20;
        pontos += 0; // zero pontos neste bloco — penalidade de -20pts no score total
        console.log(`⚠ IP divergente penalizando score: total=${total}, pontos=${pontos}`);
    }

    // ── Integração ERP (25pts) ────────────────────────────────────────────────
    // Pedidos não chegando ao sistema de gestão = impacto direto no negócio
    // Score proporcional: até 3 retries/hora = sem desconto; cresce conforme volume
    total += 25;
    if (snapshot.retry_expirados === 0 && snapshot.erros_erp === 0) {
        pontos += 25; // sem pendências — máximo
    } else if (snapshot.retry_expirados === 0) {
        pontos += 18; // erros ERP mas sem retry expirado = parcial
    } else {
        // Taxa de retry por hora com base no intervalo do agente
        const minutos = Math.ceil((INTERVALO_MS || 300000) / 60000);
        const taxaRetryHora = Math.round((snapshot.retry_expirados / minutos) * 60);
        if (taxaRetryHora <= 3) {
            pontos += 22; // até 3/hora — desconto leve
        } else if (taxaRetryHora <= 10) {
            pontos += 14; // 4 a 10/hora — desconto moderado
        } else {
            pontos += 0;  // acima de 10/hora — crítico
        }
    }

    // ── Volume de erros sistêmicos (20pts) ────────────────────────────────────
    // Erros de log indicam problemas de software/integração
    total += 20;
    if      (snapshot.erros_log === 0)        pontos += 20;
    else if (snapshot.erros_log < 50)         pontos += 16;
    else if (snapshot.erros_log < 200)        pontos += 10;
    else if (snapshot.erros_log < 500)        pontos +=  5;
    // acima de 500 = 0pts

    // ── Saúde da máquina servidora (15pts) ────────────────────────────────────
    // CPU/RAM/disco do servidor impactam todos os serviços
    total += 15;
    if (snapshot.maquina) {
        const cpu   = parseInt(snapshot.maquina.cpu || 0);
        const ram   = parseInt(snapshot.maquina.ram_pct || 0);
        const discos = snapshot.maquina.discos || [];
        const discoCrit  = discos.some(d => parseInt(d.pct) > 90);
        const discoAlerta = discos.some(d => parseInt(d.pct) > 75);

        if      (cpu < 70 && ram < 80 && !discoAlerta) pontos += 15; // RAM até 80% = ok
        else if (cpu < 85 && ram < 90 && !discoCrit)   pontos += 10; // RAM até 90% = parcial
        else if (cpu < 95 && ram < 95 && !discoCrit)   pontos +=  5;
        // recursos críticos = 0pts
    } else {
        pontos += 10; // sem dados de máquina = parcial (não penalizar agente antigo)
    }

    // ── Situação financeira não entra no score ────────────────────────────────
    // Bloqueio financeiro força status "Crítico" diretamente (ver abaixo)
    // mas não altera o score numérico pois é questão administrativa, não técnica

    pontos = Math.max(0, pontos);
    const pct = total > 0 ? Math.round((pontos/total)*100) : 0;

    // Status base pelo score
    let status = pct>=85?'Saudável':pct>=65?'Estável':pct>=45?'Atenção':pct>=25?'Crítico':'Grave';

    // Forçar status mínimo por condições críticas independente do score
    if (snapshot.servidor_goomer === 'offline') {
        // Servidor Goomer offline = no mínimo Crítico
        if (status === 'Saudável' || status === 'Estável' || status === 'Atenção') {
            status = 'Crítico';
        }
    }
    if (snapshot.inadimplente) {
        // Bloqueio financeiro = no mínimo Crítico
        if (status === 'Saudável' || status === 'Estável') {
            status = 'Crítico';
        }
    }
    if (snapshot.ip_divergente === true) {
        // IP divergente = tablets sem comunicação = no mínimo Crítico
        if (status === 'Saudável' || status === 'Estável' || status === 'Atenção') {
            status = 'Crítico';
        }
    }
    if (snapshot.retry_expirados > 0) {
        // Pedidos sem ERP = no mínimo Atenção
        if (status === 'Saudável' || status === 'Estável') {
            status = 'Atenção';
        }
    }

    snapshot.score  = pct;
    snapshot.nota   = status; // mantido para compatibilidade
    snapshot.status = status;

    // Alertas resumidos — apenas condições sistêmicas principais
    const alertas_criticos = [
        snapshot.inadimplente,
        snapshot.servidor_goomer === 'offline',
        snapshot.retry_expirados > 0,
        snapshot.ip_divergente === true,
        (snapshot.conflito_ip_rede||[]).length > 0
        // tablets removidos — status físico não é crítico sistêmico
    ].filter(Boolean).length;

    const alertas_avisos = [
        snapshot.erros_log > 100,
        snapshot.erros_impressora > 0
    ].filter(Boolean).length;

    snapshot.alertas_criticos = alertas_criticos;
    snapshot.alertas_avisos   = alertas_avisos;

    // Alertas de máquina (para exibição — não afetam score)
    if (snapshot.maquina) {
        const cpu  = parseInt(snapshot.maquina.cpu||0);
        const ram  = parseInt(snapshot.maquina.ram_pct||0);
        const discoCrit = (snapshot.maquina.discos||[]).some(d=>parseInt(d.pct)>90);
        if (discoCrit)           snapshot.alertas_criticos++;
        if (cpu > 90 || ram > 90) snapshot.alertas_avisos++;
    }

    // ── Merge final de autocorreções pendentes (ex: servidor iniciado) ──────────
    if (!snapshot.autocorrecoes) snapshot.autocorrecoes = [];
    if (_autocorrecoesPendentes.length > 0) {
        snapshot.autocorrecoes.push(..._autocorrecoesPendentes);
        _autocorrecoesPendentes = [];
    }

    return snapshot;
}

// ── Enviar para Supabase ──────────────────────────────────────────────────────
async function enviarSupabase(snap) {
    // Payload leve — KPIs essenciais sempre enviados
    // Dados detalhados (logs, máquina, top erros) só a cada 30min para economizar espaço
    const agora = new Date();
    const minuto = agora.getMinutes();
    const isSnapCompleto = (minuto % 30 < 5); // janela de 5min a cada 30min

    // Payload compacto do snapshot (sem dados pesados)
    const snapLeve = {
        token_loja:      snap.token_loja,
        nome_loja:       snap.nome_loja,
        score:           snap.score,
        nota:            snap.nota,
        status:          snap.status,
        servidor_goomer: snap.servidor_goomer,
        ip_servidor:     snap.ip_servidor,
        ip_configurado:     snap.ip_configurado,
        ip_divergente:      snap.ip_divergente,
        conflito_ip_rede:   snap.conflito_ip_rede,
        autocorrecoes:      snap.autocorrecoes,
        latencia:        snap.latencia,
        correlacoes:     snap.correlacoes,
        goomer_version:  snap.goomer_version,
        diagnostico_rede: _testeRedeAtivo || Object.keys(_testeRedeResultados).length > 0 ? consolidarTesteRede() : null,
        agente_version:  AGENTE_VERSION,
        anydesk_id:      snap.anydesk_id,
        teamviewer_id:   snap.teamviewer_id,
        hostname:        snap.hostname,
        mac_address:     snap.mac_address,
        conflitos_porta: snap.conflitos_porta,
        conflitos_ip:    snap.conflitos_ip,
        agente_duplicado: snap.agente_duplicado,
        db_port:         snap.db_port,
        db_versao:       snap.db_versao,
        db_instalado:    snap.db_instalado,
        inadimplente:    snap.inadimplente,
        pedidos_hoje:    snap.pedidos_hoje,
        retry_expirados:       snap.retry_expirados,
        acumulo_sale_order:   snap.acumulo_sale_order || 0,
        erros_erp:       snap.erros_erp,
        erros_log:       snap.erros_log,
        erros_impressora:snap.erros_impressora,
        tablets_total:   snap.tablets_total,
        tablets_ativos:  snap.tablets_ativos,
        tablets_criticos:snap.tablets_criticos,
        alertas_criticos:snap.alertas_criticos,
        alertas_avisos:  snap.alertas_avisos,
        coletado_em:     snap.coletado_em,
        // Tablets com dados básicos (sem histórico detalhado)
        tablets: (snap.tablets||[]).map(t => ({
            mesa:    t.mesa,
            modelo:  t.modelo,
            ip:      t.ip,
            bateria: t.bateria,
            sinal:   t.sinal,
            versao:  t.versao,
            ping:    t.ping,
            fonte_bateria: t.fonte_bateria
        })),
        impressoras:      snap.impressoras,
        hostname:         snap.maquina?.host || require('os').hostname(),
        conflitos_porta:  snap.conflitos_porta  || null,
        conflitos_ip:     snap.conflitos_ip     || null,
        agente_duplicado: snap.agente_duplicado || null,
        causa_erros_impressao: snap.causa_erros_impressao || null,
    };

    // Dados de máquina sempre incluídos (tamanho pequeno, muito útil no NOC)
    snapLeve.maquina = snap.maquina;

    // top_erros sempre incluído — são só 10 registros, impacto mínimo no tamanho
    snapLeve.top_erros = snap.top_erros;

    // Volume por hora só a cada 30min (mais pesado)
    if (isSnapCompleto) {
        snapLeve.erros_por_hora = snap.erros_por_hora;
        console.log(`[snapshot completo com volume por hora]`);
    }

    const payload = {
        token_loja:       snap.token_loja,
        nome_loja:        snap.nome_loja,
        score:            snap.score,
        nota:             snap.nota,
        servidor_online:  snap.servidor_goomer === 'online',
        tablets_ativos:   snap.tablets_ativos || 0,
        tablets_total:    snap.tablets_total  || 0,
        erros_log:        snap.erros_log      || 0,
        retry_expirados:  snap.retry_expirados|| 0,
        alertas_criticos: snap.alertas_criticos|| 0,
        alertas_avisos:   snap.alertas_avisos  || 0,
        ip_servidor:      snap.ip_servidor    || null,
        ip_configurado:   snap.ip_configurado || null,
        ip_divergente:    snap.ip_divergente  || false,
        payload:          snapLeve
    };

    const resp = await axios.post(
        `${SUPABASE_URL}/rest/v1/snapshots`,
        payload,
        {
            headers: {
                'apikey':        SUPABASE_KEY,
                'Authorization': `Bearer ${SUPABASE_KEY}`,
                'Content-Type':  'application/json',
                'Prefer':        'return=minimal'
            },
            timeout: 10000
        }
    );
    return resp.status;
}

// ── Configuração do agente (feature flags) ───────────────────────────────────
// Carregada do Supabase a cada ciclo — permite controle remoto sem reinstalar
let _config = {
    autocorrecao_ip:       true,
    autocorrecao_spooler:  true,
    autocorrecao_mysql:    true,
    autocorrecao_deadlock: true,   // Limpeza automática de pedidos antigos já enviados ao ERP
                                   // Previne deadlocks no MySQL causados pelo acúmulo de
                                   // registros em sale_order sem deletedAt. Roda 1x/dia em
                                   // horário de baixo movimento, em lotes de 500 registros.
    coleta_logs:           true,
    coleta_latencia:       true,
    coleta_maquina:        true,
    coleta_tablets:        true,
    intervalo_minutos:     5,
    auto_atualizacao:      true,  // controla se o agente verifica/aplica atualizações
    manter_servidor_ativo: true,  // Verifica a cada ciclo se o servidor Goomer está rodando.
                                  // Se estiver offline, tenta iniciá-lo automaticamente.
                                  // Não interfere em configurações — apenas abre o processo.
    teste_rede_ativo:      false, // Modo Teste de Rede — bateria de 40 pings por tablet
    teste_rede_inicio:     '00:00', // horário de início (HH:MM)
    teste_rede_fim:        '00:00', // horário de fim (HH:MM)
};

// ── Controle de auto-atualização ──────────────────────────────────────────────
let _ciclosDesdeUltimaVerificacao = 0;
let _portaLocalCache = 4999; // porta do servidor, atualizada a cada ciclo
let _autocorrecoesPendentes = []; // autocorreções registradas antes do snapshot

// ── Controle de limpeza de deadlock (1x/dia) ───────────────────────────────────
let _ultimaLimpezaDeadlock = null; // data da última execução

// ── Controle do Modo Teste de Rede ────────────────────────────────────────────
let _testeRedeResultados = {}; // acumula por tablet: { [ip]: { ciclos, pings, wifi, bat } }
let _testeRedeAtivo      = false; // flag de estado interno (dentro da janela de horário)
let _testeRedeInicio     = null;  // timestamp de início da sessão atual
const CICLOS_ENTRE_VERIFICACOES = 12; // ~1h com ciclo de 5min
let _atualizacaoPendente = null; // { sha, conteudo }

// ── Manter servidor Goomer ativo ──────────────────────────────────────────────
// Verifica localmente se o servidor está respondendo e o inicia se estiver offline
async function verificarEIniciarServidor(portaLocal) {
    if (!_config.manter_servidor_ativo) return;
    try {
        // Teste rápido: tentar conectar na porta do servidor
        const porta = portaLocal || 4999;
        const respondeu = await new Promise(resolve => {
            const net = require('net');
            const sock = new net.Socket();
            sock.setTimeout(2000);
            sock.on('connect', () => { sock.destroy(); resolve(true); });
            sock.on('error',   () => resolve(false));
            sock.on('timeout', () => { sock.destroy(); resolve(false); });
            try { sock.connect(porta, '127.0.0.1'); } catch(e) { resolve(false); }
        });

        if (respondeu) return; // servidor online — nada a fazer

        console.log(`[SERVIDOR] Offline na porta ${porta} — tentando iniciar...`);

        const { execSync: esServ, spawn: spServ } = require('child_process');
        const fs9 = require('fs');

        // Localizar o executável
        let exePath = null;
        let usuarios = ['Goomer'];
        try {
            const u = execSync('powershell -NoProfile -Command "Get-ChildItem C:\\Users | Select-Object -ExpandProperty Name"', { timeout: 3000, encoding: 'utf8' });
            usuarios = u.trim().split('\n').map(x => x.trim()).filter(Boolean);
        } catch(e) {}

        const candidatos = [];
        for (const u of usuarios) {
            candidatos.push(
                `C:\\Users\\${u}\\AppData\\Local\\Programs\\abrahao-servidor\\Goomer - Servidor.exe`,
                `C:\\Users\\${u}\\AppData\\Local\\Programs\\abrahao-servidor\\abrahao-servidor.exe`
            );
        }
        for (const p of candidatos) {
            try { if (fs9.existsSync(p)) { exePath = p; break; } } catch(e) {}
        }

        if (!exePath) {
            console.log('[SERVIDOR] Executável não encontrado — não foi possível iniciar');
            return;
        }

        const dirExe  = exePath.substring(0, exePath.lastIndexOf('\\'));
        const nomeExe = exePath.split('\\').pop().replace('.exe', '');

        // Tentar iniciar: spawn → Scheduled Task → cmd start
        let iniciou = false;
        try {
            spServ(exePath, [], { detached: true, stdio: 'ignore', cwd: dirExe, shell: false }).unref();
            iniciou = true;
        } catch(e) {}

        if (!iniciou) {
            try {
                esServ(`schtasks /Create /TN "GoomerServidorInicio" /TR "${exePath}" /SC ONCE /ST 00:00 /RL HIGHEST /F`, { timeout: 5000, encoding: 'utf8', shell: true });
                esServ('schtasks /Run /TN "GoomerServidorInicio"', { timeout: 5000, encoding: 'utf8' });
                setTimeout(() => { try { esServ('schtasks /Delete /TN "GoomerServidorInicio" /F', { timeout: 3000, encoding: 'utf8' }); } catch(e) {} }, 10000);
                iniciou = true;
                console.log('[SERVIDOR] Iniciado via Scheduled Task');
            } catch(eTask) {}
        }

        if (!iniciou) {
            try { esServ(`cmd /c start "" "${exePath}"`, { timeout: 3000, encoding: 'utf8', shell: true }); iniciou = true; } catch(e) {}
        }

        if (iniciou) {
            console.log(`✅ [SERVIDOR] Processo iniciado — aguardando subir...`);
            // Aguardar até 30s para confirmar
            for (let t = 0; t < 10; t++) {
                await new Promise(r => setTimeout(r, 3000));
                const ok = await new Promise(resolve => {
                    const net = require('net');
                    const s = new net.Socket();
                    s.setTimeout(1500);
                    s.on('connect', () => { s.destroy(); resolve(true); });
                    s.on('error',   () => resolve(false));
                    s.on('timeout', () => { s.destroy(); resolve(false); });
                    try { s.connect(porta, '127.0.0.1'); } catch(e) { resolve(false); }
                });
                if (ok) {
                    console.log(`✅ [SERVIDOR] Online após ${(t+1)*3}s`);
                    _autocorrecoesPendentes.push({
                        tipo:     'servidor_iniciado',
                        horario:  new Date().toISOString(),
                        resultado:'sucesso',
                        obs:      `Servidor offline detectado — iniciado automaticamente após ${(t+1)*3}s`
                    });
                    break;
                }
            }
        } else {
            console.log('[SERVIDOR] ⚠ Não foi possível iniciar o processo');
        _autocorrecoesPendentes.push({
            tipo:     'servidor_iniciado',
            horario:  new Date().toISOString(),
            resultado:'falhou',
            obs:      'Servidor offline — não foi possível iniciar o processo'
        });
        }

    } catch(eServ) {
        console.log(`[SERVIDOR] Erro: ${eServ.message}`);
    }
}

// ── Modo Teste de Rede ────────────────────────────────────────────────────────
// Coleta bateria de 40 pings por tablet dentro da janela de horário configurada
// Acumula resultados em memória e consolida ao final da sessão

function dentroJanelaTeste() {
    if (!_config.teste_rede_ativo) return false;
    const agora   = new Date();
    const hAtual  = agora.getHours() * 60 + agora.getMinutes();
    const parseFn = h => {
        const [hh, mm] = (h||'00:00').split(':').map(Number);
        return hh * 60 + mm;
    };
    const hInicio = parseFn(_config.teste_rede_inicio);
    const hFim    = parseFn(_config.teste_rede_fim);
    if (hInicio === hFim) return false; // janela inválida
    if (hInicio < hFim) return hAtual >= hInicio && hAtual < hFim;
    // Janela que cruza meia-noite (ex: 22:00–02:00)
    return hAtual >= hInicio || hAtual < hFim;
}

async function executarTesteRede(tablets) {
    if (!tablets || tablets.length === 0) return;

    const { execSync: esNet } = require('child_process');
    const tabletsComIP = tablets.filter(t => t.ip && /\d+\.\d+\.\d+\.\d+/.test(t.ip));
    if (tabletsComIP.length === 0) return;

    const agora = new Date();
    const horaStr = agora.toTimeString().slice(0, 5); // "HH:MM"

    console.log(`[TESTE-REDE] Iniciando bateria de 40 pings em ${tabletsComIP.length} tablet(s) — ${horaStr}`);

    // Rodar pings em paralelo (Promise.all) com timeout curto por ping
    const resultados = await Promise.all(tabletsComIP.map(async t => {
        let ping_min = null, ping_med = null, ping_max = null, ping_perda_pct = 0;

        try {
            const saida = esNet(
                `ping -n 40 -w 500 ${t.ip}`,
                { timeout: 30000, encoding: 'utf8', shell: true }
            );

            // Extrair estatísticas do output do ping Windows
            // "Mínimo = Xms, Máximo = Xms, Média = Xms"
            const mMatch = saida.match(/M[íi]nimo\s*=\s*(\d+)ms/i);
            const xMatch = saida.match(/M[áa]ximo\s*=\s*(\d+)ms/i);
            const aMatch = saida.match(/M[eé]dia\s*=\s*(\d+)ms/i);
            if (mMatch) ping_min = parseInt(mMatch[1]);
            if (xMatch) ping_max = parseInt(xMatch[1]);
            if (aMatch) ping_med = parseInt(aMatch[1]);

            // Perda de pacotes: "Perdidos = X (Y%)"
            const pMatch = saida.match(/Perdidos\s*=\s*\d+\s*\((\d+)%/i);
            if (pMatch) ping_perda_pct = parseInt(pMatch[1]);

        } catch(ePing) {
            // Timeout ou host inacessível = 100% de perda
            ping_perda_pct = 100;
        }

        return {
            mesa:            t.mesa_numero || t.mesa || '?',
            ip:              t.ip,
            identity:        t.identity || t.ip,
            horario:         horaStr,
            ping_min,
            ping_med,
            ping_max,
            ping_perda_pct,
            wifi_level:      parseInt(t.sinal || t.wifi_level || 0),
            bateria:         parseInt(t.bateria || 0),
        };
    }));

    // Inicializar entrada no acumulador se novo
    resultados.forEach(r => {
        const chave = r.ip;
        if (!_testeRedeResultados[chave]) {
            _testeRedeResultados[chave] = {
                mesa:            r.mesa,
                ip:              r.ip,
                identity:        r.identity,
                ciclos:          0,
                soma_ping_med:   0,
                soma_ping_max:   0,
                soma_perda:      0,
                soma_wifi:       0,
                soma_bat:        0,
                ping_max_abs:    0,   // pior ping absoluto
                perda_max:       0,   // pior perda de ciclo
                wifi_min:        100, // pior sinal
                horario_inicio:  horaStr,
                horario_pior_ping:  null,
                horario_pior_perda: null,
                historico_ciclos:   [],
            };
        }

        const acc = _testeRedeResultados[chave];
        acc.ciclos++;
        if (r.ping_med !== null) acc.soma_ping_med += r.ping_med;
        if (r.ping_max !== null) {
            acc.soma_ping_max += r.ping_max;
            if (r.ping_max > acc.ping_max_abs) {
                acc.ping_max_abs = r.ping_max;
                acc.horario_pior_ping = r.horario;
            }
        }
        acc.soma_perda += r.ping_perda_pct;
        acc.soma_wifi  += r.wifi_level;
        acc.soma_bat   += r.bateria;
        if (r.ping_perda_pct > acc.perda_max) {
            acc.perda_max = r.ping_perda_pct;
            acc.horario_pior_perda = r.horario;
        }
        if (r.wifi_level > 0 && r.wifi_level < acc.wifi_min) {
            acc.wifi_min = r.wifi_level;
        }
        // Guardar snapshot do ciclo (máx 72 ciclos = 6h)
        if (acc.historico_ciclos.length < 72) {
            acc.historico_ciclos.push({
                h: r.horario,
                pm: r.ping_med,
                pp: r.ping_perda_pct,
                w: r.wifi_level
            });
        }
    });

    console.log(`[TESTE-REDE] Ciclo concluído: ${resultados.map(r=>`Mesa ${r.mesa}(${r.ping_med||'?'}ms/${r.ping_perda_pct}%)`).join(', ')}`);
}

function consolidarTesteRede() {
    const consolidado = Object.values(_testeRedeResultados).map(acc => {
        const c = acc.ciclos || 1;
        const pingMed  = Math.round(acc.soma_ping_med / c);
        const perdaMed = Math.round(acc.soma_perda / c);
        const wifiMed  = Math.round(acc.soma_wifi / c);
        const batMed   = Math.round(acc.soma_bat / c);

        // Classificação por cor
        let saude;
        if (perdaMed < 2 && pingMed < 20)       saude = 'excelente';
        else if (perdaMed < 10 && pingMed < 80) saude = 'instavel';
        else                                     saude = 'critico';

        return {
            mesa:              acc.mesa,
            ip:                acc.ip,
            identity:          acc.identity,
            ciclos:            c,
            ping_med:          pingMed,
            ping_max_abs:      acc.ping_max_abs,
            perda_med_pct:     perdaMed,
            perda_max_pct:     acc.perda_max,
            wifi_med:          wifiMed,
            wifi_min:          acc.wifi_min === 100 ? 0 : acc.wifi_min,
            bat_med:           batMed,
            horario_inicio:    acc.horario_inicio,
            horario_pior_ping: acc.horario_pior_ping,
            horario_pior_perda:acc.horario_pior_perda,
            saude,
            historico:         acc.historico_ciclos,
        };
    }).sort((a,b) => b.perda_med_pct - a.perda_med_pct); // piores primeiro

    return consolidado;
}

// ── Prevenção de deadlock MySQL ────────────────────────────────────────────────
// Faz soft delete em lotes dos pedidos já enviados ao ERP com mais de 3 dias
// Esses registros acumulam e causam deadlocks nas transações do servidor Goomer
async function limparDeadlockMySQL() {
    if (!_config.autocorrecao_deadlock) return;

    // Rodar só 1x por dia
    const hoje = new Date().toDateString();
    if (_ultimaLimpezaDeadlock === hoje) return;

    // Rodar só em horário de baixo movimento (1h-6h)
    const hora = new Date().getHours();
    if (hora < 1 || hora > 6) return;

    try {
        const db = await getDb();
        let totalLimpos = 0;
        let continuarLimpando = true;

        console.log('[DEADLOCK] Iniciando limpeza preventiva de pedidos antigos...');

        // Lotes de 500 para não segurar lock longo
        while (continuarLimpando) {
            const [result] = await db.execute(`
                UPDATE sale_order
                SET deletedAt = NOW(), updatedAt = NOW()
                WHERE sent_to_erp = 1
                  AND deletedAt IS NULL
                  AND createdAt < NOW() - INTERVAL 3 DAY
                LIMIT 500
            `);

            const afetados = result.affectedRows || 0;
            totalLimpos += afetados;

            if (afetados < 500) {
                continuarLimpando = false; // último lote
            } else {
                // Pausa entre lotes para não sobrecarregar o MySQL
                await new Promise(r => setTimeout(r, 2000));
            }
        }

        await db.end();
        _ultimaLimpezaDeadlock = hoje;

        if (totalLimpos > 0) {
            console.log(`✅ [DEADLOCK] Limpeza concluída: ${totalLimpos} pedido(s) antigos arquivados`);
        } else {
            console.log('[DEADLOCK] Nenhum pedido antigo para limpar');
        }

        return totalLimpos;

    } catch(eDL) {
        console.log(`[DEADLOCK] Erro na limpeza: ${eDL.message}`);
        return 0;
    }
}

// ── Auto-atualização do agente ────────────────────────────────────────────────
async function verificarAtualizacao() {
    if (!_config.auto_atualizacao) return;
    try {
        // Verificar SHA mais recente no GitHub via API
        const apiUrl = `https://api.github.com/repos/${GITHUB_RAW_USER}/${GITHUB_RAW_REPO}/contents/${GITHUB_RAW_FILE}?ref=${GITHUB_RAW_BRANCH}`;
        const resp = await axios.get(apiUrl, {
            headers: { 'Accept': 'application/vnd.github.v3+json', 'User-Agent': 'GoomerAgente' },
            timeout: 8000
        });

        // Ler versão remota — o agente.js no GitHub deve ter AGENTE_VERSION = 'X.Y.Z'
        const conteudoMeta = resp.data?.content
            ? Buffer.from(resp.data.content, 'base64').toString('utf8').slice(0, 500)
            : '';
        const matchVer = conteudoMeta.match(/AGENTE_VERSION\s*=\s*['"]([\d.]+)['"]/);
        const versaoRemota = matchVer ? matchVer[1] : null;

        if (!versaoRemota) {
            console.log('[UPDATE] Não foi possível ler versão remota');
            return;
        }

        // Comparar versões semânticas (MAJOR.MINOR.PATCH)
        const partes = v => v.split('.').map(Number);
        const maiorQue = (a, b) => {
            const [pa, pb] = [partes(a), partes(b)];
            for (let i = 0; i < 3; i++) {
                if ((pa[i]||0) > (pb[i]||0)) return true;
                if ((pa[i]||0) < (pb[i]||0)) return false;
            }
            return false;
        };

        if (!maiorQue(versaoRemota, AGENTE_VERSION)) {
            console.log(`[UPDATE] Agente já está na versão mais recente (${AGENTE_VERSION})`);
            return;
        }

        console.log(`[UPDATE] Nova versão disponível: ${versaoRemota} (atual: ${AGENTE_VERSION})`);

        // Baixar novo conteúdo via raw
        const rawUrl = `https://raw.githubusercontent.com/${GITHUB_RAW_USER}/${GITHUB_RAW_REPO}/${GITHUB_RAW_BRANCH}/${GITHUB_RAW_FILE}`;
        const rawResp = await axios.get(rawUrl, { timeout: 15000, responseType: 'text' });
        const novoConteudo = rawResp.data;

        // Validação mínima: arquivo deve ter > 50KB e conter marcadores do agente
        if (!novoConteudo || novoConteudo.length < 50000) {
            console.log(`[UPDATE] ⚠ Arquivo baixado muito pequeno (${novoConteudo?.length||0} bytes) — abortando`);
            return;
        }
        if (!novoConteudo.includes('SUPABASE_URL') || !novoConteudo.includes('coletarSnapshot')) {
            console.log(`[UPDATE] ⚠ Arquivo não parece ser o agente Goomer — abortando`);
            return;
        }

        _atualizacaoPendente = { sha: versaoRemota, conteudo: novoConteudo };
        console.log(`[UPDATE] Atualização validada e pronta para aplicar`);

    } catch(eUpd) {
        console.log(`[UPDATE] Falha ao verificar: ${eUpd.message}`);
    }
}

async function aplicarAtualizacao() {
    if (!_atualizacaoPendente) return false;
    const { sha, conteudo } = _atualizacaoPendente;
    try {
        const caminhoAtual = process.argv[1] || __filename;
        const caminhoBak   = caminhoAtual + '.bak';
        const caminhoNovo  = caminhoAtual + '.novo';

        // Salvar arquivo novo temporário
        fs.writeFileSync(caminhoNovo, conteudo, 'utf8');

        // Backup do arquivo atual
        fs.copyFileSync(caminhoAtual, caminhoBak);

        // Substituir pelo novo
        fs.copyFileSync(caminhoNovo, caminhoAtual);
        fs.unlinkSync(caminhoNovo);

        console.log(`✅ [UPDATE] Agente atualizado: ${AGENTE_VERSION} → ${sha} — reiniciando...`);
        _atualizacaoPendente = null;

        // Aguardar 2s para garantir que o log foi escrito
        await new Promise(r => setTimeout(r, 2000));

        // NSSM vai reiniciar automaticamente
        process.exit(0);
        return true;
    } catch(eApl) {
        console.log(`[UPDATE] ⚠ Erro ao aplicar atualização: ${eApl.message}`);
        _atualizacaoPendente = null;
        return false;
    }
}

async function carregarConfig(tokenLoja) {
    try {
        // Buscar config global + config específica da loja
        const url = `${process.env.SUPABASE_URL || SUPABASE_URL}/rest/v1/agente_config?select=chave,valor,token_loja&or=(token_loja.is.null,token_loja.eq.${encodeURIComponent(tokenLoja||'')})`;
        const resp = await axios.get(url, {
            headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` },
            timeout: 5000
        });
        const rows = resp.data || [];

        // Aplicar: config global primeiro, depois config específica da loja sobrescreve
        const global = rows.filter(r => !r.token_loja);
        const local  = rows.filter(r =>  r.token_loja);

        [...global, ...local].forEach(r => {
            const val = r.valor === 'true' ? true : r.valor === 'false' ? false : r.valor;
            _config[r.chave] = val;
        });

        console.log(`[CONFIG] flags carregadas: ip=${_config.autocorrecao_ip} spooler=${_config.autocorrecao_spooler} mysql=${_config.autocorrecao_mysql} logs=${_config.coleta_logs} latência=${_config.coleta_latencia}`);
    } catch(e) {
        console.log(`[CONFIG] Falha ao carregar — usando padrões: ${e.message}`);
    }
}

// ── Medição contínua de latência ─────────────────────────────────────────────
// Acumula medições a cada 30s e envia médias no snapshot
const _lat = {
    gateway:  { amostras: [], alvo: null },
    internet: { amostras: [], alvo: '8.8.8.8' },
    api:      { amostras: [], alvo: 'api.abrahao.com.br' },
    servidor: { amostras: [], alvo: null } // detectado dinamicamente
};

function _medirPing(host) {
    return new Promise(resolve => {
        try {
            const { execSync } = require('child_process');
            // Usar o tempo reportado pelo próprio ping (mais preciso que Date.now)
            // -n 2 para média de 2 pacotes reduz variação
            const out = execSync(`ping -n 2 -w 1500 ${host}`, { timeout: 5000 }).toString();
            const matches = [...out.matchAll(/(?:tempo|time)[=<](\d+)ms/gi)];
            const times = matches.map(m => parseInt(m[1])).filter(n => !isNaN(n));
            const perda = out.match(/100%\s+(?:de\s+)?(?:perda|loss)/i);
            if (times.length > 0) {
                const med = Math.round(times.reduce((a,b)=>a+b,0)/times.length);
                resolve({ ms: med, perda: false });
            } else {
                resolve({ ms: null, perda: true });
            }
        } catch(e) { resolve({ ms: null, perda: true }); }
    });
}

// Fallback TCP — para gateways que bloqueiam ICMP
function _medirTCP(host, porta=80) {
    return new Promise(resolve => {
        const net = require('net');
        const inicio = Date.now();
        const socket = new net.Socket();
        socket.setTimeout(2000);
        socket.connect(porta, host, () => {
            const ms = Date.now() - inicio;
            socket.destroy();
            resolve({ ms, perda: false });
        });
        socket.on('error', () => { socket.destroy(); resolve({ ms: null, perda: true }); });
        socket.on('timeout', () => { socket.destroy(); resolve({ ms: null, perda: true }); });
    });
}

async function _medirGateway(host) {
    // Tenta ICMP primeiro, fallback para TCP porta 80
    const res = await _medirPing(host);
    if (res.ms !== null) return res;
    return await _medirTCP(host, 80);
}

async function _medirHTTP(url) {
    const inicio = Date.now(); // guardar ANTES do try para medir tempo real
    try {
        await axios.head(url, { timeout: 4000 });
        return { ms: Date.now() - inicio, perda: false };
    } catch(e) {
        // Se retornou qualquer resposta HTTP (401, 404, etc) = servidor respondeu
        // O tempo é real — mede latência real de rede mesmo com erro HTTP
        if (e.response) return { ms: Date.now() - inicio, perda: false };
        return { ms: null, perda: true };
    }
}

function _resumoLat(amostras) {
    const validos = amostras.filter(a => a.ms !== null);
    const perdas  = amostras.filter(a => a.perda).length;
    if (!validos.length) return { min: null, med: null, max: null, perda_pct: 100, amostras: amostras.length };
    const ms = validos.map(a => a.ms);
    return {
        min:       Math.min(...ms),
        med:       Math.round(ms.reduce((s,v)=>s+v,0)/ms.length),
        max:       Math.max(...ms),
        perda_pct: Math.round((perdas/amostras.length)*100),
        amostras:  amostras.length
    };
}

async function coletarLatencia() {
    // Detectar gateway — tenta múltiplos métodos com fallback
    if (!_lat.gateway.alvo) {
        // Método 1: PowerShell (mais preciso)
        try {
            const fs2 = require('fs'), os2 = require('os');
            const tmp = os2.tmpdir() + '\\gt_gw_lat_' + Date.now() + '.ps1';
            const sc = '$a=Get-NetIPConfiguration|Where-Object{$_.IPv4DefaultGateway -ne $null -and $_.NetAdapter.Status -eq "Up"}|Select-Object -First 1; if($a){$a.IPv4DefaultGateway.NextHop}else{""}';
            fs2.writeFileSync(tmp, sc, 'utf8');
            const { execSync: es2 } = require('child_process');
            const gw = es2('powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + tmp + '"', { timeout: 8000, encoding: 'utf8' }).trim();
            try { fs2.unlinkSync(tmp); } catch(e) {}
            if (gw && gw.match(/^\d+\.\d+\.\d+\.\d+$/)) {
                _lat.gateway.alvo = gw;
                // console.log(`[LAT] Gateway detectado: ${gw}`);
            }
        } catch(e) {}

        // Método 2: route print via CMD (fallback mais simples)
        if (!_lat.gateway.alvo) {
            try {
                const { execSync: es3 } = require('child_process');
                const out = es3('route print 0.0.0.0', { timeout: 3000, encoding: 'utf8' });
                const match = out.match(/0\.0\.0\.0\s+0\.0\.0\.0\s+(\d+\.\d+\.\d+\.\d+)/);
                if (match) {
                    _lat.gateway.alvo = match[1];
                    // console.log(`[LAT] Gateway via route: ${match[1]}`);
                }
            } catch(e) {}
        }
    }

    // Detectar porta do servidor Goomer
    if (!_lat.servidor.alvo) {
        try {
            const db3 = await getDb();
            const [cfg3] = await db3.execute('SELECT local_server_port FROM app_configuration LIMIT 1');
            await db3.end();
            const porta = cfg3[0]?.local_server_port || 5000;
            _lat.servidor.alvo = `http://localhost:${porta}/configurations`;
        } catch(e) {}
    }

    // Medir em paralelo
    // Resolver IP da API Abrahão uma vez e cachear
    if (!_lat.api._ip) {
        try {
            const { execSync: es4 } = require('child_process');
            const out = es4('nslookup api.abrahao.com.br', { timeout: 3000, encoding: 'utf8' });
            const match = out.match(/Address(?:es)?:\s*(\d+\.\d+\.\d+\.\d+)/i);
            if (match) {
                _lat.api._ip = match[1];
                _lat.api.alvo = `api.abrahao.com.br (${match[1]})`;
                // console.log(`[LAT] API IP resolvido: ${match[1]}`);
            }
        } catch(e) {}
        if (!_lat.api._ip) _lat.api._ip = 'api.abrahao.com.br'; // fallback hostname
    }

    const [resGW, resNet, resAPI] = await Promise.all([
        _lat.gateway.alvo  ? _medirGateway(_lat.gateway.alvo)  : Promise.resolve({ ms: null, perda: true }),
        _medirPing('8.8.8.8'),
        _medirPing(_lat.api._ip)  // ICMP igual ao CMD — mesma metodologia
    ]);

    // Servidor local — medir via TCP para refletir conectividade real de rede
    // (não via localhost — seria sempre rápido e não indicaria qualidade de rede)
    let resSrv = { ms: null, perda: true };
    if (_lat.servidor.alvo) {
        try {
            // Extrair porta do alvo (http://localhost:PORT/...)
            const portMatch = _lat.servidor.alvo.match(/:(\d+)\//);
            const porta = portMatch ? parseInt(portMatch[1]) : 5000;
            // Medir via TCP na porta do servidor
            resSrv = await _medirTCP('127.0.0.1', porta);
        } catch(e) {}
    }

    _lat.gateway.amostras.push(resGW);
    _lat.internet.amostras.push(resNet);
    _lat.api.amostras.push(resAPI);
    _lat.servidor.amostras.push(resSrv);

    // Manter apenas últimas 12 amostras (6 minutos)
    ['gateway','internet','api','servidor'].forEach(k => {
        if (_lat[k].amostras.length > 12) _lat[k].amostras = _lat[k].amostras.slice(-12);
    });
}

function getLatenciaSnapshot() {
    const res = {};
    ['gateway','internet','api','servidor'].forEach(k => {
        if (_lat[k].amostras.length > 0) {
            res[k] = { ..._resumoLat(_lat[k].amostras), alvo: _lat[k].alvo };
        }
    });
    // Limpar amostras após snapshot (nova janela de 5 min)
    ['gateway','internet','api','servidor'].forEach(k => { _lat[k].amostras = []; });
    return res;
}

// Iniciar medição a cada 30s
setInterval(coletarLatencia, 30000);
coletarLatencia(); // primeira medição imediata

// ── Loop principal ────────────────────────────────────────────────────────────
async function ciclo() {
    const agora = new Date().toLocaleTimeString('pt-BR');
    try {
        console.log(`[${agora}] Coletando snapshot...`);

        // Carregar config do Supabase (permite controle remoto)
        // Tentar pegar token da loja para config específica
        let tokenParaConfig = null;
        try {
            const dbT = await getDb();
            const [tkRows] = await dbT.execute('SELECT token FROM store WHERE deletedAt IS NULL LIMIT 1');
            await dbT.end();
            tokenParaConfig = tkRows[0]?.token?.trim() || null;
        } catch(e) {}
        await carregarConfig(tokenParaConfig);

        // ── Manter servidor Goomer ativo ────────────────────────────────────
        await verificarEIniciarServidor(_portaLocalCache || 4999);

        // ── Limpeza preventiva de deadlock MySQL ─────────────────────────────
        await limparDeadlockMySQL();

        // ── Modo Teste de Rede ────────────────────────────────────────────
        const _dentroJanela = dentroJanelaTeste();
        if (_dentroJanela) {
            if (!_testeRedeAtivo) {
                // Início de nova sessão de teste
                _testeRedeAtivo  = true;
                _testeRedeInicio = new Date();
                _testeRedeResultados = {};
                console.log(`[TESTE-REDE] Sessão iniciada — janela ${_config.teste_rede_inicio}–${_config.teste_rede_fim}`);
            }
        } else if (_testeRedeAtivo) {
            // Fim da janela — sessão encerrada
            _testeRedeAtivo = false;
            const consolidado = consolidarTesteRede();
            console.log(`[TESTE-REDE] Sessão encerrada — ${consolidado.length} tablet(s) analisados`);
            // Resultado fica em _testeRedeResultados consolidado para o snapshot pegar
        }

        // ── Auto-atualização ─────────────────────────────────────────────────
        if (_config.auto_atualizacao) {
            _ciclosDesdeUltimaVerificacao++;
            const forcarAgora = _config.atualizar_agora === true;

            // Verificar nova versão a cada CICLOS_ENTRE_VERIFICACOES ou quando forçado
            if (forcarAgora || _ciclosDesdeUltimaVerificacao >= CICLOS_ENTRE_VERIFICACOES) {
                _ciclosDesdeUltimaVerificacao = 0;
                await verificarAtualizacao();
            }

            // Aplicar atualização pendente — só em horário seguro ou quando forçado
            if (_atualizacaoPendente) {
                const horaAtual    = new Date().getHours();
                const horarioSeguro = horaAtual >= 2 && horaAtual <= 5;
                if (horarioSeguro || forcarAgora) {
                    await aplicarAtualizacao(); // process.exit(0) interno
                } else {
                    console.log(`[UPDATE] Atualização pendente — aguardando horário seguro (2h-5h) | atual: ${horaAtual}h. Ative 'atualizar_agora' para forçar.`);
                }
            }
        }

        const snap = await coletarSnapshot();

        // Verificar se a loja está bloqueada no NOC — se sim, não enviar snapshot
        if (snap.token_loja) {
            try {
                const urlBlq = `${SUPABASE_URL}/rest/v1/lojas_bloqueadas?token_loja=eq.${encodeURIComponent(snap.token_loja)}&select=token_loja`;
                const resBlq = await axios.get(urlBlq, {
                    headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` },
                    timeout: 4000
                });
                if (Array.isArray(resBlq.data) && resBlq.data.length > 0) {
                    console.log(`[${agora}] ⊘ Loja bloqueada — snapshot não enviado (inativada no NOC)`);
                    return; // encerra o ciclo sem enviar
                }
            } catch(eBlq) {
                // Se não conseguir verificar, continua e envia normalmente
                console.log(`[${agora}] ⚠ Não foi possível verificar bloqueio — enviando normalmente`);
            }
        }
        const status = await enviarSupabase(snap);
        console.log(`[${agora}] ✓ Enviado — loja: ${snap.nome_loja} | nota: ${snap.nota} (${snap.score}%) | status HTTP: ${status}`);
    } catch(e) {
        console.error(`[${agora}] ✗ Erro na coleta: ${e.message}`);
        // ── Autocorreção: MySQL offline ──────────────────────────────────────
        // Se o banco caiu mas a máquina está de pé, tentar reiniciar o serviço
        if (_config.autocorrecao_mysql) {
            try {
                const { execSync: esSQL } = require('child_process');

                // Detectar nome do serviço MySQL instalado
                let servicoMySQL = null;
                const candidatos = ['MySQL80', 'MySQL', 'MySQL57', 'MariaDB', 'mysql'];
                for (const svc of candidatos) {
                    try {
                        const out = esSQL(`sc query ${svc}`, { timeout: 2000, encoding: 'utf8' });
                        if (out.includes('SERVICE_NAME')) { servicoMySQL = svc; break; }
                    } catch(e) {}
                }

                if (servicoMySQL) {
                    // Verificar se está realmente parado
                    const statusSQL = esSQL(`sc query ${servicoMySQL}`, { timeout: 2000, encoding: 'utf8' });
                    const parado = statusSQL.includes('STOPPED') || statusSQL.includes('PARADO');

                    // Verificar uptime da máquina — não reiniciar se a máquina acabou de ligar
                    const os5 = require('os');
                    const uptimeMins = Math.floor(os5.uptime() / 60);

                    if (parado && uptimeMins > 5) {
                        console.log(`[MYSQL] Serviço ${servicoMySQL} parado — tentando reiniciar`);
                        esSQL(`net start ${servicoMySQL}`, { timeout: 15000, encoding: 'utf8' });
                        await new Promise(r => setTimeout(r, 3000));

                        const statusPos = esSQL(`sc query ${servicoMySQL}`, { timeout: 2000, encoding: 'utf8' });
                        const subiu = statusPos.includes('RUNNING') || statusPos.includes('EM_EXECUCAO');
                        console.log(`[MYSQL] Reinício: ${subiu ? 'sucesso' : 'falhou'}`);
                    } else if (parado && uptimeMins <= 5) {
                        console.log(`[MYSQL] Serviço parado mas máquina iniciou há ${uptimeMins}min — aguardando inicialização`);
                    }
                }
            } catch(eSqlAuto) {
                console.log(`[MYSQL-AUTO] Erro: ${eSqlAuto.message}`);
            }
        }

        // Enviar snapshot mínimo de emergência para o NOC saber que a máquina está online
        try {
            const os = require('os');
            const ips = Object.values(os.networkInterfaces())
                .flat().filter(i => i.family==='IPv4' && !i.internal && !i.address.startsWith('169.254'))
                .map(i => i.address);
            const snapMin = {
                token_loja:       'desconhecido',
                nome_loja:        os.hostname(),
                score:            0,
                nota:             'Grave',
                servidor_online:  false,
                tablets_ativos:   0,
                tablets_total:    0,
                erros_log:        0,
                retry_expirados:  0,
                alertas_criticos: 1,
                alertas_avisos:   0,
                payload: {
                    db_offline:     true,
                    erro:           e.message,
                    ip_servidor:    ips[0] || null,
                    status:         'Grave',
                    coletado_em:    new Date().toISOString()
                }
            };
            // Tentar recuperar token do banco mesmo com erro parcial
            try {
                const db = await getDb();
                const [rows] = await db.execute('SELECT token FROM store WHERE deletedAt IS NULL LIMIT 1');
                const [nome] = await db.execute('SELECT store_name FROM app_configuration LIMIT 1');
                await db.end();
                if (rows[0]?.token) snapMin.token_loja = rows[0].token.trim();
                if (nome[0]?.store_name) snapMin.nome_loja = nome[0].store_name;
                snapMin.payload.db_offline = false; // banco respondeu, erro foi em outro lugar
            } catch(e2) {
                snapMin.payload.db_offline = true;
                console.error(`[${agora}] ✗ Banco também offline: ${e2.message}`);
            }
            await enviarSupabase(snapMin);
            console.log(`[${agora}] ⚠ Snapshot de emergência enviado`);
        } catch(e3) {
            console.error(`[${agora}] ✗ Falha no snapshot de emergência: ${e3.message}`);
        }
    }
}

// Rodar imediatamente e depois a cada INTERVALO_MS
ciclo();
setInterval(ciclo, INTERVALO_MS);
console.log(`Goomer Agente iniciado — enviando a cada ${INTERVALO_MS/60000} minutos para Supabase`);
