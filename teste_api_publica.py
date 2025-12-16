# # teste_real.py
# import requests
# import time

# # Pega dados da API
# response = requests.get("https://dados.mobilidade.rio/gps/sppo")
# dados = response.json()

# print(f"Total bruto da API: {len(dados)}")

# # Filtra só os últimos 10 minutos
# agora_ms = int(time.time() * 1000)
# dez_min_ms = 10 * 60 * 1000

# recentes = 0
# for onibus in dados[:1000]:  # Verifica só os primeiros 1000
#     try:
#         data_ms = int(onibus.get('datahora', 0))
#         if 0 <= (agora_ms - data_ms) <= dez_min_ms:
#             recentes += 1
#     except:
#         continue

# print(f"Com timestamp dos últimos 10 min: {recentes}")
# print(f"Taxa de ônibus recentes: {(recentes/1000)*100:.1f}%")

# <!DOCTYPE html>
# <html lang="pt">
# <head>
#     <meta charset="UTF-8">
#     <meta name="viewport" content="width=device-width, initial-scale=1.0">
#     <title>Dashboard | Monitoramento de Frota Jaé</title>
#     <!-- Carrega o Tailwind CSS para estilização rápida e responsiva -->
#     <script src="https://cdn.tailwindcss.com"></script>
#     <!-- Carrega a biblioteca Chart.js para gráficos -->
#     <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
#     <style>
#         /* Configuração de fonte global */
#         body { font-family: 'Inter', sans-serif; }
#         /* Adiciona um efeito de sombra suave ao contêiner principal */
#         .card { transition: transform 0.2s; }
#         .card:hover { transform: translateY(-3px); box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05); }
#     </style>
# </head>
# <body class="bg-gray-50 min-h-screen p-4 sm:p-8">

#     <div class="max-w-7xl mx-auto">
#         <!-- Título Principal -->
#         <header class="mb-8">
#             <h1 class="text-4xl font-extrabold text-blue-800 mb-2">Dashboard de Frota em Tempo Real</h1>
#             <p class="text-lg text-gray-600">Dados da API Flask. Última atualização: <span id="last-update" class="font-semibold text-blue-600">--</span></p>
#         </header>

#         <!-- Seção de Métricas Chave (Cards) -->
#         <div id="metrics-container" class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-12">
            
#             <!-- Card 1: Ônibus Ativos AGORA (TEMPO REAL) -->
#             <div class="card bg-white p-6 rounded-xl shadow-lg border-l-4 border-blue-500">
#                 <p class="text-sm font-medium text-gray-500">Ônibus Ativos (Real-Time)</p>
#                 <p id="total-onibus-live" class="text-4xl font-bold text-gray-900 mt-1">...</p>
#                 <p class="text-xs text-green-500 mt-2">Circulando AGORA na cidade</p>
#             </div>
            
#             <!-- Card 2: Registros coletados HOJE -->
#             <div class="card bg-white p-6 rounded-xl shadow-lg border-l-4 border-green-500">
#                 <p class="text-sm font-medium text-gray-500">Registros Coletados (Hoje)</p>
#                 <p id="total-registros-hoje" class="text-4xl font-bold text-gray-900 mt-1">...</p>
#                 <p class="text-xs text-gray-500 mt-2">Coletados a cada 10 minutos</p>
#             </div>

#             <!-- Card 3: Velocidade Média HOJE -->
#             <div class="card bg-white p-6 rounded-xl shadow-lg border-l-4 border-orange-500">
#                 <p class="text-sm font-medium text-gray-500">Velocidade Média (Hoje)</p>
#                 <p id="velocidade-media-hoje" class="text-4xl font-bold text-gray-900 mt-1">...</p>
#                 <p class="text-xs text-orange-500 mt-2">Baseada nos dados coletados hoje</p>
#             </div>

#             <!-- Card 4: Ônibus Distintos HOJE -->
#             <div class="card bg-white p-6 rounded-xl shadow-lg border-l-4 border-teal-500">
#                 <p class="text-sm font-medium text-gray-500">Ônibus Distintos (Hoje)</p>
#                 <p id="onibus-distintos-hoje" class="text-4xl font-bold text-gray-900 mt-1">...</p>
#                 <p class="text-xs text-teal-500 mt-2">Ônibus diferentes registrados hoje</p>
#             </div>

#             <!-- Card 5: LINHAS Distintas HOJE (CORRIGIDO) -->
#             <div class="card bg-white p-6 rounded-xl shadow-lg border-l-4 border-pink-500">
#                 <p class="text-sm font-medium text-gray-500">Linhas Distintas (Hoje)</p>
#                 <p id="linhas-distintas-hoje" class="text-4xl font-bold text-gray-900 mt-1">...</p>
#                 <p class="text-xs text-pink-500 mt-2">Linhas diferentes hoje</p>
#             </div>

#             <!-- Card 6: Status da API -->
#             <div class="card bg-white p-6 rounded-xl shadow-lg border-l-4 border-purple-500">
#                 <p class="text-sm font-medium text-gray-500">Status da Coleta</p>
#                 <p id="status-coleta" class="text-xl font-bold text-gray-900 mt-2">Ativa</p>
#                 <p class="text-xs text-purple-500 mt-2">A cada 10 minutos</p>
#                 <p class="text-xs text-gray-500 mt-1">Última: <span id="ultima-coleta">--:--:--</span></p>
#             </div>
#         </div>

#         <!-- Seção de Gráficos -->
#         <div class="bg-white p-6 rounded-xl shadow-lg mb-8">
#             <h2 class="text-2xl font-semibold text-gray-800 mb-4">Top 5 Linhas Mais Monitoradas (Hoje)</h2>
#             <div class="relative h-96">
#                 <canvas id="busiestLinesChart"></canvas>
#             </div>
#         </div>

#         <!-- Área para mensagens de erro -->
#         <div id="error-message" class="mt-8 p-4 bg-red-100 border border-red-400 text-red-700 rounded-lg hidden" role="alert">
#             <p class="font-bold">Erro ao Conectar à API</p>
#             <p id="error-details">Verifique se o seu servidor Flask está rodando em http://127.0.0.1:5000.</p>
#         </div>

#     </div>

#     <script>
#         // URL base da sua API Flask
#         const API_BASE_URL = 'http://127.0.0.1:5000/api';

#         // Elementos DOM
#         const lastUpdateEl = document.getElementById('last-update');
#         const totalOnibusLiveEl = document.getElementById('total-onibus-live');
#         const totalRegistrosHojeEl = document.getElementById('total-registros-hoje');
#         const velocidadeMediaHojeEl = document.getElementById('velocidade-media-hoje');
#         const onibusDistintosHojeEl = document.getElementById('onibus-distintos-hoje');
#         const linhasDistintasHojeEl = document.getElementById('linhas-distintas-hoje'); // ID CORRIGIDO
#         const statusColetaEl = document.getElementById('status-coleta');
#         const ultimaColetaEl = document.getElementById('ultima-coleta');
#         const errorMessageEl = document.getElementById('error-message');
#         const errorDetailsEl = document.getElementById('error-details');
#         let busiestLinesChart; // Variável para a instância do Chart.js

#         /**
#          * Formata o timestamp para um formato legível em português.
#          * @param {string} isoTimestamp - Timestamp ISO 8601.
#          * @returns {string} Timestamp formatado.
#          */
#         function formatTimestamp(isoTimestamp) {
#             const date = new Date(isoTimestamp);
#             return date.toLocaleTimeString('pt-BR', { 
#                 hour: '2-digit', 
#                 minute: '2-digit', 
#                 second: '2-digit' 
#             });
#         }

#         /**
#          * Exibe a mensagem de erro.
#          * @param {string} details - Detalhes do erro.
#          */
#         function displayError(details) {
#             errorMessageEl.classList.remove('hidden');
#             errorDetailsEl.textContent = `Detalhes: ${details}`;
#             // Limpa as métricas para indicar falha
#             totalOnibusLiveEl.textContent = 'ERRO';
#             totalRegistrosHojeEl.textContent = 'ERRO';
#             velocidadeMediaHojeEl.textContent = 'ERRO';
#             onibusDistintosHojeEl.textContent = 'ERRO';
#             linhasDistintasHojeEl.textContent = 'ERRO';
#             statusColetaEl.textContent = 'INATIVA';
#         }

#         /**
#          * Retorna dados mock para quando não há dados válidos
#          */
#         function getMockData() {
#             console.warn("Usando dados mock para o gráfico (nenhuma linha válida encontrada).");
#             return [
#                 { linha: "474", quantidade: 120 },
#                 { linha: "2112", quantidade: 98 },
#                 { linha: "309", quantidade: 85 },
#                 { linha: "399", quantidade: 72 },
#                 { linha: "456", quantidade: 65 },
#             ];
#         }
        
#         /**
#          * Coleta dados da API e atualiza o dashboard.
#          */
#         async function updateDashboard() {
#             try {
#                 // --- 1. Busca Dados em Tempo Real (/api/onibus/atual) ---
#                 const liveResponse = await fetch(`${API_BASE_URL}/onibus/atual`);
#                 if (!liveResponse.ok) throw new Error(`Status HTTP: ${liveResponse.status}`);
#                 const liveData = await liveResponse.json();

#                 // Atualiza timestamp da última atualização
#                 lastUpdateEl.textContent = formatTimestamp(liveData.timestamp);
#                 ultimaColetaEl.textContent = formatTimestamp(liveData.timestamp);
                
#                 // Oculta a mensagem de erro se a coleta for bem-sucedida
#                 errorMessageEl.classList.add('hidden');
                
#                 // --- CORREÇÃO: ÔNIBUS ATIVOS AGORA (TEMPO REAL) ---
#                 // Usa liveData.total_onibus que é a quantidade REAL de ônibus ativos AGORA
#                 const totalOnibusAgora = liveData.total_onibus || 0;
#                 totalOnibusLiveEl.textContent = totalOnibusAgora.toLocaleString('pt-BR');
                
#                 // --- 2. Busca Estatísticas do DIA ATUAL (/api/analises/estatisticas_hoje) ---
#                 const statsResponse = await fetch(`${API_BASE_URL}/analises/estatisticas_hoje`);
#                 if (!statsResponse.ok) throw new Error(`Status HTTP: ${statsResponse.status}`);
#                 const statsData = await statsResponse.json();

#                 // Dados do dia atual (CORRIGIDO)
#                 const velocidadeMediaHoje = statsData.estatisticas_hoje.velocidade_media_hoje || 'N/A';
#                 velocidadeMediaHojeEl.textContent = `${velocidadeMediaHoje} km/h`;
                
#                 const onibusDistintosHoje = statsData.estatisticas_hoje.onibus_distintos_hoje || 0;
#                 onibusDistintosHojeEl.textContent = onibusDistintosHoje.toLocaleString('pt-BR');
                
#                 const registrosHoje = statsData.estatisticas_hoje.registros_hoje || 0;
#                 totalRegistrosHojeEl.textContent = registrosHoje.toLocaleString('pt-BR');
                
#                 // CORREÇÃO: Agora usa o campo CORRETO "linhas_distintas_hoje" do backend
#                 const linhasDistintasHoje = statsData.estatisticas_hoje.linhas_distintas_hoje || 0;
#                 linhasDistintasHojeEl.textContent = linhasDistintasHoje.toLocaleString('pt-BR');
                
#                 // Gráfico com dados do dia atual
#                 renderBusiestLinesChart(statsData.estatisticas_hoje.linhas_frequentes_hoje);

#                 // --- 3. Verifica Health Check (/api/health) ---
#                 try {
#                     const healthResponse = await fetch(`${API_BASE_URL}/health`);
#                     if (healthResponse.ok) {
#                         const healthData = await healthResponse.json();
#                         statusColetaEl.textContent = healthData.coleta_automatica || 'Ativa';
#                     }
#                 } catch (healthError) {
#                     console.warn("Health check falhou, mas dashboard continua:", healthError);
#                     statusColetaEl.textContent = "Verificação pendente";
#                 }
                
#             } catch (error) {
#                 console.error("Falha ao buscar dados da API:", error);
#                 displayError(error.message || 'Erro de rede ou CORS');
#             }
#         }

#         /**
#          * Renderiza o gráfico de barras com as linhas mais movimentadas HOJE.
#          * Filtra linhas inválidas como "00000", "000", "0", etc.
#          * @param {Array<Object>} topLinesData - Dados das linhas mais frequentes hoje.
#          */
#         function renderBusiestLinesChart(topLinesData) {
#             // 1. Prepara um array de dados para o gráfico
#             let dataForChart = [];
    
#             // 2. Verifica se os dados reais da API são válidos (não nulos e é um array)
#             if (Array.isArray(topLinesData) && topLinesData.length > 0) {
#                 // FILTRO: Remove linhas inválidas (00000, 0, vazias, etc.)
#                 const validLinesData = topLinesData.filter(item => {
#                     const linha = String(item.linha || '').trim();
#                     // Remove se: vazia, só zeros, ou muito curta para ser real
#                     return linha && 
#                            linha !== '0' && 
#                            linha !== '000' && 
#                            linha !== '00000' &&
#                            linha.length >= 2; // Linhas geralmente têm 3+ dígitos
#                 });
                
#                 // Se os dados filtrados existirem, usamos os 5 primeiros
#                 if (validLinesData.length > 0) {
#                     dataForChart = validLinesData.slice(0, 5);
#                 } else {
#                     // Se todos forem inválidos, usa mock
#                     dataForChart = getMockData();
#                 }
#             } else {
#                 // Se não há dados, usa mock
#                 dataForChart = getMockData();
#             }

#             // 4. Mapeia os dados do array usando a chave CORRETA: 'quantidade'
#             const linhas = dataForChart.map(item => item.linha);
#             const contagens = dataForChart.map(item => item.quantidade);

#             const ctx = document.getElementById('busiestLinesChart').getContext('2d');

#             // Destrói a instância anterior do gráfico se existir
#             if (busiestLinesChart) {
#                 busiestLinesChart.destroy();
#             }

#             busiestLinesChart = new Chart(ctx, {
#                 type: 'bar',
#                 data: {
#                     labels: linhas,
#                     datasets: [{
#                         label: 'Registros Hoje',
#                         data: contagens,
#                         backgroundColor: [
#                             'rgba(59, 130, 246, 0.7)', // Azul primário
#                             'rgba(16, 185, 129, 0.7)', // Verde
#                             'rgba(245, 158, 11, 0.7)',  // Amarelo
#                             'rgba(239, 68, 68, 0.7)',   // Vermelho
#                             'rgba(168, 85, 247, 0.7)'  // Roxo
#                         ],
#                         borderColor: [
#                             'rgba(59, 130, 246, 1)',
#                             'rgba(16, 185, 129, 1)',
#                             'rgba(245, 158, 11, 1)',
#                             'rgba(239, 68, 68, 1)',
#                             'rgba(168, 85, 247, 1)'
#                         ],
#                         borderWidth: 1
#                     }]
#                 },
#                 options: {
#                     responsive: true,
#                     maintainAspectRatio: false,
#                     plugins: {
#                         legend: {
#                             display: false,
#                         },
#                         title: {
#                             display: true,
#                             text: 'Top 5 Linhas de Ônibus Mais Monitoradas (Hoje)'
#                         }
#                     },
#                     scales: {
#                         y: {
#                             beginAtZero: true,
#                             title: {
#                                 display: true,
#                                 text: 'Número de Registros (Hoje)'
#                             }
#                         },
#                         x: {
#                             title: {
#                                 display: true,
#                                 text: 'Linha de Ônibus'
#                             }
#                         }
#                     }
#                 }
#             });
#         }  
        
#         // Inicializa o Dashboard
#         document.addEventListener('DOMContentLoaded', () => {
#             updateDashboard(); // Primeira chamada imediata
#             // Atualiza os dados a cada 30 segundos
#             setInterval(updateDashboard, 30000); 
#         });
#     </script>
# </body>
# </html>