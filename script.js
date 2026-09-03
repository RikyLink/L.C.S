// ============================================
// LAN Chat & File Transfer - WebRTC P2P
// Sinalização manual via copy & paste
// ============================================

// ---------- Configuração Global ----------
const ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
];

const CHUNK_SIZE = 16 * 1024; // 16 KB por chunk
const BUFFER_THRESHOLD = 1024 * 1024; // 1 MB - pausa se bufferedAmount exceder
const GATHERING_TIMEOUT = 3000; // ms para aguardar coleta de ICE

// Estado global
let localUserName = '';
let peerConnection = null;       // RTCPeerConnection
let dataChannel = null;          // DataChannel principal
let isOfferer = false;
let pendingIceCandidates = [];
let iceGatheringComplete = false;
let gatheringResolve = null;

// Estado para envio de arquivos
let currentSendFile = null;      // { file, fileId, fileName, fileSize, totalChunks, sentChunks, startTime, bytesSent }
let isSendingFile = false;

// Estado para recebimento de arquivos
let currentReceiveFile = null;   // { fileId, fileName, fileSize, totalChunks, chunks: [], bytesReceived }

// Elementos DOM
const elements = {
    userName: document.getElementById('userName'),
    btnCreateOffer: document.getElementById('btnCreateOffer'),
    offerOutput: document.getElementById('offerOutput'),
    btnCopyOffer: document.getElementById('btnCopyOffer'),
    offerInput: document.getElementById('offerInput'),
    btnAnswer: document.getElementById('btnAnswer'),
    answerOutput: document.getElementById('answerOutput'),
    btnCopyAnswer: document.getElementById('btnCopyAnswer'),
    answerInput: document.getElementById('answerInput'),
    btnConnect: document.getElementById('btnConnect'),
    statusDot: document.getElementById('statusDot'),
    statusText: document.getElementById('statusText'),
    logContainer: document.getElementById('logContainer'),
    btnReset: document.getElementById('btnReset'),
    chatMessages: document.getElementById('chatMessages'),
    chatInput: document.getElementById('chatInput'),
    btnSendChat: document.getElementById('btnSendChat'),
    dropZone: document.getElementById('dropZone'),
    btnSelectFile: document.getElementById('btnSelectFile'),
    fileInput: document.getElementById('fileInput'),
    uploadProgress: document.getElementById('uploadProgress'),
    uploadFileName: document.getElementById('uploadFileName'),
    uploadProgressBar: document.getElementById('uploadProgressBar'),
    uploadProgressText: document.getElementById('uploadProgressText'),
    receivedFilesList: document.getElementById('receivedFilesList')
};

// ---------- Utilidades ----------
function log(message) {
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    const time = new Date().toLocaleTimeString();
    entry.innerHTML = `<span class="log-time">[${time}]</span> ${message}`;
    elements.logContainer.appendChild(entry);
    elements.logContainer.scrollTop = elements.logContainer.scrollHeight;
}

function setStatus(state, text) {
    elements.statusDot.className = 'dot';
    switch (state) {
        case 'connected':
            elements.statusDot.classList.add('dot-connected');
            elements.statusText.textContent = text || 'Conectado';
            break;
        case 'connecting':
            elements.statusDot.classList.add('dot-connecting');
            elements.statusText.textContent = text || 'Conectando...';
            break;
        case 'failed':
            elements.statusDot.classList.add('dot-failed');
            elements.statusText.textContent = text || 'Falha na conexão';
            break;
        default:
            elements.statusDot.classList.add('dot-disconnected');
            elements.statusText.textContent = text || 'Desconectado';
    }
}

async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        return true;
    } catch (err) {
        // Fallback
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        try {
            document.execCommand('copy');
            return true;
        } catch (err2) {
            return false;
        } finally {
            document.body.removeChild(textarea);
        }
    }
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + units[i];
}

function generateFileId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2, 9);
}

// ---------- Gerenciamento de ICE Candidates ----------
function setupIceCandidateHandler(pc) {
    pc.onicecandidate = (event) => {
        if (event.candidate) {
            pendingIceCandidates.push({
                candidate: event.candidate.candidate,
                sdpMid: event.candidate.sdpMid,
                sdpMLineIndex: event.candidate.sdpMLineIndex
            });
            log(`Candidato ICE coletado (${pendingIceCandidates.length} total)`);
        }
    };

    pc.onicegatheringstatechange = () => {
        log(`Estado de coleta ICE: ${pc.iceGatheringState}`);
        if (pc.iceGatheringState === 'complete') {
            iceGatheringComplete = true;
            if (gatheringResolve) gatheringResolve();
        }
    };
}

// Aguarda a coleta de candidatos ICE terminar (ou timeout)
async function waitForIceGathering(pc) {
    if (iceGatheringComplete) return;
    log('Aguardando coleta de candidatos ICE...');
    await new Promise((resolve) => {
        gatheringResolve = resolve;
        // Timeout de segurança
        setTimeout(() => {
            log('Timeout de coleta ICE — usando candidatos disponíveis');
            if (gatheringResolve) {
                gatheringResolve();
                gatheringResolve = null;
            }
        }, GATHERING_TIMEOUT);
    });
}

// Monta o bloco JSON com SDP + candidatos
function buildSignalingPayload() {
    if (!peerConnection || !peerConnection.localDescription) {
        throw new Error('Descrição local não disponível');
    }
    return JSON.stringify({
        sdp: peerConnection.localDescription.sdp,
        type: peerConnection.localDescription.type,
        candidates: pendingIceCandidates
    }, null, 2);
}

// Parseia o bloco JSON recebido
function parseSignalingPayload(text) {
    try {
        const data = JSON.parse(text);
        if (!data.sdp || !data.type) {
            throw new Error('Dados incompletos: faltam sdp ou type');
        }
        return data;
    } catch (err) {
        throw new Error('JSON inválido: ' + err.message);
    }
}

// Adiciona candidatos ICE a partir de um array
async function addIceCandidates(pc, candidates) {
    for (const candidate of candidates) {
        try {
            await pc.addIceCandidate(candidate);
            log('Candidato ICE adicionado');
        } catch (err) {
            log(`Erro ao adicionar candidato: ${err.message}`);
        }
    }
}

// ---------- Configuração do PeerConnection ----------
function createPeerConnection() {
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    setupIceCandidateHandler(pc);

    pc.onconnectionstatechange = () => {
        const state = pc.connectionState;
        log(`Estado da conexão: ${state}`);
        switch (state) {
            case 'connected':
                setStatus('connected');
                elements.btnSendChat.disabled = false;
                log('🎉 Conexão P2P estabelecida com sucesso!');
                break;
            case 'connecting':
                setStatus('connecting');
                break;
            case 'disconnected':
            case 'failed':
                setStatus('failed');
                elements.btnSendChat.disabled = true;
                break;
            default:
                setStatus('disconnected');
        }
    };

    return pc;
}

// ---------- Configuração do DataChannel ----------
function setupDataChannel(channel) {
    dataChannel = channel;
    channel.binaryType = 'arraybuffer';

    channel.onopen = () => {
        log('📡 DataChannel aberto');
        setStatus('connected');
        elements.btnSendChat.disabled = false;
    };

    channel.onclose = () => {
        log('📡 DataChannel fechado');
        setStatus('disconnected');
        elements.btnSendChat.disabled = true;
    };

    channel.onerror = (err) => {
        log(`⚠️ Erro no DataChannel: ${err}`);
    };

    channel.onmessage = (event) => {
        if (typeof event.data === 'string') {
            handleJsonMessage(event.data);
        } else if (event.data instanceof ArrayBuffer) {
            handleBinaryMessage(event.data);
        }
    };
}

// ---------- Handlers de mensagens ----------
function handleJsonMessage(jsonStr) {
    try {
        const message = JSON.parse(jsonStr);
        switch (message.type) {
            case 'text':
                displayChatMessage(message.sender, message.text, message.timestamp);
                break;
            case 'file-start':
                handleFileStart(message);
                break;
            case 'file-end':
                handleFileEnd(message);
                break;
            default:
                log(`Mensagem JSON desconhecida: ${message.type}`);
        }
    } catch (err) {
        log(`Erro ao processar mensagem JSON: ${err.message}`);
    }
}

function handleBinaryMessage(arrayBuffer) {
    if (!currentReceiveFile) {
        log('⚠️ Chunk binário recebido sem file-start correspondente');
        return;
    }
    currentReceiveFile.chunks.push(arrayBuffer);
    currentReceiveFile.bytesReceived += arrayBuffer.byteLength;
    const progress = (currentReceiveFile.bytesReceived / currentReceiveFile.fileSize) * 100;
    log(`📦 Recebendo ${currentReceiveFile.fileName}: ${progress.toFixed(1)}%`);
}

// ---------- Chat ----------
function displayChatMessage(sender, text, timestamp) {
    const msgDiv = document.createElement('div');
    msgDiv.className = 'chat-message';
    if (sender === localUserName) {
        msgDiv.classList.add('own');
    }
    const time = new Date(timestamp || Date.now()).toLocaleTimeString();
    msgDiv.innerHTML = `
        <span class="msg-sender">${escapeHtml(sender)}</span>
        <span class="msg-time">${time}</span>
        <div class="msg-text">${escapeHtml(text)}</div>
    `;
    elements.chatMessages.appendChild(msgDiv);
    elements.chatMessages.scrollTop = elements.chatMessages.scrollHeight;
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function sendChatMessage() {
    const text = elements.chatInput.value.trim();
    if (!text || !dataChannel || dataChannel.readyState !== 'open') return;
    const message = {
        type: 'text',
        sender: localUserName,
        text: text,
        timestamp: new Date().toISOString()
    };
    dataChannel.send(JSON.stringify(message));
    displayChatMessage(localUserName, text, message.timestamp);
    elements.chatInput.value = '';
}

// ---------- Transferência de Arquivos ----------
function handleFileStart(message) {
    currentReceiveFile = {
        fileId: message.fileId,
        fileName: message.fileName,
        fileSize: message.fileSize,
        totalChunks: message.totalChunks,
        chunks: [],
        bytesReceived: 0
    };
    log(`📥 Iniciando recebimento de "${message.fileName}" (${formatBytes(message.fileSize)})`);
}

function handleFileEnd(message) {
    if (!currentReceiveFile || currentReceiveFile.fileId !== message.fileId) {
        log('⚠️ file-end inesperado');
        return;
    }
    const { fileName, fileSize, chunks } = currentReceiveFile;
    if (chunks.length !== currentReceiveFile.totalChunks) {
        log(`⚠️ Arquivo incompleto: recebidos ${chunks.length}/${currentReceiveFile.totalChunks} chunks`);
        currentReceiveFile = null;
        return;
    }
    const blob = new Blob(chunks, { type: 'application/octet-stream' });
    const url = URL.createObjectURL(blob);
    log(`✅ Arquivo recebido: ${fileName} (${formatBytes(fileSize)})`);
    
    // Adiciona à lista de recebidos
    addReceivedFile(fileName, fileSize, url);
    
    // Tenta download automático
    tryDownload(url, fileName);
    
    currentReceiveFile = null;
}

function addReceivedFile(fileName, fileSize, url) {
    const li = document.createElement('li');
    li.innerHTML = `
        <div class="file-info">
            <span class="file-name">📄 ${escapeHtml(fileName)}</span>
            <span class="file-size">${formatBytes(fileSize)}</span>
        </div>
        <button class="btn-download" data-url="${url}" data-filename="${escapeHtml(fileName)}">Baixar</button>
    `;
    li.querySelector('.btn-download').addEventListener('click', (e) => {
        const btn = e.target;
        const dlUrl = btn.dataset.url;
        const dlName = btn.dataset.filename;
        tryDownload(dlUrl, dlName);
    });
    elements.receivedFilesList.appendChild(li);
}

function tryDownload(url, fileName) {
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    log(`📥 Download automático de "${fileName}" disparado`);
}

async function sendFile(file) {
    if (!dataChannel || dataChannel.readyState !== 'open') {
        alert('⚠️ Você precisa estar conectado para enviar arquivos.');
        return;
    }
    if (isSendingFile) {
        alert('⚠️ Já existe uma transferência em andamento.');
        return;
    }
    
    const fileId = generateFileId();
    const fileSize = file.size;
    const totalChunks = Math.ceil(fileSize / CHUNK_SIZE);
    
    currentSendFile = {
        file: file,
        fileId: fileId,
        fileName: file.name,
        fileSize: fileSize,
        totalChunks: totalChunks,
        sentChunks: 0,
        startTime: Date.now(),
        bytesSent: 0
    };
    
    isSendingFile = true;
    elements.uploadProgress.style.display = 'block';
    elements.uploadFileName.textContent = file.name;
    updateUploadProgress();
    
    // Envia mensagem de início
    const startMsg = {
        type: 'file-start',
        fileId: fileId,
        fileName: file.name,
        fileSize: fileSize,
        totalChunks: totalChunks
    };
    dataChannel.send(JSON.stringify(startMsg));
    log(`📤 Iniciando envio de "${file.name}" (${formatBytes(fileSize)})`);
    
    try {
        for (let i = 0; i < totalChunks; i++) {
            // Verifica buffer e aguarda liberar se necessário
            if (dataChannel.bufferedAmount > BUFFER_THRESHOLD) {
                await waitForBufferToDrain();
            }
            
            const start = i * CHUNK_SIZE;
            const end = Math.min(start + CHUNK_SIZE, fileSize);
            const chunk = file.slice(start, end);
            const arrayBuffer = await chunk.arrayBuffer();
            
            dataChannel.send(arrayBuffer);
            currentSendFile.sentChunks++;
            currentSendFile.bytesSent += arrayBuffer.byteLength;
            updateUploadProgress();
        }
        
        // Envia mensagem de fim
        const endMsg = {
            type: 'file-end',
            fileId: fileId
        };
        dataChannel.send(JSON.stringify(endMsg));
        log(`✅ Envio de "${file.name}" concluído!`);
    } catch (err) {
        log(`❌ Erro ao enviar arquivo: ${err.message}`);
    } finally {
        isSendingFile = false;
        currentSendFile = null;
        setTimeout(() => {
            elements.uploadProgress.style.display = 'none';
        }, 2000);
    }
}

function waitForBufferToDrain() {
    return new Promise((resolve) => {
        const originalHandler = dataChannel.onbufferedamountlow;
        dataChannel.onbufferedamountlow = () => {
            if (originalHandler) originalHandler();
            dataChannel.onbufferedamountlow = originalHandler;
            resolve();
        };
        // Se o buffer já estiver baixo, resolve imediatamente
        if (dataChannel.bufferedAmount <= BUFFER_THRESHOLD) {
            dataChannel.onbufferedamountlow = originalHandler;
            resolve();
        }
    });
}

function updateUploadProgress() {
    if (!currentSendFile) return;
    const percent = (currentSendFile.sentChunks / currentSendFile.totalChunks) * 100;
    const elapsed = (Date.now() - currentSendFile.startTime) / 1000;
    const speed = elapsed > 0 ? currentSendFile.bytesSent / elapsed / 1024 : 0; // KB/s
    elements.uploadProgressBar.style.width = percent.toFixed(1) + '%';
    elements.uploadProgressText.textContent = 
        `${percent.toFixed(1)}% — ${speed.toFixed(1)} KB/s (${formatBytes(currentSendFile.bytesSent)} de ${formatBytes(currentSendFile.fileSize)})`;
}

// ---------- Ações de Sinalização ----------
// Ofertante: criar oferta
async function createOffer() {
    try {
        if (!localUserName.trim()) {
            alert('⚠️ Digite seu nome primeiro!');
            return;
        }
        if (peerConnection && peerConnection.connectionState !== 'closed') {
            alert('⚠️ Já existe uma conexão. Use "Resetar" para recomeçar.');
            return;
        }
        
        log('📤 Criando oferta...');
        isOfferer = true;
        pendingIceCandidates = [];
        iceGatheringComplete = false;
        
        peerConnection = createPeerConnection();
        
        // Cria o DataChannel (lado do ofertante)
        const channel = peerConnection.createDataChannel('data', { ordered: true });
        setupDataChannel(channel);
        
        // Cria oferta
        const offer = await peerConnection.createOffer();
        await peerConnection.setLocalDescription(offer);
        
        // Aguarda coleta de candidatos
        await waitForIceGathering(peerConnection);
        
        // Monta e exibe o payload
        const payload = buildSignalingPayload();
        elements.offerOutput.value = payload;
        elements.btnCopyOffer.disabled = false;
        log('✅ Oferta criada! Copie o texto e envie ao respondente.');
    } catch (err) {
        log(`❌ Erro ao criar oferta: ${err.message}`);
        alert('Erro ao criar oferta: ' + err.message);
    }
}

// Respondente: responder à oferta
async function createAnswer() {
    try {
        if (!localUserName.trim()) {
            alert('⚠️ Digite seu nome primeiro!');
            return;
        }
        if (peerConnection && peerConnection.connectionState !== 'closed') {
            alert('⚠️ Já existe uma conexão. Use "Resetar" para recomeçar.');
            return;
        }
        
        const offerText = elements.offerInput.value.trim();
        if (!offerText) {
            alert('⚠️ Cole a oferta no campo de texto.');
            return;
        }
        
        const offerData = parseSignalingPayload(offerText);
        log('📥 Processando oferta recebida...');
        isOfferer = false;
        pendingIceCandidates = [];
        iceGatheringComplete = false;
        
        peerConnection = createPeerConnection();
        
        // Configura o recebimento do DataChannel (lado do respondente)
        peerConnection.ondatachannel = (event) => {
            log('📡 DataChannel recebido do ofertante');
            setupDataChannel(event.channel);
        };
        
        // Aplica a descrição remota (oferta)
        await peerConnection.setRemoteDescription({
            sdp: offerData.sdp,
            type: offerData.type
        });
        
        // Adiciona candidatos ICE da oferta
        await addIceCandidates(peerConnection, offerData.candidates || []);
        
        // Cria resposta
        log('📥 Criando resposta...');
        const answer = await peerConnection.createAnswer();
        await peerConnection.setLocalDescription(answer);
        
        // Aguarda coleta de candidatos
        await waitForIceGathering(peerConnection);
        
        // Monta e exibe a resposta
        const payload = buildSignalingPayload();
        elements.answerOutput.value = payload;
        elements.btnCopyAnswer.disabled = false;
        log('✅ Resposta criada! Copie o texto e envie de volta ao ofertante.');
    } catch (err) {
        log(`❌ Erro ao responder: ${err.message}`);
        alert('Erro ao responder: ' + err.message);
    }
}

// Ofertante: finalizar conexão com a resposta
async function finalizeConnection() {
    try {
        if (!peerConnection || !isOfferer) {
            alert('⚠️ Você precisa ser o ofertante e ter criado uma oferta primeiro.');
            return;
        }
        
        const answerText = elements.answerInput.value.trim();
        if (!answerText) {
            alert('⚠️ Cole a resposta no campo de texto.');
            return;
        }
        
        const answerData = parseSignalingPayload(answerText);
        log('🔗 Processando resposta recebida...');
        
        // Aplica a descrição remota (resposta)
        await peerConnection.setRemoteDescription({
            sdp: answerData.sdp,
            type: answerData.type
        });
        
        // Adiciona candidatos ICE da resposta
        await addIceCandidates(peerConnection, answerData.candidates || []);
        
        log('✅ Resposta aplicada! A conexão deve ser estabelecida automaticamente.');
        setStatus('connecting');
    } catch (err) {
        log(`❌ Erro ao conectar: ${err.message}`);
        alert('Erro ao conectar: ' + err.message);
    }
}

// Resetar tudo
function resetAll() {
    if (dataChannel) {
        try { dataChannel.close(); } catch (e) {}
        dataChannel = null;
    }
    if (peerConnection) {
        try { peerConnection.close(); } catch (e) {}
        peerConnection = null;
    }
    pendingIceCandidates = [];
    iceGatheringComplete = false;
    isOfferer = false;
    isSendingFile = false;
    currentSendFile = null;
    currentReceiveFile = null;
    
    elements.offerOutput.value = '';
    elements.answerOutput.value = '';
    elements.btnCopyOffer.disabled = true;
    elements.btnCopyAnswer.disabled = true;
    elements.btnSendChat.disabled = true;
    elements.chatMessages.innerHTML = '';
    elements.receivedFilesList.innerHTML = '';
    elements.uploadProgress.style.display = 'none';
    elements.logContainer.innerHTML = '';
    setStatus('disconnected');
    log('🔄 Estado resetado. Pronto para nova conexão.');
}

// ---------- Event Listeners ----------
document.addEventListener('DOMContentLoaded', () => {
    log('Aplicação carregada. Digite seu nome e siga os passos.');
    
    // Nome do usuário
    elements.userName.addEventListener('input', () => {
        localUserName = elements.userName.value.trim();
    });
    
    // Ofertante
    elements.btnCreateOffer.addEventListener('click', createOffer);
    elements.btnCopyOffer.addEventListener('click', async () => {
        if (elements.offerOutput.value) {
            const success = await copyToClipboard(elements.offerOutput.value);
            if (success) log('📋 Oferta copiada para a área de transferência!');
            else log('⚠️ Falha ao copiar — selecione e copie manualmente.');
        }
    });
    
    // Respondente
    elements.btnAnswer.addEventListener('click', createAnswer);
    elements.btnCopyAnswer.addEventListener('click', async () => {
        if (elements.answerOutput.value) {
            const success = await copyToClipboard(elements.answerOutput.value);
            if (success) log('📋 Resposta copiada para a área de transferência!');
            else log('⚠️ Falha ao copiar — selecione e copie manualmente.');
        }
    });
    
    // Finalizar
    elements.btnConnect.addEventListener('click', finalizeConnection);
    
    // Reset
    elements.btnReset.addEventListener('click', resetAll);
    
    // Chat
    elements.btnSendChat.addEventListener('click', sendChatMessage);
    elements.chatInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') sendChatMessage();
    });
    
    // Arquivos
    elements.btnSelectFile.addEventListener('click', () => {
        elements.fileInput.click();
    });
    elements.fileInput.addEventListener('change', (e) => {
        if (e.target.files.length > 0) {
            sendFile(e.target.files[0]);
            e.target.value = '';
        }
    });
    
    // Drag and drop
    elements.dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        elements.dropZone.classList.add('dragover');
    });
    elements.dropZone.addEventListener('dragleave', () => {
        elements.dropZone.classList.remove('dragover');
    });
    elements.dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        elements.dropZone.classList.remove('dragover');
        if (e.dataTransfer.files.length > 0) {
            sendFile(e.dataTransfer.files[0]);
        }
    });
});