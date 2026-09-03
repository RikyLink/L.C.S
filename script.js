// ============================================
// LAN Chat & File Transfer - WebRTC P2P
// Sinalização via Supabase Realtime (presence + broadcast)
// ============================================

// ---------- Configuração Global ----------
const ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
];

const CHUNK_SIZE = 16 * 1024; // 16 KB por chunk
const BUFFER_THRESHOLD = 1024 * 1024; // 1 MB - pausa se bufferedAmount exceder

// Estado global
let localUserName = '';
let supabaseClient = null;      // cliente Supabase (evita conflito com window.supabase)
let supabaseChannel = null;
let roomCode = null;
let isOfferer = false;
let peerConnection = null;
let dataChannel = null;
let connectionEstablished = false;

// Estado para envio de arquivos
let currentSendFile = null;
let isSendingFile = false;

// Estado para recebimento de arquivos
let currentReceiveFile = null;

// Elementos DOM
const elements = {
    userName: document.getElementById('userName'),
    btnCreateRoom: document.getElementById('btnCreateRoom'),
    roomCodeInput: document.getElementById('roomCodeInput'),
    btnJoinRoom: document.getElementById('btnJoinRoom'),
    roomInfo: document.getElementById('roomInfo'),
    roomCodeDisplay: document.getElementById('roomCodeDisplay'),
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

function generateRoomCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // sem I, O, 1, 0
    let code = '';
    for (let i = 0; i < 6; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
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

// ---------- Inicialização do Supabase ----------
function initSupabase() {
    if (typeof SUPABASE_URL === 'undefined' || typeof SUPABASE_ANON_KEY === 'undefined') {
        log('❌ Credenciais do Supabase não encontradas. Verifique config.js');
        return false;
    }
    if (typeof window.supabase === 'undefined') {
        log('❌ SDK do Supabase não carregado. Verifique sua conexão ou bloqueador de anúncios.');
        return false;
    }
    try {
        supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
        log('✅ Cliente Supabase inicializado');
        return true;
    } catch (err) {
        log(`❌ Erro ao inicializar Supabase: ${err.message}`);
        return false;
    }
}

// ---------- Gerenciamento da Sala e Sinalização ----------
async function setupSupabaseChannel(code) {
    const channelName = `room:${code}`;
    supabaseChannel = supabaseClient.channel(channelName, {
        config: {
            broadcast: { self: false } // não recebe as próprias mensagens
        }
    });

    // Escuta mensagens de sinalização (broadcast)
    supabaseChannel.on('broadcast', { event: 'signal' }, (payload) => {
        handleSignal(payload.payload);
    });

    // Escuta mudanças de presença
    supabaseChannel.on('presence', { event: 'sync' }, () => {
        const state = supabaseChannel.presenceState();
        const participants = Object.keys(state).length;
        log(`👥 Participantes na sala: ${participants}`);
        if (participants >= 2 && isOfferer && !peerConnection) {
            log('🎯 Segundo participante detectado, iniciando oferta...');
            createAndSendOffer();
        }
    });

    // Inscreve-se no canal
    await supabaseChannel.subscribe(async (status) => {
        if (status === 'SUBSCRIBED') {
            log(`📡 Inscrito no canal ${channelName}`);
            // Rastreia presença do usuário
            await supabaseChannel.track({ user: localUserName });
        }
    });
}

function sendSignal(type, data) {
    if (!supabaseChannel) return;
    supabaseChannel.send({
        type: 'broadcast',
        event: 'signal',
        payload: { type, ...data }
    });
}

function handleSignal(signal) {
    log(`📨 Sinal recebido: ${signal.type}`);
    switch (signal.type) {
        case 'offer':
            handleOffer(signal);
            break;
        case 'answer':
            handleAnswer(signal);
            break;
        case 'ice':
            handleIceCandidate(signal);
            break;
    }
}

// ---------- Criação e entrada na sala ----------
async function createRoom() {
    if (!localUserName.trim()) {
        alert('⚠️ Digite seu nome primeiro!');
        return;
    }
    if (supabaseChannel) {
        alert('⚠️ Você já está em uma sala. Use Resetar para sair.');
        return;
    }
    if (!initSupabase()) return;

    roomCode = generateRoomCode();
    isOfferer = true;
    elements.roomInfo.style.display = 'block';
    elements.roomCodeDisplay.textContent = roomCode;
    log(`🏠 Sala criada com código: ${roomCode}`);

    await setupSupabaseChannel(roomCode);
    // A oferta será criada automaticamente quando outro participante entrar
}

async function joinRoom() {
    if (!localUserName.trim()) {
        alert('⚠️ Digite seu nome primeiro!');
        return;
    }
    if (supabaseChannel) {
        alert('⚠️ Você já está em uma sala. Use Resetar para sair.');
        return;
    }
    const code = elements.roomCodeInput.value.trim().toUpperCase();
    if (!code) {
        alert('⚠️ Digite o código da sala.');
        return;
    }
    if (!initSupabase()) return;

    roomCode = code;
    isOfferer = false;
    elements.roomInfo.style.display = 'block';
    elements.roomCodeDisplay.textContent = roomCode;
    log(`🚪 Entrando na sala: ${roomCode}`);

    await setupSupabaseChannel(roomCode);
    // O respondente aguardará a oferta
}

// ---------- WebRTC: criação de PeerConnection e DataChannel ----------
function createPeerConnection() {
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            sendSignal('ice', {
                candidate: event.candidate.candidate,
                sdpMid: event.candidate.sdpMid,
                sdpMLineIndex: event.candidate.sdpMLineIndex
            });
            log('🧊 Candidato ICE enviado');
        }
    };

    pc.onconnectionstatechange = () => {
        const state = pc.connectionState;
        log(`Estado da conexão: ${state}`);
        switch (state) {
            case 'connected':
                connectionEstablished = true;
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

// ---------- Ofertante: criar e enviar oferta ----------
async function createAndSendOffer() {
    try {
        log('📤 Criando oferta...');
        peerConnection = createPeerConnection();

        // Cria DataChannel (lado do ofertante)
        const channel = peerConnection.createDataChannel('data', { ordered: true });
        setupDataChannel(channel);

        const offer = await peerConnection.createOffer();
        await peerConnection.setLocalDescription(offer);

        // Envia a oferta via Supabase
        sendSignal('offer', {
            sdp: peerConnection.localDescription.sdp,
            type: peerConnection.localDescription.type
        });
        log('✅ Oferta enviada via Supabase');
    } catch (err) {
        log(`❌ Erro ao criar oferta: ${err.message}`);
        alert('Erro ao criar oferta: ' + err.message);
    }
}

// ---------- Respondente: receber oferta e responder ----------
async function handleOffer(offerData) {
    if (isOfferer || peerConnection) {
        log('⚠️ Oferta recebida, mas este dispositivo não é o respondente ou já possui conexão.');
        return;
    }
    try {
        log('📥 Oferta recebida, processando...');
        peerConnection = createPeerConnection();

        // Configura recebimento do DataChannel
        peerConnection.ondatachannel = (event) => {
            log('📡 DataChannel recebido do ofertante');
            setupDataChannel(event.channel);
        };

        await peerConnection.setRemoteDescription({
            sdp: offerData.sdp,
            type: offerData.type
        });

        // Cria resposta
        const answer = await peerConnection.createAnswer();
        await peerConnection.setLocalDescription(answer);

        // Envia resposta
        sendSignal('answer', {
            sdp: peerConnection.localDescription.sdp,
            type: peerConnection.localDescription.type
        });
        log('✅ Resposta enviada via Supabase');
    } catch (err) {
        log(`❌ Erro ao processar oferta: ${err.message}`);
        alert('Erro ao processar oferta: ' + err.message);
    }
}

// ---------- Ofertante: receber resposta ----------
async function handleAnswer(answerData) {
    if (!isOfferer || !peerConnection) {
        log('⚠️ Resposta recebida, mas este dispositivo não é o ofertante ou não há conexão.');
        return;
    }
    try {
        log('🔗 Resposta recebida, finalizando conexão...');
        await peerConnection.setRemoteDescription({
            sdp: answerData.sdp,
            type: answerData.type
        });
        log('✅ Resposta aplicada! Aguardando conexão P2P...');
        setStatus('connecting');
    } catch (err) {
        log(`❌ Erro ao aplicar resposta: ${err.message}`);
        alert('Erro ao aplicar resposta: ' + err.message);
    }
}

// ---------- ICE Candidate ----------
async function handleIceCandidate(iceData) {
    if (!peerConnection || !peerConnection.remoteDescription) {
        log('⚠️ Candidato ICE recebido antes da descrição remota, ignorando...');
        return;
    }
    try {
        await peerConnection.addIceCandidate({
            candidate: iceData.candidate,
            sdpMid: iceData.sdpMid,
            sdpMLineIndex: iceData.sdpMLineIndex
        });
        log('🧊 Candidato ICE adicionado');
    } catch (err) {
        log(`❌ Erro ao adicionar ICE: ${err.message}`);
    }
}

// ---------- Handlers de mensagens do DataChannel ----------
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
    addReceivedFile(fileName, fileSize, url);
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
        tryDownload(btn.dataset.url, btn.dataset.filename);
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
        file,
        fileId,
        fileName: file.name,
        fileSize,
        totalChunks,
        sentChunks: 0,
        startTime: Date.now(),
        bytesSent: 0
    };

    isSendingFile = true;
    elements.uploadProgress.style.display = 'block';
    elements.uploadFileName.textContent = file.name;
    updateUploadProgress();

    const startMsg = {
        type: 'file-start',
        fileId,
        fileName: file.name,
        fileSize,
        totalChunks
    };
    dataChannel.send(JSON.stringify(startMsg));
    log(`📤 Iniciando envio de "${file.name}" (${formatBytes(fileSize)})`);

    try {
        for (let i = 0; i < totalChunks; i++) {
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
        const endMsg = { type: 'file-end', fileId };
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
    const speed = elapsed > 0 ? currentSendFile.bytesSent / elapsed / 1024 : 0;
    elements.uploadProgressBar.style.width = percent.toFixed(1) + '%';
    elements.uploadProgressText.textContent =
        `${percent.toFixed(1)}% — ${speed.toFixed(1)} KB/s (${formatBytes(currentSendFile.bytesSent)} de ${formatBytes(currentSendFile.fileSize)})`;
}

// ---------- Resetar ----------
function resetAll() {
    if (dataChannel) {
        try { dataChannel.close(); } catch (e) {}
        dataChannel = null;
    }
    if (peerConnection) {
        try { peerConnection.close(); } catch (e) {}
        peerConnection = null;
    }
    if (supabaseChannel) {
        try {
            supabaseChannel.untrack();
            supabaseChannel.unsubscribe();
            supabaseChannel = null;
        } catch (e) {}
    }
    isOfferer = false;
    connectionEstablished = false;
    isSendingFile = false;
    currentSendFile = null;
    currentReceiveFile = null;
    roomCode = null;

    elements.roomInfo.style.display = 'none';
    elements.roomCodeDisplay.textContent = '';
    elements.roomCodeInput.value = '';
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
    log('Aplicação carregada. Digite seu nome e crie/entre em uma sala.');

    elements.userName.addEventListener('input', () => {
        localUserName = elements.userName.value.trim();
    });

    elements.btnCreateRoom.addEventListener('click', createRoom);
    elements.btnJoinRoom.addEventListener('click', joinRoom);
    elements.roomCodeInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') joinRoom();
    });

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